// ============================================================
//  scripts/fetch.js — ULIFT Mixpanel Dashboard
//  실행: node scripts/fetch.js
// ============================================================

const https = require('https');
const zlib  = require('zlib');
const fs    = require('fs');
const path  = require('path');

const USERNAME   = process.env.MIXPANEL_USERNAME;
const SECRET     = process.env.MIXPANEL_SECRET;
const PROJECT_ID = process.env.MIXPANEL_PROJECT_ID;
const DAYS       = parseInt(process.env.FETCH_DAYS || '7', 10);

if (!USERNAME || !SECRET || !PROJECT_ID) {
  console.error('❌ 환경변수 누락: MIXPANEL_USERNAME / MIXPANEL_SECRET / MIXPANEL_PROJECT_ID');
  process.exit(1);
}

const CREDENTIALS = Buffer.from(USERNAME + ':' + SECRET).toString('base64');

function dateStr(daysAgo) {
  var d = new Date();
  d.setDate(d.getDate() - daysAgo);
  return d.toISOString().split('T')[0];
}

function httpsGet(host, urlPath) {
  return new Promise(function(resolve, reject) {
    https.get({
      host: host,
      path: urlPath,
      headers: { 'Authorization': 'Basic ' + CREDENTIALS, 'Accept-Encoding': 'gzip, deflate' },
    }, function(res) {
      var chunks = [];
      var stream = res.headers['content-encoding'] === 'gzip' ? res.pipe(zlib.createGunzip()) : res;
      stream.on('data', function(c) { chunks.push(c); });
      stream.on('end', function() { resolve(Buffer.concat(chunks).toString('utf8')); });
      stream.on('error', reject);
    }).on('error', reject);
  });
}

// ── Export API ───────────────────────────────────────────────
function fetchEvents(fromDate, toDate, eventNames) {
  console.log('  이벤트 수집: ' + fromDate + ' ~ ' + toDate);
  var evParam  = encodeURIComponent(JSON.stringify(eventNames));
  var urlPath  = '/api/2.0/export/?project_id=' + PROJECT_ID
    + '&from_date=' + fromDate + '&to_date=' + toDate
    + '&event=' + evParam;

  return httpsGet('data.mixpanel.com', urlPath).then(function(raw) {
    var trimmed = (raw || '').trim();
    var events  = [];
    if (!trimmed) { console.log('  → 0개'); return events; }
    if (trimmed.startsWith('[')) {
      try { events = JSON.parse(trimmed); } catch(e) {}
    } else {
      trimmed.split('\n').forEach(function(line) {
        line = line.trim();
        if (!line) return;
        try { events.push(JSON.parse(line)); } catch(e) {}
      });
    }
    console.log('  → ' + events.length + '개 이벤트');
    return events;
  });
}

// ── Query API (집계) ─────────────────────────────────────────
function queryAPI(endpoint) {
  return httpsGet('data.mixpanel.com', endpoint).then(function(raw) {
    try { return JSON.parse(raw); } catch(e) { return null; }
  }).catch(function(e) { console.warn('  Query 실패:', e.message); return null; });
}

// ── 분석 로직 ────────────────────────────────────────────────
function analyze(events, from, to) {
  var userMap = {};

  events.forEach(function(ev) {
    var props = ev.properties || {};
    var uid   = props.distinct_id;
    if (!uid) return;
    if (props.is_internal_test === true || props.is_internal_user === true) return;
    if (!userMap[uid]) userMap[uid] = [];
    userMap[uid].push({
      event      : ev.event,
      time       : (props.time || 0) * 1000,
      url        : props.current_url || props['$current_url'] || '',
      scrollPct  : props['$mp_scroll_percentage'] || props.mp_scroll_percentage || 0,
      courseName : props.course_name || '',
      cellName   : props.cell_name || '',
    });
  });

  // 유저별 시간순 정렬
  Object.keys(userMap).forEach(function(uid) {
    userMap[uid].sort(function(a,b){ return a.time - b.time; });
  });

  var allUids    = Object.keys(userMap);
  var buyerUids  = allUids.filter(function(uid) {
    return userMap[uid].some(function(e){ return e.event === 'web_complete_purchase'; });
  });
  var trialUids  = allUids.filter(function(uid) {
    return userMap[uid].some(function(e){ return e.event === 'web_start_cell_free'; });
  });
  var ldm7Uids   = allUids.filter(function(uid) {
    return userMap[uid].some(function(e){ return e.url.indexOf('/ldm7') !== -1; });
  });

  // ① MAU / DAU
  var dauMap = {};
  events.forEach(function(ev) {
    var props = ev.properties || {};
    if (props.is_internal_test === true) return;
    var uid = props.distinct_id;
    if (!uid) return;
    var day = new Date((props.time||0)*1000).toISOString().split('T')[0];
    if (!dauMap[day]) dauMap[day] = new Set();
    dauMap[day].add(uid);
  });
  var dauArr   = Object.keys(dauMap).sort().map(function(d){ return { date:d, count:dauMap[d].size }; });
  var mauUsers = new Set(allUids).size;

  // ② 코스별 무료체험 수
  var courseTrialMap = {};
  events.forEach(function(ev) {
    if (ev.event !== 'web_start_cell_free') return;
    var cn = (ev.properties||{}).course_name || '(미분류)';
    courseTrialMap[cn] = (courseTrialMap[cn] || 0) + 1;
  });
  var courseTrials = Object.keys(courseTrialMap)
    .sort(function(a,b){ return courseTrialMap[b]-courseTrialMap[a]; })
    .slice(0,10)
    .map(function(c){ return { course:c, count:courseTrialMap[c] }; });

  // ③ 무료체험 후 구매 vs 바로구매 코호트
  var trialThenBuy = 0, directBuy = 0;
  var cohortDetail = { trial:{}, direct:{} };

  buyerUids.forEach(function(uid) {
    var evs = userMap[uid];
    var hasTrial   = evs.some(function(e){ return e.event==='web_start_cell_free'; });
    var signupEv   = evs.find(function(e){ return e.event==='web_complete_signup'||e.event==='app_complete_signup'; });
    var purchaseEv = evs.find(function(e){ return e.event==='web_complete_purchase'; });
    var usedCoupon = evs.some(function(e){ return e.event==='web_complete_coupon'; });
    var ttp        = (signupEv && purchaseEv) ? purchaseEv.time - signupEv.time : null;
    var visits     = evs.filter(function(e){ return e.event==='$mp_web_page_view'; }).length;

    if (hasTrial) {
      trialThenBuy++;
      if (!cohortDetail.trial.ttpArr) cohortDetail.trial.ttpArr=[];
      if (!cohortDetail.trial.couponCount) cohortDetail.trial.couponCount=0;
      if (!cohortDetail.trial.visitArr) cohortDetail.trial.visitArr=[];
      if (ttp) cohortDetail.trial.ttpArr.push(ttp);
      if (usedCoupon) cohortDetail.trial.couponCount++;
      cohortDetail.trial.visitArr.push(visits);
    } else {
      directBuy++;
      if (!cohortDetail.direct.ttpArr) cohortDetail.direct.ttpArr=[];
      if (!cohortDetail.direct.couponCount) cohortDetail.direct.couponCount=0;
      if (!cohortDetail.direct.visitArr) cohortDetail.direct.visitArr=[];
      if (ttp) cohortDetail.direct.ttpArr.push(ttp);
      if (usedCoupon) cohortDetail.direct.couponCount++;
      cohortDetail.direct.visitArr.push(visits);
    }
  });

  function median(arr) {
    if (!arr||!arr.length) return null;
    var s=arr.slice().sort(function(a,b){return a-b;});
    var m=Math.floor(s.length/2);
    return s.length%2?s[m]:(s[m-1]+s[m])/2;
  }
  function avg(arr) { return arr&&arr.length?arr.reduce(function(a,b){return a+b;},0)/arr.length:0; }
  function pct(a,b) { return b?+((a/b)*100).toFixed(1):0; }

  var cohort = {
    trial: {
      count      : trialThenBuy,
      medianTime : median(cohortDetail.trial.ttpArr),
      couponRate : pct(cohortDetail.trial.couponCount||0, trialThenBuy),
      avgVisits  : +avg(cohortDetail.trial.visitArr||[]).toFixed(1),
    },
    direct: {
      count      : directBuy,
      medianTime : median(cohortDetail.direct.ttpArr),
      couponRate : pct(cohortDetail.direct.couponCount||0, directBuy),
      avgVisits  : +avg(cohortDetail.direct.visitArr||[]).toFixed(1),
    },
    trialConversionRate: pct(trialThenBuy, trialUids.length),
    totalTrialUsers    : trialUids.length,
  };

  // ④ LDM7 퍼널
  var ldm7Visit     = ldm7Uids.length;
  var ldm7Scroll5   = allUids.filter(function(uid) {
    return userMap[uid].some(function(e){
      return e.url.indexOf('/ldm7')!==-1 && e.event==='$mp_scroll' && e.scrollPct>=5;
    });
  }).length;
  var couponModal   = allUids.filter(function(uid) {
    return userMap[uid].some(function(e){ return e.event==='web_open_coupon_modal'; });
  }).length;
  var ldm6Visit     = allUids.filter(function(uid) {
    return userMap[uid].some(function(e){ return e.url.indexOf('/ldm6')!==-1; });
  }).length;
  var paymentVisit  = allUids.filter(function(uid) {
    return userMap[uid].some(function(e){ return e.url.indexOf('/payment')!==-1; });
  }).length;
  var purchaseCount = buyerUids.length;

  var funnel = [
    { step:'LDM7 방문',          count: ldm7Visit },
    { step:'스크롤 5% 이상',      count: ldm7Scroll5 },
    { step:'쿠폰 모달 오픈',      count: couponModal },
    { step:'마스터패키지(LDM6)',   count: ldm6Visit },
    { step:'결제 상세(/payment)', count: paymentVisit },
    { step:'결제 완료',           count: purchaseCount },
  ];

  // ⑤ LDM7 방문자 행동 패턴 (방문 후 다음 이벤트 분포)
  var ldm7BehaviorMap = {};
  ldm7Uids.forEach(function(uid) {
    var evs = userMap[uid];
    var ldm7Idx = -1;
    for (var i=0;i<evs.length;i++) {
      if (evs[i].url.indexOf('/ldm7')!==-1) { ldm7Idx=i; break; }
    }
    if (ldm7Idx===-1||ldm7Idx>=evs.length-1) return;
    var next = evs[ldm7Idx+1];
    var key = next.event==='$mp_web_page_view'
      ? (next.url.replace(/https?:\/\/[^/]+/,'').split('?')[0]||'/')
      : next.event;
    ldm7BehaviorMap[key]=(ldm7BehaviorMap[key]||0)+1;
  });
  var ldm7Behavior = Object.keys(ldm7BehaviorMap)
    .sort(function(a,b){return ldm7BehaviorMap[b]-ldm7BehaviorMap[a];})
    .slice(0,8)
    .map(function(k){return{action:k,count:ldm7BehaviorMap[k]};});

  // ⑥ 토스 구매자 행동 (구매 전 방문 페이지)
  var tossPageMap = {};
  buyerUids.forEach(function(uid) {
    var evs = userMap[uid];
    var buyIdx = -1;
    for (var i=evs.length-1;i>=0;i--) {
      if (evs[i].event==='web_complete_purchase') { buyIdx=i; break; }
    }
    if (buyIdx<=0) return;
    for (var j=Math.max(0,buyIdx-10);j<buyIdx;j++) {
      if (evs[j].event!=='$mp_web_page_view') continue;
      var pg=(evs[j].url.replace(/https?:\/\/[^/]+/,'').split('?')[0])||'/';
      tossPageMap[pg]=(tossPageMap[pg]||0)+1;
    }
  });
  var tossPages = Object.keys(tossPageMap)
    .sort(function(a,b){return tossPageMap[b]-tossPageMap[a];})
    .slice(0,8)
    .map(function(k){return{page:k,count:tossPageMap[k]};});

  return {
    meta        : { updatedAt:new Date().toISOString(), from:from, to:to, fetchDays:DAYS },
    dau         : dauArr,
    mau         : mauUsers,
    courseTrials: courseTrials,
    cohort      : cohort,
    funnel      : funnel,
    ldm7Behavior: ldm7Behavior,
    tossPages   : tossPages,
  };
}

// ── 메인 ─────────────────────────────────────────────────────
var from = dateStr(DAYS);
var to   = dateStr(1);

console.log('\n🚀 ULIFT Dashboard Fetch 시작 (' + from + ' ~ ' + to + ')\n');

fetchEvents(from, to, [
  '$mp_web_page_view', '$mp_scroll',
  'web_complete_purchase', 'web_complete_signup', 'app_complete_signup',
  'web_start_cell_free', 'web_complete_coupon', 'web_open_coupon_modal',
  'app_page_home', 'app_page_freetrial', 'app_page_offering',
])
.then(function(events) {
  console.log('\n  분석 중…');
  var result = analyze(events, from, to);

  var outPath = path.join(__dirname, '..', 'docs', 'data.json');
  fs.writeFileSync(outPath, JSON.stringify(result, null, 2), 'utf8');

  console.log('\n✅ 완료: docs/data.json');
  console.log('   MAU:         ' + result.mau + '명');
  console.log('   DAU (최근):  ' + (result.dau.length ? result.dau[result.dau.length-1].count : 0) + '명');
  console.log('   코스 무료체험: ' + result.courseTrials.length + '종');
  console.log('   셀체험→구매: ' + result.cohort.trial.count + '명');
  console.log('   바로구매:    ' + result.cohort.direct.count + '명');
  console.log('   LDM7 퍼널:  ' + result.funnel.map(function(f){return f.step+'('+f.count+')'}).join(' → ') + '\n');
})
.catch(function(e) {
  console.error('❌ 오류:', e.message);
  process.exit(1);
});
