// ============================================================
//  scripts/fetch.js — ULIFT Mixpanel Dashboard (스트리밍 방식)
// ============================================================

const https  = require('https');
const zlib   = require('zlib');
const fs     = require('fs');
const path   = require('path');
const stream = require('stream');

const USERNAME   = process.env.MIXPANEL_USERNAME;
const SECRET     = process.env.MIXPANEL_SECRET;
const PROJECT_ID = process.env.MIXPANEL_PROJECT_ID;
const DAYS       = parseInt(process.env.FETCH_DAYS || '7', 10);

if (!USERNAME || !SECRET || !PROJECT_ID) {
  console.error('❌ 환경변수 누락');
  process.exit(1);
}

const CREDENTIALS = Buffer.from(USERNAME + ':' + SECRET).toString('base64');

function dateStr(daysAgo) {
  var d = new Date();
  d.setDate(d.getDate() - daysAgo);
  return d.toISOString().split('T')[0];
}

// ── 스트리밍 방식으로 NDJSON 파싱 ────────────────────────────
// 한 번에 전체를 메모리에 올리지 않고 줄 단위로 처리
function fetchEventsStreaming(fromDate, toDate, eventNames, onEvent) {
  return new Promise(function(resolve, reject) {
    console.log('  이벤트 수집 (스트리밍): ' + fromDate + ' ~ ' + toDate);
    var evParam = encodeURIComponent(JSON.stringify(eventNames));
    var urlPath = '/api/2.0/export/?project_id=' + PROJECT_ID
      + '&from_date=' + fromDate
      + '&to_date=' + toDate
      + '&event=' + evParam;

    var count = 0;
    var buffer = '';

    https.get({
      host: 'data.mixpanel.com',
      path: urlPath,
      headers: {
        'Authorization': 'Basic ' + CREDENTIALS,
        'Accept-Encoding': 'gzip',
      },
    }, function(res) {
      var dataStream = res.headers['content-encoding'] === 'gzip'
        ? res.pipe(zlib.createGunzip())
        : res;

      dataStream.on('data', function(chunk) {
        // 청크를 버퍼에 추가하고 줄 단위로 파싱
        buffer += chunk.toString('utf8');
        var lines = buffer.split('\n');
        // 마지막 줄은 아직 완성 안됐을 수 있으니 버퍼에 남김
        buffer = lines.pop();

        for (var i = 0; i < lines.length; i++) {
          var line = lines[i].trim();
          if (!line) continue;
          try {
            var ev = JSON.parse(line);
            onEvent(ev);
            count++;
          } catch(e) {}
        }
      });

      dataStream.on('end', function() {
        // 남은 버퍼 처리
        if (buffer.trim()) {
          try {
            var ev = JSON.parse(buffer.trim());
            onEvent(ev);
            count++;
          } catch(e) {}
        }
        console.log('  → ' + count + '개 이벤트 처리 완료');
        resolve(count);
      });

      dataStream.on('error', reject);
    }).on('error', reject);
  });
}

// ── 분석 ─────────────────────────────────────────────────────
var userMap = {};
var dauMap  = {};

function processEvent(ev) {
  var props = ev.properties || {};
  var uid   = props.distinct_id;
  if (!uid) return;
  if (props.is_internal_test === true || props.is_internal_user === true) return;

  if (!userMap[uid]) userMap[uid] = [];
  userMap[uid].push({
    event     : ev.event,
    time      : (props.time || 0) * 1000,
    url       : props.cep_url_path || props.current_url || props['$current_url'] || '',
    scrollPct : props['$mp_scroll_percentage'] || props.mp_scroll_percentage || props['$scroll_percentage'] || props.scroll_percentage || props.scrollPercentage || 0,
    courseName: props.course_name || '',
  });

  // DAU 집계
  var day = new Date((props.time || 0) * 1000).toISOString().split('T')[0];
  if (!dauMap[day]) dauMap[day] = new Set();
  dauMap[day].add(uid);
}

function buildResult(from, to) {
  // 유저별 시간순 정렬
  Object.keys(userMap).forEach(function(uid) {
    userMap[uid].sort(function(a,b){ return a.time - b.time; });
  });

  var allUids   = Object.keys(userMap);
  var buyerUids = allUids.filter(function(uid) {
    return userMap[uid].some(function(e){
      return e.event === 'web_complete_purchase' || e.event === 'app_complete_purchase';
    });
  });
  var trialUids = allUids.filter(function(uid) {
    return userMap[uid].some(function(e){ return e.event === 'web_start_cell_free'; });
  });

  // DAU
  var dauArr = Object.keys(dauMap).sort().map(function(d){
    return { date: d, count: dauMap[d].size };
  });
  var mau = allUids.length;

  // 코스별 무료체험
  var courseMap = {};
  Object.keys(userMap).forEach(function(uid) {
    userMap[uid].forEach(function(e) {
      if (e.event !== 'web_start_cell_free') return;
      var cn = e.courseName || '(미분류)';
      courseMap[cn] = (courseMap[cn] || 0) + 1;
    });
  });
  var courseTrials = Object.keys(courseMap)
    .sort(function(a,b){ return courseMap[b] - courseMap[a]; })
    .slice(0, 10)
    .map(function(c){ return { course: c, count: courseMap[c] }; });

  // 코호트
  var trialGroup = [], directGroup = [];
  buyerUids.forEach(function(uid) {
    var evs = userMap[uid];
    var hasTrial   = evs.some(function(e){ return e.event === 'web_start_cell_free'; });
    var signupEv   = evs.find(function(e){ return e.event==='web_complete_signup'||e.event==='app_complete_signup'; });
    var purchaseEv = evs.find(function(e){ return e.event==='web_complete_purchase'||e.event==='app_complete_purchase'; });
    var usedCoupon = evs.some(function(e){ return e.event === 'web_complete_coupon'; });
    var ttp        = signupEv && purchaseEv ? purchaseEv.time - signupEv.time : null;
    var visits     = evs.filter(function(e){ return e.event === '$mp_web_page_view'; }).length;
    var record     = { ttp: ttp, usedCoupon: usedCoupon, visits: visits };
    if (hasTrial) { trialGroup.push(record); } else { directGroup.push(record); }
  });

  function median(arr) {
    if (!arr.length) return null;
    var s = arr.slice().sort(function(a,b){return a-b;});
    var m = Math.floor(s.length/2);
    return s.length%2 ? s[m] : (s[m-1]+s[m])/2;
  }
  function avg(arr) { return arr.length ? arr.reduce(function(a,b){return a+b;},0)/arr.length : 0; }
  function pct(a,b) { return b ? +((a/b)*100).toFixed(1) : 0; }

  function statGroup(group) {
    var times   = group.map(function(u){return u.ttp;}).filter(function(t){return t&&t>0;});
    var visits  = group.map(function(u){return u.visits;});
    var coupons = group.filter(function(u){return u.usedCoupon;}).length;
    var med = median(times), mn = avg(times), mv = avg(visits);
    return {
      count      : group.length,
      medianTime : med ? Math.round(med) : null,
      meanTime   : mn  ? Math.round(mn)  : null,
      couponRate : pct(coupons, group.length),
      avgVisits  : +mv.toFixed(1),
    };
  }

  // 퍼널 — 각 단계는 LDM7 방문 이후 순서대로 발생한 경우만 카운트
  var ldm7Visit    = 0;
  var ldm7Scroll5  = 0;
  var couponModal  = 0;
  var ldm6Visit    = 0;
  var paymentVisit = 0;

  allUids.forEach(function(uid) {
    var evs = userMap[uid];

    // 1단계: LDM7 방문 시점 찾기
    var ldm7Time = -1;
    for (var i=0; i<evs.length; i++) {
      if (evs[i].url.indexOf('/ldm/7') !== -1) {
        ldm7Time = evs[i].time;
        break;
      }
    }
    if (ldm7Time === -1) return;
    ldm7Visit++;

    // 2단계: LDM7 방문 이후 스크롤 5% 이상
    var scroll5Time = -1;
    for (var i=0; i<evs.length; i++) {
      if (evs[i].time < ldm7Time) continue;
      if (evs[i].url.indexOf('/ldm/7') !== -1 && evs[i].event === '$mp_scroll' && evs[i].scrollPct >= 5) {
        scroll5Time = evs[i].time;
        break;
      }
    }
    if (scroll5Time === -1) return;
    ldm7Scroll5++;

    // 3단계: LDM7 방문 이후 쿠폰 모달 오픈 (LDM7 방문한 세션 내에서만)
    var couponTime = -1;
    for (var i=0; i<evs.length; i++) {
      if (evs[i].time < ldm7Time) continue;
      if (evs[i].event === 'web_open_coupon_modal') {
        // LDM7 방문 이후, LDM7 페이지에서 열렸거나 LDM7 방문 후 바로 열린 경우만
        // 직전 페이지뷰가 /ldm/7 이었는지 확인
        var lastPageUrl = '';
        for (var k=i-1; k>=0; k--) {
          if (evs[k].event === '$mp_web_page_view') {
            lastPageUrl = evs[k].url;
            break;
          }
        }
        if (lastPageUrl.indexOf('/ldm/7') !== -1) {
          couponTime = evs[i].time;
          break;
        }
      }
    }
    if (couponTime === -1) return;
    couponModal++;

    // 4단계: 쿠폰 모달 이후 LDM6 방문
    var ldm6Time = -1;
    for (var i=0; i<evs.length; i++) {
      if (evs[i].time < ldm7Time) continue;
      if (evs[i].url.indexOf('/ldm/6') !== -1) {
        ldm6Time = evs[i].time;
        break;
      }
    }
    if (ldm6Time === -1) return;
    ldm6Visit++;

    // 5단계: LDM7 방문 이후 결제 상세
    var payTime = -1;
    for (var i=0; i<evs.length; i++) {
      if (evs[i].time < ldm7Time) continue;
      if (evs[i].url.indexOf('/payment') !== -1) {
        payTime = evs[i].time;
        break;
      }
    }
    if (payTime === -1) return;
    paymentVisit++;
  });

  // LDM7 다음 행동
  // LDM7 방문 후 다음 행동
  // - 조건: 페이지뷰 이벤트만, /ldm/7 이 아닌 페이지, 웹뷰(/web-view) 제외
  var ldm7BehMap = {};
  allUids.forEach(function(uid) {
    var evs = userMap[uid];
    for (var i=0; i<evs.length-1; i++) {
      // LDM7 페이지 진입 이벤트 찾기
      if (evs[i].event !== '$mp_web_page_view') continue;
      if (evs[i].url.indexOf('/ldm/7') === -1) continue;

      // 다음 이벤트 중 페이지뷰이면서 다른 페이지인 것 찾기
      for (var j=i+1; j<evs.length; j++) {
        var next = evs[j];
        // 페이지뷰가 아니면 스킵 (스크롤 등 제외)
        if (next.event !== '$mp_web_page_view') continue;
        var nextPath = next.url.replace(/https?:\/\/[^/]+/,'').split('?')[0] || '/';
        // LDM7 자기 자신, 웹뷰 경로 제외
        if (nextPath.indexOf('/ldm/7') !== -1) continue;
        if (nextPath.indexOf('/web-view') !== -1) continue;
        ldm7BehMap[nextPath] = (ldm7BehMap[nextPath]||0) + 1;
        break;
      }
      break;
    }
  });
  var ldm7Behavior = Object.keys(ldm7BehMap)
    .sort(function(a,b){return ldm7BehMap[b]-ldm7BehMap[a];})
    .slice(0,8).map(function(k){return{action:k,count:ldm7BehMap[k]};});

  // 구매자 결제 전 방문 경로
  // - 결제 상세(/payment) 진입 직전까지의 페이지만 추출
  // - /payment 관련 경로 전체 제외
  var tossPageMap = {};
  buyerUids.forEach(function(uid) {
    var evs = userMap[uid];

    // 결제 상세 진입 시점 찾기 (/payment 첫 방문)
    var paymentIdx = -1;
    for (var i=0; i<evs.length; i++) {
      if (evs[i].event === '$mp_web_page_view' && evs[i].url.indexOf('/payment') !== -1) {
        paymentIdx = i;
        break;
      }
    }
    if (paymentIdx <= 0) return;

    // 결제 상세 진입 전 최대 15개 페이지뷰 수집 (payment 관련 경로 전체 제외)
    var excludePaths = ['/payment', '/splash'];
    for (var j=Math.max(0, paymentIdx-15); j<paymentIdx; j++) {
      if (evs[j].event !== '$mp_web_page_view') continue;
      var pg = evs[j].url.replace(/https?:\/\/[^/]+/,'').split('?')[0] || '/';
      // 결제 관련 경로 제외
      var excluded = false;
      for (var k=0; k<excludePaths.length; k++) {
        if (pg.indexOf(excludePaths[k]) !== -1) { excluded = true; break; }
      }
      if (excluded) continue;
      tossPageMap[pg] = (tossPageMap[pg]||0) + 1;
    }
  });
  var tossPages = Object.keys(tossPageMap)
    .sort(function(a,b){return tossPageMap[b]-tossPageMap[a];})
    .slice(0,8).map(function(k){return{page:k,count:tossPageMap[k]};});

  return {
    meta: { updatedAt: new Date().toISOString(), from: from, to: to, fetchDays: DAYS },
    mau : mau,
    dau : dauArr,
    courseTrials: courseTrials,
    cohort: {
      trial : statGroup(trialGroup),
      direct: statGroup(directGroup),
      trialConversionRate: pct(trialGroup.length, trialUids.length),
      totalTrialUsers    : trialUids.length,
    },
    funnel: [
      { step:'LDM7 방문',          count: ldm7Visit },
      { step:'스크롤 5% 이상',      count: ldm7Scroll5 },
      { step:'쿠폰 모달 오픈',      count: couponModal },
      { step:'마스터패키지(LDM6)',   count: ldm6Visit },
      { step:'결제 상세(/payment)', count: paymentVisit },
      { step:'결제 완료',           count: buyerUids.length },
    ],
    ldm7Behavior: ldm7Behavior,
    tossPages   : tossPages,
  };
}

// ── 메인 ─────────────────────────────────────────────────────
var from = dateStr(DAYS);
var to   = dateStr(1);

console.log('\n🚀 ULIFT Dashboard Fetch 시작 (' + from + ' ~ ' + to + ')\n');

fetchEventsStreaming(from, to, [
  'web_complete_purchase', 'app_complete_purchase',
  'web_complete_signup',   'app_complete_signup',
  'web_start_cell_free',
  'web_complete_coupon',   'web_open_coupon_modal',
  '$mp_web_page_view',     '$mp_scroll',
  'app_page_home',         'app_page_freetrial',
  'app_page_offering',     'app_page_course_list',
], processEvent)
.then(function() {
  console.log('\n  분석 중…');
  var result  = buildResult(from, to);
  var outPath = path.join(__dirname, '..', 'docs', 'data.json');
  fs.writeFileSync(outPath, JSON.stringify(result, null, 2), 'utf8');

  console.log('\n✅ 완료: docs/data.json');
  console.log('   MAU:      ' + result.mau + '명');
  console.log('   셀체험→구매: ' + result.cohort.trial.count + '명');
  console.log('   바로구매:   ' + result.cohort.direct.count + '명');
  console.log('   전환율:    ' + result.cohort.trialConversionRate + '%\n');
})
.catch(function(e) {
  console.error('❌ 오류:', e.message);
  process.exit(1);
});
