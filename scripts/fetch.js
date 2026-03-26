// ============================================================
//  scripts/fetch.js — ULIFT Mixpanel Dashboard v2
//  스트리밍 방식 + 유료회원 리텐션 분석
// ============================================================

const https  = require('https');
const zlib   = require('zlib');
const fs     = require('fs');
const path   = require('path');

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

// ── 스트리밍 이벤트 수집 ──────────────────────────────────────
function fetchEventsStreaming(fromDate, toDate, eventNames, onEvent) {
  return new Promise(function(resolve, reject) {
    console.log('  이벤트 수집: ' + fromDate + ' ~ ' + toDate);
    var evParam = encodeURIComponent(JSON.stringify(eventNames));
    var urlPath = '/api/2.0/export/?project_id=' + PROJECT_ID
      + '&from_date=' + fromDate + '&to_date=' + toDate
      + '&event=' + evParam;

    var count = 0, buffer = '';

    https.get({
      host: 'data.mixpanel.com',
      path: urlPath,
      headers: { 'Authorization': 'Basic ' + CREDENTIALS, 'Accept-Encoding': 'gzip' },
    }, function(res) {
      var stream = res.headers['content-encoding'] === 'gzip' ? res.pipe(zlib.createGunzip()) : res;
      stream.on('data', function(chunk) {
        buffer += chunk.toString('utf8');
        var lines = buffer.split('\n');
        buffer = lines.pop();
        lines.forEach(function(line) {
          line = line.trim();
          if (!line) return;
          try { onEvent(JSON.parse(line)); count++; } catch(e) {}
        });
      });
      stream.on('end', function() {
        if (buffer.trim()) { try { onEvent(JSON.parse(buffer.trim())); count++; } catch(e) {} }
        console.log('  → ' + count + '개 이벤트 처리');
        resolve(count);
      });
      stream.on('error', reject);
    }).on('error', reject);
  });
}

// ── 데이터 저장소 ─────────────────────────────────────────────
var userMap = {};
var dauMap  = {};

function processEvent(ev) {
  var props = ev.properties || {};
  var uid   = props.distinct_id;
  if (!uid) return;
  if (props.is_internal_test === true || props.is_internal_user === true) return;

  if (!userMap[uid]) userMap[uid] = { events: [], isPaid: false };
  userMap[uid].events.push({
    event     : ev.event,
    time      : (props.time || 0) * 1000,
    url       : props.cep_url_path || props.current_url || '',
    scrollPct : props['$mp_scroll_percentage'] || 0,
    courseName: props.course_name || '',
    cellName  : props.cell_name || '',
    studyType : props.study_type || '',
    platform  : props.platform || '',
  });

  // 유료회원 마킹
  if (props.study_type === 'paid') userMap[uid].isPaid = true;
  if (ev.event === 'web_complete_purchase' || ev.event === 'app_complete_purchase') {
    userMap[uid].isPaid = true;
    userMap[uid].purchaseTime = (props.time || 0) * 1000;
  }

  // DAU
  var day = new Date((props.time||0)*1000).toISOString().split('T')[0];
  if (!dauMap[day]) dauMap[day] = new Set();
  dauMap[day].add(uid);
}

// ── 분석 ─────────────────────────────────────────────────────
function analyze(from, to) {
  var allUids  = Object.keys(userMap);
  var paidUids = allUids.filter(function(uid){ return userMap[uid].isPaid; });

  console.log('  전체 유저: ' + allUids.length + '명, 유료회원: ' + paidUids.length + '명');

  // 유저별 시간순 정렬
  allUids.forEach(function(uid) {
    userMap[uid].events.sort(function(a,b){ return a.time - b.time; });
  });

  // DAU
  var dauArr = Object.keys(dauMap).sort().map(function(d){
    return { date:d, count:dauMap[d].size };
  });

  // ── 1. 유료회원 현황 지표 ───────────────────────────────────

  // D3/D7 리텐션: 구매 후 N일 내 app_page_home 방문
  var d3Count = 0, d7Count = 0, retBase = 0;
  var D3 = 3*24*3600*1000, D7 = 7*24*3600*1000;

  paidUids.forEach(function(uid) {
    var u = userMap[uid];
    var purchaseEv = u.events.find(function(e){
      return e.event === 'web_complete_purchase' || e.event === 'app_complete_purchase';
    });
    if (!purchaseEv) return;
    retBase++;
    var pt = purchaseEv.time;
    var hasD3 = u.events.some(function(e){
      return e.event === 'app_page_home' && e.time > pt && e.time <= pt + D3;
    });
    var hasD7 = u.events.some(function(e){
      return e.event === 'app_page_home' && e.time > pt && e.time <= pt + D7;
    });
    if (hasD3) d3Count++;
    if (hasD7) d7Count++;
  });

  var d3Rate = retBase ? +(d3Count/retBase*100).toFixed(1) : 0;
  var d7Rate = retBase ? +(d7Count/retBase*100).toFixed(1) : 0;

  // 주간 평균 방문 횟수 (유료회원의 app_page_home 기준, 주당)
  var totalPaidVisits = 0;
  paidUids.forEach(function(uid) {
    totalPaidVisits += userMap[uid].events.filter(function(e){ return e.event==='app_page_home'; }).length;
  });
  var weeks = Math.max(1, DAYS / 7);
  var avgWeeklyVisit = paidUids.length ? +(totalPaidVisits / paidUids.length / weeks).toFixed(1) : 0;

  // 일일 평균 학습 시간 (app_start_cell ~ app_complete_cell 쌍 매칭)
  var totalStudyMs = 0, studySessionCount = 0;
  paidUids.forEach(function(uid) {
    var evs = userMap[uid].events;
    for (var i=0; i<evs.length; i++) {
      if (evs[i].event !== 'app_start_cell') continue;
      var cellName = evs[i].cellName;
      for (var j=i+1; j<evs.length; j++) {
        if (evs[j].event === 'app_complete_cell' && evs[j].cellName === cellName) {
          var diff = evs[j].time - evs[i].time;
          if (diff > 0 && diff < 7200000) { // 2시간 이내만 유효
            totalStudyMs += diff;
            studySessionCount++;
          }
          break;
        }
      }
    }
  });
  var avgDailyStudyMin = studySessionCount && DAYS
    ? +(totalStudyMs / studySessionCount / 60000).toFixed(1) : 0;

  // 일일 평균 학습 개수 (app_start_cell 기준)
  var totalCells = 0;
  paidUids.forEach(function(uid) {
    totalCells += userMap[uid].events.filter(function(e){ return e.event==='app_start_cell'; }).length;
  });
  var avgDailyCells = paidUids.length && DAYS
    ? +(totalCells / paidUids.length / DAYS).toFixed(1) : 0;

  // 주간 평균 뱃지 획득 비율
  var badgeUids = paidUids.filter(function(uid) {
    return userMap[uid].events.some(function(e){
      return e.event==='app_open_modal_newbadge' || e.event==='webapp_open_modal_newbadge';
    });
  });
  var badgeRate = paidUids.length ? +(badgeUids.length/paidUids.length*100).toFixed(1) : 0;

  // ── 2. 리텐션 개선 지표 ─────────────────────────────────────

  // 구매 후 3일 내 첫 학습 시작 비율
  var first3DayStudy = 0;
  paidUids.forEach(function(uid) {
    var u = userMap[uid];
    var purchaseEv = u.events.find(function(e){
      return e.event==='web_complete_purchase' || e.event==='app_complete_purchase';
    });
    if (!purchaseEv) return;
    var pt = purchaseEv.time;
    var hasStudy = u.events.some(function(e){
      return e.event==='app_start_cell' && e.time > pt && e.time <= pt + D3;
    });
    if (hasStudy) first3DayStudy++;
  });
  var first3DayRate = retBase ? +(first3DayStudy/retBase*100).toFixed(1) : 0;

  // 방문일에 학습 5개 이상 진행 비율
  var deepLearningUids = 0;
  paidUids.forEach(function(uid) {
    var evs = userMap[uid].events;
    // 날짜별로 그루핑
    var dayMap = {};
    evs.forEach(function(e) {
      if (e.event !== 'app_start_cell' && e.event !== 'app_page_home') return;
      var day = new Date(e.time).toISOString().split('T')[0];
      if (!dayMap[day]) dayMap[day] = { visits:0, cells:0 };
      if (e.event === 'app_page_home') dayMap[day].visits++;
      if (e.event === 'app_start_cell') dayMap[day].cells++;
    });
    var has5plus = Object.keys(dayMap).some(function(d){
      return dayMap[d].visits > 0 && dayMap[d].cells >= 5;
    });
    if (has5plus) deepLearningUids++;
  });
  var deepLearningRate = paidUids.length ? +(deepLearningUids/paidUids.length*100).toFixed(1) : 0;

  // 첫 학습 후 다음 학습까지 리드타임 3일 이내 비율
  var quickReturnCount = 0, quickReturnBase = 0;
  paidUids.forEach(function(uid) {
    var evs = userMap[uid].events.filter(function(e){ return e.event==='app_start_cell'; });
    if (evs.length < 2) return;
    quickReturnBase++;
    var diff = evs[1].time - evs[0].time;
    if (diff > 0 && diff <= D3) quickReturnCount++;
  });
  var quickReturnRate = quickReturnBase ? +(quickReturnCount/quickReturnBase*100).toFixed(1) : 0;

  // ── 3. 코스별 학습 Top 10 ───────────────────────────────────
  var courseMap = {};
  paidUids.forEach(function(uid) {
    userMap[uid].events.forEach(function(e) {
      if (e.event !== 'app_start_cell') return;
      var cn = e.courseName || '(미분류)';
      courseMap[cn] = (courseMap[cn]||0) + 1;
    });
  });
  var topCourses = Object.keys(courseMap)
    .sort(function(a,b){ return courseMap[b]-courseMap[a]; })
    .slice(0,10)
    .map(function(c){ return { course:c, count:courseMap[c] }; });

  // ── 4. 기존 지표 유지 ───────────────────────────────────────
  var buyerUids = allUids.filter(function(uid) {
    return userMap[uid].events.some(function(e){
      return e.event==='web_complete_purchase'||e.event==='app_complete_purchase';
    });
  });
  var trialUids = allUids.filter(function(uid) {
    return userMap[uid].events.some(function(e){ return e.event==='web_start_cell_free'; });
  });

  // 코호트
  var trialGroup=[], directGroup=[];
  function pct(a,b){ return b?+((a/b)*100).toFixed(1):0; }
  function median(arr) {
    if(!arr.length) return null;
    var s=arr.slice().sort(function(a,b){return a-b;});
    var m=Math.floor(s.length/2);
    return s.length%2?s[m]:(s[m-1]+s[m])/2;
  }
  function avg(arr){ return arr.length?arr.reduce(function(a,b){return a+b;},0)/arr.length:0; }

  buyerUids.forEach(function(uid) {
    var evs=userMap[uid].events;
    var hasTrial=evs.some(function(e){return e.event==='web_start_cell_free';});
    var signupEv=evs.find(function(e){return e.event==='web_complete_signup'||e.event==='app_complete_signup';});
    var purchaseEv=evs.find(function(e){return e.event==='web_complete_purchase'||e.event==='app_complete_purchase';});
    var usedCoupon=evs.some(function(e){return e.event==='web_complete_coupon';});
    var ttp=signupEv&&purchaseEv?purchaseEv.time-signupEv.time:null;
    var visits=evs.filter(function(e){return e.event==='$mp_web_page_view';}).length;
    var record={ttp:ttp,usedCoupon:usedCoupon,visits:visits};
    if(hasTrial){trialGroup.push(record);}else{directGroup.push(record);}
  });

  function statGroup(group) {
    var times=group.map(function(u){return u.ttp;}).filter(function(t){return t&&t>0;});
    var visits=group.map(function(u){return u.visits;});
    var coupons=group.filter(function(u){return u.usedCoupon;}).length;
    var med=median(times),mn=avg(times),mv=avg(visits);
    return {count:group.length,medianTime:med?Math.round(med):null,meanTime:mn?Math.round(mn):null,couponRate:pct(coupons,group.length),avgVisits:+mv.toFixed(1)};
  }

  // 퍼널
  var ldm7Visit=0,ldm7Scroll5=0,couponModal=0,ldm6Visit=0,paymentVisit=0;
  allUids.forEach(function(uid){
    var evs=userMap[uid].events;
    var ldm7Time=-1;
    for(var i=0;i<evs.length;i++){if(evs[i].url.indexOf('/ldm/7')!==-1){ldm7Time=evs[i].time;break;}}
    if(ldm7Time===-1)return;
    ldm7Visit++;
    var scroll5Time=-1;
    for(var i=0;i<evs.length;i++){
      if(evs[i].time<ldm7Time)continue;
      if(evs[i].url.indexOf('/ldm/7')!==-1&&evs[i].event==='$mp_scroll'&&evs[i].scrollPct>=5){scroll5Time=evs[i].time;break;}
    }
    if(scroll5Time===-1)return;
    ldm7Scroll5++;
    var couponTime=-1;
    for(var i=0;i<evs.length;i++){
      if(evs[i].time<ldm7Time)continue;
      if(evs[i].event==='web_open_coupon_modal'){
        var lastPg='';
        for(var k=i-1;k>=0;k--){if(evs[k].event==='$mp_web_page_view'){lastPg=evs[k].url;break;}}
        if(lastPg.indexOf('/ldm/7')!==-1){couponTime=evs[i].time;break;}
      }
    }
    if(couponTime===-1)return;
    couponModal++;
    var ldm6Time=-1;
    for(var i=0;i<evs.length;i++){if(evs[i].time<ldm7Time)continue;if(evs[i].url.indexOf('/ldm/6')!==-1){ldm6Time=evs[i].time;break;}}
    if(ldm6Time===-1)return;
    ldm6Visit++;
    for(var i=0;i<evs.length;i++){if(evs[i].time<ldm7Time)continue;if(evs[i].url.indexOf('/payment')!==-1){paymentVisit++;break;}}
  });

  // LDM7 다음 행동 + 체류시간
  var ldm7BehMap={}, ldm7DurMap={}, ldm7DurCnt={};
  allUids.forEach(function(uid){
    var evs=userMap[uid].events;
    for(var i=0;i<evs.length-1;i++){
      if(evs[i].event!=='$mp_web_page_view')continue;
      if(evs[i].url.indexOf('/ldm/7')===-1)continue;
      // LDM7 체류시간: 다음 페이지뷰까지 시간차
      for(var j=i+1;j<evs.length;j++){
        if(evs[j].event!=='$mp_web_page_view')continue;
        var np=evs[j].url.replace(/https?:\/\/[^/]+/,'').split('?')[0]||'/';
        if(np.indexOf('/ldm/7')!==-1)continue;
        if(np.indexOf('/web-view')!==-1)continue;
        var dur=evs[j].time-evs[i].time;
        ldm7BehMap[np]=(ldm7BehMap[np]||0)+1;
        if(dur>0&&dur<1800000){ // 30분 이내만 유효
          ldm7DurMap[np]=(ldm7DurMap[np]||0)+dur;
          ldm7DurCnt[np]=(ldm7DurCnt[np]||0)+1;
        }
        break;
      }
      break;
    }
  });
  var ldm7Behavior=Object.keys(ldm7BehMap).sort(function(a,b){return ldm7BehMap[b]-ldm7BehMap[a];}).slice(0,8).map(function(k){
    var avgDur=ldm7DurCnt[k]?Math.round(ldm7DurMap[k]/ldm7DurCnt[k]):null;
    return{action:k,count:ldm7BehMap[k],avgDuration:avgDur};
  });

  // 구매 전 방문 경로
  var tossPageMap={}, tossDurMap={}, tossDurCnt={};
  var excludePaths=['/payment','/splash'];
  buyerUids.forEach(function(uid){
    var evs=userMap[uid].events;
    var payIdx=-1;
    for(var i=0;i<evs.length;i++){if(evs[i].event==='$mp_web_page_view'&&evs[i].url.indexOf('/payment')!==-1){payIdx=i;break;}}
    if(payIdx<=0)return;
    for(var j=Math.max(0,payIdx-15);j<payIdx;j++){
      if(evs[j].event!=='$mp_web_page_view')continue;
      var pg=evs[j].url.replace(/https?:\/\/[^/]+/,'').split('?')[0]||'/';
      var ex=false;
      for(var k=0;k<excludePaths.length;k++){if(pg.indexOf(excludePaths[k])!==-1){ex=true;break;}}
      if(pg.indexOf('/web-view')!==-1)ex=true;
      if(pg.indexOf('/oAuth')!==-1)ex=true;
      if(ex)continue;
      tossPageMap[pg]=(tossPageMap[pg]||0)+1;
      // 체류시간: 다음 페이지뷰까지 시간차
      if(j+1<evs.length&&evs[j+1].event==='$mp_web_page_view'){
        var dur=evs[j+1].time-evs[j].time;
        if(dur>0&&dur<1800000){
          tossDurMap[pg]=(tossDurMap[pg]||0)+dur;
          tossDurCnt[pg]=(tossDurCnt[pg]||0)+1;
        }
      }
    }
  });
  var tossPages=Object.keys(tossPageMap).sort(function(a,b){return tossPageMap[b]-tossPageMap[a];}).slice(0,8).map(function(k){
    var avgDur=tossDurCnt[k]?Math.round(tossDurMap[k]/tossDurCnt[k]):null;
    return{page:k,count:tossPageMap[k],avgDuration:avgDur};
  });

  // 코스별 무료체험
  var webTrialMap={};
  allUids.forEach(function(uid){
    userMap[uid].events.forEach(function(e){
      if(e.event!=='web_start_cell_free')return;
      var cn=e.courseName||'(미분류)';
      webTrialMap[cn]=(webTrialMap[cn]||0)+1;
    });
  });
  var courseTrials=Object.keys(webTrialMap).sort(function(a,b){return webTrialMap[b]-webTrialMap[a];}).slice(0,10).map(function(c){return{course:c,count:webTrialMap[c]};});

  var mau=allUids.length;

  return {
    meta: { updatedAt:new Date().toISOString(), from:from, to:to, fetchDays:DAYS },
    mau : mau,
    dau : dauArr,
    paidCount: paidUids.length,

    // 유료회원 현황
    retention: {
      d3Rate:d3Rate, d7Rate:d7Rate, base:retBase,
      d3Count:d3Count, d7Count:d7Count,
    },
    paidMetrics: {
      avgWeeklyVisit  : avgWeeklyVisit,
      avgDailyStudyMin: avgDailyStudyMin,
      avgDailyCells   : avgDailyCells,
      badgeRate       : badgeRate,
      badgeUidCount   : badgeUids.length,
    },

    // 리텐션 개선 지표
    retentionImprove: {
      first3DayRate   : first3DayRate,
      first3DayCount  : first3DayStudy,
      deepLearningRate: deepLearningRate,
      deepLearningCount:deepLearningUids,
      quickReturnRate : quickReturnRate,
      quickReturnCount: quickReturnCount,
      quickReturnBase : quickReturnBase,
    },

    // 코스별 학습 Top 10 (유료회원)
    topCourses: topCourses,

    // 기존 지표
    courseTrials: courseTrials,
    cohort: {
      trial:statGroup(trialGroup), direct:statGroup(directGroup),
      trialConversionRate:pct(trialGroup.length,trialUids.length),
      totalTrialUsers:trialUids.length,
    },
    funnel:[
      {step:'LDM7 방문',count:ldm7Visit},
      {step:'스크롤 5% 이상',count:ldm7Scroll5},
      {step:'쿠폰 모달 오픈',count:couponModal},
      {step:'마스터패키지(LDM6)',count:ldm6Visit},
      {step:'결제 상세(/payment)',count:paymentVisit},
      {step:'결제 완료',count:buyerUids.length},
    ],
    ldm7Behavior: ldm7Behavior,
    tossPages   : tossPages,
  };
}

// ── 메인 ─────────────────────────────────────────────────────
var from=dateStr(DAYS), to=dateStr(1);
console.log('\n🚀 ULIFT Dashboard v2 Fetch 시작 ('+from+' ~ '+to+')\n');

fetchEventsStreaming(from, to, [
  // 웹 이벤트
  'web_complete_purchase','web_complete_signup','web_start_cell_free',
  'web_complete_coupon','web_open_coupon_modal',
  '$mp_web_page_view','$mp_scroll',
  // 앱 이벤트
  'app_complete_purchase','app_complete_signup','app_complete_login',
  'app_start_cell','app_complete_cell',
  'app_open_modal_newbadge','webapp_open_modal_newbadge',
  'app_page_home','app_page_course_list','app_page_freetrial',
  'app_page_course_intro','app_page_course_classroom',
  'app_page_offering','app_page_profile',
], processEvent)
.then(function() {
  console.log('\n  분석 중…');
  var result = analyze(from, to);
  var outPath = path.join(__dirname,'..','docs','data.json');
  fs.writeFileSync(outPath, JSON.stringify(result,null,2),'utf8');
  console.log('\n✅ 완료');
  console.log('   유료회원: '+result.paidCount+'명');
  console.log('   D3 리텐션: '+result.retention.d3Rate+'%');
  console.log('   D7 리텐션: '+result.retention.d7Rate+'%');
  console.log('   구매 후 3일 내 첫 학습: '+result.retentionImprove.first3DayRate+'%\n');
})
.catch(function(e){ console.error('❌ 오류:', e.message); process.exit(1); });
