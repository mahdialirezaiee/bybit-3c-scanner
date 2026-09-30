import fs from 'node:fs';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {pathToFileURL} from 'node:url';
import engine from './engine.cjs';

export const TF = {'1':60000,'3':180000,'5':300000,'15':900000,'30':1800000,'60':3600000,'240':14400000,D:86400000};
const sleep = ms => new Promise(r => setTimeout(r, ms));
const STATE_BRANCH = 'scanner-state';
const MARKER = '3c-clean-reversal-github-v1';
let nextRequest = 0;
let config;
const API_BASES = ['https://api.bybit.com','https://api.bybit.eu','https://api.bytick.com'];
let apiBase = API_BASES[0];
export function validateConfig(c) {
  if (!Array.isArray(c.timeframes) || !c.timeframes.length || c.timeframes.some(t=>!TF[t]) || new Set(c.timeframes).size!==c.timeframes.length) throw Error('Invalid timeframes');
  for (const [k,min,max] of [['swingLen',1,1000],['maxWait',10,1000],['requestsPerSecond',1,20],['concurrency',1,12],['maxCatchupHours',1,168],['closeGraceMs',2000,60000]]) {
    if (!Number.isInteger(c[k]) || c[k]<min || c[k]>max) throw Error(`Invalid config: ${k}`);
  }
  if (!Array.isArray(c.symbols) || c.symbols.some(s=>!/^\w+USDT$/.test(s))) throw Error('Invalid symbols');
  return c;
}
function git(args, input) {
  try {return execFileSync('git', args, {input, encoding:'utf8', stdio:['pipe','pipe','pipe'], maxBuffer:32*1024*1024}).trim();}
  catch {throw Error('Git state operation failed. Check contents:write permission and scanner-state branch rules.');}
}
export class StateStore {
  constructor() {this.head=null; this.dirty=false;}
  load() {
    const refs=git(['ls-remote','--heads','origin',STATE_BRANCH]);
    if (!refs) return {kind:MARKER,version:1,cursors:{},outbox:[],notes:[],fingerprint:null};
    git(['fetch','--quiet','--no-tags','origin',`refs/heads/${STATE_BRANCH}`]);
    this.head=git(['rev-parse','FETCH_HEAD']);
    let s;
    try {s=JSON.parse(git(['show',`${this.head}:state.json`]));} catch {throw Error('Unreadable scanner-state. Refusing to reset or overwrite state.');}
    if(s.kind!==MARKER || s.version!==1 || !s.cursors || !Array.isArray(s.outbox) || !Array.isArray(s.notes)) throw Error('Unknown state format; stopped without overwriting.');
    return s;
  }
  save(s) {
    // State contains market data and cursors only. Never store Telegram credentials.
    git(['config','user.name','3C Scanner']);
    git(['config','user.email','scanner@users.noreply.github.com']);
    const blob=git(['hash-object','-w','--stdin'], JSON.stringify(s)+'\n');
    const tree=git(['mktree'],`100644 blob ${blob}\tstate.json\n`);
    const args=['commit-tree',tree];
    if(this.head)args.push('-p',this.head);
    const commit=git(args,'Update scanner checkpoint\n');
    // Fast-forward only: a concurrent writer makes this fail instead of losing data.
    git(['push','--quiet','origin',`${commit}:refs/heads/${STATE_BRANCH}`]);
    this.head=commit;
  }
}
async function throttle() {
  const now=Date.now(), slot=Math.max(now,nextRequest);
  nextRequest=slot+1000/config.requestsPerSecond;
  if(slot>now)await sleep(slot-now);
}
async function bybit(path, params={}) {
  let last;
  for (const base of [apiBase, ...API_BASES.filter(x=>x!==apiBase)]) {
    const url=base+path+'?'+new URLSearchParams(params);
    for(let attempt=0;attempt<3;attempt++) {
      await throttle();
      let r;
      try {r=await fetch(url,{signal:AbortSignal.timeout(20000)});} catch (e) {last=e;if(attempt<2){await sleep(1000*(attempt+1));continue;}break;}
      if(r.status===403 || r.status===451) {last=Error(`BYBIT_BLOCKED: ${base} rejected this runner`);break;}
      if(r.status===429)throw Error('BYBIT_RATE_LIMIT: stop and wait for the next scheduled run');
      if(!r.ok) {last=Error(`Bybit HTTP ${r.status}`);if(r.status>=500 && attempt<2){await sleep(1500*(attempt+1));continue;}break;}
      let j;
      try {j=await r.json();} catch {last=Error('Bybit returned a non-JSON page; connectivity is not verified');break;}
      if(j.retCode!==0)throw Error(`Bybit API error ${j.retCode}`);
      apiBase=base; return j;
    }
  }
  throw Error(`BYBIT_BLOCKED: all official Bybit endpoints rejected this runner (${last?.message||'unknown error'})`);
}
async function symbols() {
  const all=[], seen=new Set();let cursor='';
  do {
    const j=await bybit('/v5/market/instruments-info',{category:'linear',limit:'1000',...(cursor?{cursor}:{})});
    if(!Array.isArray(j.result?.list))throw Error('Invalid instrument response');
    all.push(...j.result.list);
    cursor=j.result.nextPageCursor||'';
    if(cursor && seen.has(cursor))throw Error('Repeated instrument cursor');
    seen.add(cursor);
  } while(cursor);
  const names=[...new Set(all.filter(s=>s.status==='Trading'&&s.contractType==='LinearPerpetual'&&s.quoteCoin==='USDT'&&s.settleCoin==='USDT').map(s=>s.symbol))].sort();
  if(!names.length)throw Error('No USDT perpetual instruments received');
  const missing=config.symbols.filter(s=>!names.includes(s));
  if(missing.length)throw Error(`Configured symbols unavailable: ${missing.join(', ')}`);
  return config.symbols.length?config.symbols:names;
}
export function parseCandles(rows, ms, boundary) {
  const out=new Map();
  for(const a of rows) {
    const [t,open,high,low,close]=a.slice(0,5).map(Number);
    if(![t,open,high,low,close].every(Number.isFinite)||t<0||t%ms!==0||low<=0||high<Math.max(open,close)||low>Math.min(open,close)||high<low)throw Error('Invalid candle received');
    if(t+ms<=boundary)out.set(t,{t,open,high,low,close});
  }
  return [...out.values()].sort((a,b)=>a.t-b.t);
}
export function replay(rows, cursor, ms, c) {
  const s=engine.fresh(), events=[];
  for(const bar of rows) {
    for(const e of engine.process(s,bar,c.swingLen,c.maxWait)) {
      if(cursor!==undefined && bar.t>cursor)events.push({...e,confirmedAt:e.time+ms});
    }
  }
  return {last:rows.at(-1)?.t,events};
}
async function scanPair(symbol,tf,cursor,boundary) {
  const ms=TF[tf], target=Math.floor(boundary/ms)*ms-ms;
  if(cursor!==undefined && cursor>=target)return {skip:true};
  const cutoff=target-Math.floor(config.maxCatchupHours*3600000/ms)*ms;
  const effective=cursor===undefined?undefined:Math.max(cursor,cutoff);
  // Rebuild all potentially active setups before the first unprocessed candle.
  const warm=config.maxWait+config.swingLen+10;
  const first=effective===undefined?target-warm*ms:effective-warm*ms;
  let end=target, rows=[];
  while(end>=first) {
    const limit=Math.min(1000,Math.floor((end-first)/ms)+1);
    const j=await bybit('/v5/market/kline',{category:'linear',symbol,interval:tf,start:String(Math.max(0,first)),end:String(end),limit:String(limit)});
    if(!Array.isArray(j.result?.list))throw Error('Invalid kline response');
    const page=j.result.list;
    if(!page.length)break;
    const oldest=Math.min(...page.map(a=>Number(a[0])));
    if(!Number.isFinite(oldest)||oldest>end)throw Error('Non-advancing kline page');
    rows.push(...page);end=oldest-1;
    if(page.length<limit)break;
  }
  const bars=parseCandles(rows,ms,boundary);
  if(!bars.length||bars.at(-1).t!==target)throw Error('Latest closed candle unavailable; cursor unchanged');
  for(let i=1;i<bars.length;i++)if(bars[i].t-bars[i-1].t!==ms)throw Error('Candle gap detected; cursor unchanged');
  if(effective!==undefined && bars[0].t>effective && cursor>=cutoff)throw Error('Catch-up history incomplete; cursor unchanged');
  const result=replay(bars,effective,ms,config);
  return {...result,truncated:cursor!==undefined&&cursor<cutoff};
}
function note(state,id,text) {if(!state.notes.some(n=>n.id===id))state.notes.push({id,text});}
export function addEvents(state,symbol,tf,events) {
  const existing=new Set(state.outbox.map(e=>e.id));
  for(const e of events) {
    const id=`${symbol}/${tf}/${e.time}/${e.side}`;
    if(!existing.has(id)) {state.outbox.push({...e,id,symbol,tf});existing.add(id);}
  }
}
function textFor(e) {
  const date=new Date(e.confirmedAt).toLocaleString('en-GB',{timeZone:'Asia/Tehran',hour12:false});
  const lag=Math.max(0,Math.floor((Date.now()-e.confirmedAt)/60000));
  return `${e.symbol} | ${e.tf==='D'?'1D':e.tf+'m'} | ${e.side}\nقیمت بسته‌شدن: ${e.price}\nسطح شکست کندل ۳: ${e.level}\nتأیید (تهران): ${date}\nتأخیر ارسال: ${lag} دقیقه\nID: ${e.id}`;
}
export async function telegram(text) {
  const token=process.env.TELEGRAM_BOT_TOKEN, chat=process.env.TELEGRAM_CHAT_ID;
  if(!token||!chat)throw Error('Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID secret');
  // Never log a fetch error object: it can contain the token in its URL.
  try {
    const r=await fetch(`https://api.telegram.org/bot${token}/sendMessage`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({chat_id:chat,text}),signal:AbortSignal.timeout(20000)});
    const j=await r.json();
    if(!r.ok||!j.ok)throw Error('Telegram rejected message');
  } catch {throw Error('Telegram delivery not confirmed; check bot secrets and /start. Pending messages retained.');}
}
async function flush(state,store) {
  // A crash after Telegram accepts but before checkpoint can repeat the last message.
  // Stable signal IDs identify that unavoidable cross-service delivery ambiguity.
  while(state.notes.length) {
    await telegram(state.notes[0].text);
    state.notes.shift();store.save(state);await sleep(1200);
  }
  state.outbox.sort((a,b)=>a.confirmedAt-b.confirmedAt||a.id.localeCompare(b.id));
  let count=0;
  while(state.outbox.length && count<100) {
    const batch=state.outbox.slice(0,8);
    await telegram(batch.map(textFor).join('\n\n──────────\n\n'));
    state.outbox.splice(0,batch.length);store.save(state);count++;
    await sleep(1200);
  }
}
export async function main() {
  config=validateConfig(JSON.parse(fs.readFileSync('config.json','utf8')));
  if(process.argv.includes('--check-connection')) {
    const clock=await bybit('/v5/market/time');
    const names=await symbols();
    for(const tf of config.timeframes) {
      const j=await bybit('/v5/market/kline',{category:'linear',symbol:'BTCUSDT',interval:tf,limit:'3'});
      const bars=parseCandles(j.result.list,TF[tf],Number(clock.time)-config.closeGraceMs);
      if(!bars.length)throw Error('No closed candles returned for '+tf);
      console.log(`Verified BTCUSDT ${tf}: ${bars.length} closed candles`);
    }
    console.log(`Bybit reachable: ${names.length} USDT perpetuals. This is a connection test, not an active scanner.`);
    return;
  }
  if(!process.env.TELEGRAM_BOT_TOKEN||!process.env.TELEGRAM_CHAT_ID)throw Error('Set the two Telegram Actions secrets before running');
  if(process.argv.includes('--telegram-test')) {await telegram('3C Reversal — اتصال تلگرام برقرار است. این پیام تست است؛ اسکن هنوز تأیید نشده.');return;}
  const store=new StateStore(), state=store.load();
  const fingerprint=createHash('sha256').update(JSON.stringify({engine:MARKER,swing:config.swingLen,wait:config.maxWait})).digest('hex');
  if(state.fingerprint && state.fingerprint!==fingerprint)throw Error('Strategy settings changed. See README before resetting scanner-state; pending alerts must not be lost.');
  state.fingerprint=fingerprint;
  // Probe first: fail clearly on restricted runners, with no endpoint hopping.
  let clock;
  try {clock=await bybit('/v5/market/time');}
  catch(e) {
    const id='bybit-connection-'+new Date().toISOString().slice(0,10);
    if(state.lastConnectionWarning!==id) {
      await telegram('اسکنر متوقف است: دریافت داده از Bybit در محیط GitHub تأیید نشد. Actions را بررسی کنید. این پیام سیگنال نیست.');
      state.lastConnectionWarning=id;store.save(state);
    }
    throw e;
  }
  const now=Number(clock.time??Number(clock.result?.timeSecond)*1000);
  if(!Number.isFinite(now)||Math.abs(now-Date.now())>300000)throw Error('Invalid/stale Bybit server clock');
  const boundary=now-config.closeGraceMs, names=await symbols();
  // Retry previously persisted messages before doing more work.
  await flush(state,store);
  const tasks=names.flatMap(symbol=>config.timeframes.map(tf=>({symbol,tf})));
  let pos=0,done=0,failed=0,initialized=0,truncated=0,fatal=null;
  const errors=[];
  const started=Date.now();
  await Promise.all(Array.from({length:config.concurrency},async()=>{
    while(pos<tasks.length&&!fatal) {
      const {symbol,tf}=tasks[pos++],key=symbol+'/'+tf;
      try {
        const previous=state.cursors[key];
        const r=await scanPair(symbol,tf,previous,boundary);
        if(!r.skip) {
          if(previous===undefined)initialized++;
          addEvents(state,symbol,tf,r.events);
          state.cursors[key]=r.last;
          if(r.truncated)truncated++;
        }
        done++;
      } catch(e) {
        failed++;if(errors.length<8)errors.push(`${key}: ${e.message}`);
        if(e.message.startsWith('BYBIT_'))fatal=e;
      }
      if((done+failed)%200===0)console.log(`Processed ${done+failed}/${tasks.length}; errors=${failed}`);
      if(Date.now()-started>22*60000)fatal=Error('Scan time budget reached; remaining pairs will retry next run');
    }
  }));
  // Drop only delisted/disabled pair cursors; outbox entries are retained until sent.
  const active=new Set(tasks.map(t=>t.symbol+'/'+t.tf));
  for(const key of Object.keys(state.cursors))if(!active.has(key))delete state.cursors[key];
  state.lastRun={at:now,symbols:names.length,pairs:tasks.length,done,failed,initialized,truncated,errors};
  if(initialized)note(state,'initial-'+now,`اسکنر: تاریخچهٔ ${initialized} ترکیب نماد/تایم‌فریم آماده شد. سیگنال‌های تاریخی ارسال نشدند. بازار: ${names.length} پرپچوال USDT؛ تایم‌فریم‌ها: ${config.timeframes.join(', ')}. موفق: ${done}؛ خطا: ${failed}.`);
  if(truncated)note(state,'gap-'+now,`هشدار: ${truncated} ترکیب بیش از ${config.maxCatchupHours} ساعت عقب افتاده بود. سیگنال‌های قدیمی‌تر بازیابی نشدند.`);
  if((failed||fatal)&&state.lastErrorDay!==new Date(now).toISOString().slice(0,10)) {
    note(state,'errors-'+now,`هشدار اسکن: ${failed} دریافت ناموفق؛ ${tasks.length-done-failed} مورد بررسی‌نشده. وضعیت اجرای GitHub را ببینید. دریافت ناموفق به معنی نبود سیگنال نیست.`);
    state.lastErrorDay=new Date(now).toISOString().slice(0,10);
  }
  store.save(state); // Persist cursor AND pending alerts before sending any new signal.
  await flush(state,store);
  console.log(JSON.stringify(state.lastRun));
  if(process.env.GITHUB_STEP_SUMMARY)fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY,`## 3C scanner\nSymbols: ${names.length} | Pairs: ${tasks.length} | Success: ${done} | Errors: ${failed} | Pending alerts: ${state.outbox.length}\n\n${errors.join('\n\n')}\n`);
  if(fatal)throw fatal;
  if(failed)throw Error(`${failed} pair fetches failed; their cursors were retained for retry`);
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href)main().catch(e=>{console.error(e.message);process.exitCode=1;});
