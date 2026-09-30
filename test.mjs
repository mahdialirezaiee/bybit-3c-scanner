import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import engine from './engine.cjs';
import {TF,validateConfig,parseCandles,replay,addEvents,StateStore} from './scanner.mjs';
const config={timeframes:Object.keys(TF),symbols:[],swingLen:5,maxWait:150,maxCatchupHours:48,requestsPerSecond:12,concurrency:8,closeGraceMs:5000};
const candle=(i,open,high,low,close)=>({t:(i+1)*60000,open,high,low,close});
const fixture=()=>[
  ...Array.from({length:5},(_,i)=>candle(i,11,13,10,12)),
  candle(5,10,11,8,9), // C0 red, swing low
  candle(6,9,11,9,10), candle(7,10,12,9.5,11), candle(8,11,15,10,14),
  candle(9,13,14,8,10), // C4 clean, C0 touch; no entry
  candle(10,10,16,9,15) // reversal through C3 => BUY
];
const mirror=rows=>rows.map(c=>({...c,open:30-c.open,high:30-c.low,low:30-c.high,close:30-c.close}));
test('BUY and mirrored SELL only after touch, identical strategy both directions',()=>{
  for(const [bars,side] of [[fixture(),'BUY'],[mirror(fixture()),'SELL']]) {
    const s=engine.fresh();let events=[];
    for(let i=0;i<10;i++)events.push(...engine.process(s,bars[i],5,150));
    assert.equal(events.length,0);
    assert.equal(engine.process(s,bars[10],5,150)[0].side,side);
    assert.equal(engine.process(s,bars[10],5,150).length,0);
  }
});
function seeded(buy) {
  const s=engine.fresh();s.index=9;s.lastTime=600000;
  s.buf=[candle(9,11,14,9,12)];
  s[buy?'red':'green']=[{bar:5,time:360000,c0:buy?8:18,c3:buy?15:7,state:0,touchTime:null}];
  return s;
}
test('wick violation invalidates even when touching C0 on same candle',()=>{
  for(const buy of [true,false]) {
    const s=seeded(buy);
    const c=buy?candle(10,10,15,7,9):candle(10,12,19,8,17);
    assert.deepEqual(engine.process(s,c),[]);
    assert.equal(s.red.length+s.green.length,0);
  }
});
test('C3 break without C0 touch never signals; no invalidation flip',()=>{
  const s=seeded(true);
  assert.deepEqual(engine.process(s,candle(10,12,16,9,15)),[]);
  assert.equal(s.red.length,0);
  assert.deepEqual(engine.process(s,candle(11,14,17,7,16)),[]);
});
test('touch equality allowed, entry strictly breaks and must be a later candle',()=>{
  const s=seeded(true);
  assert.deepEqual(engine.process(s,candle(10,10,14,8,9)),[]);
  assert.equal(s.red[0].state,1);
  assert.deepEqual(engine.process(s,candle(11,10,15,9,14)),[]);
  assert.equal(engine.process(s,candle(12,14,16,10,15))[0].side,'BUY');
});
test('expired setup cannot signal',()=>{
  const s=seeded(true);s.index=156;s.red[0].state=1;
  assert.deepEqual(engine.process(s,candle(160,14,16,9,15),5,150),[]);
  assert.equal(s.red.length,0);
});
test('first bootstrap suppresses history; delayed runs recover signal and checkpoint deduplicates',()=>{
  const bars=fixture();
  assert.equal(replay(bars,undefined,60000,config).events.length,0);
  const r=replay(bars,bars[8].t,60000,config);
  assert.equal(r.events.length,1);assert.equal(r.events[0].confirmedAt,bars[10].t+60000);
  assert.equal(replay(bars,r.last,60000,config).events.length,0);
  const state={outbox:[]};addEvents(state,'BTCUSDT','1',r.events);addEvents(state,'BTCUSDT','1',r.events);
  assert.equal(state.outbox.length,1);
});
test('forming candles excluded, ordering normalized, bad OHLC rejected',()=>{
  const raw=fixture().map(c=>[c.t,c.open,c.high,c.low,c.close].map(String)).reverse();
  assert.equal(parseCandles(raw,60000,fixture()[10].t+59999).length,10);
  assert.equal(parseCandles(raw,60000,fixture()[10].t+60000).length,11);
  assert.throws(()=>parseCandles([['60000','12','10','9','12']],60000,999999));
});
test('all eight timeframes enabled and excessive request rate rejected',()=>{
  assert.equal(validateConfig(config).timeframes.length,8);
  assert.throws(()=>validateConfig({...config,requestsPerSecond:121}));
});
test('durable git checkpoint roundtrip, outbox preserved without changing main',()=>{
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'scanner-test-'));
  const cwd=process.cwd();
  const g=(args)=>execFileSync('git',args,{encoding:'utf8',stdio:['pipe','pipe','pipe']}).trim();
  try {
    g(['init','--bare',path.join(dir,'origin.git')]);
    fs.mkdirSync(path.join(dir,'work'));process.chdir(path.join(dir,'work'));
    g(['init','-b','main']);g(['remote','add','origin',path.join(dir,'origin.git')]);
    const a=new StateStore(),state=a.load();state.cursors['BTCUSDT/1']=60000;
    state.outbox.push({id:'example-pending'});a.save(state);
    const b=new StateStore(),again=b.load();assert.deepEqual(again,state);
    again.outbox=[];b.save(again);
    assert.equal(new StateStore().load().outbox.length,0);
    assert.equal(g(['ls-remote','--heads','origin','main']),'');
  } finally {process.chdir(cwd);fs.rmSync(dir,{recursive:true,force:true});}
});
test('full run persists failed Telegram signal and retries without replay duplicates',async()=>{
  const {main}=await import('./scanner.mjs');
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'scanner-flow-')),cwd=process.cwd();
  const originalFetch=globalThis.fetch, originalNow=Date.now;
  const oldToken=process.env.TELEGRAM_BOT_TOKEN,oldChat=process.env.TELEGRAM_CHAT_ID,oldSummary=process.env.GITHUB_STEP_SUMMARY;
  const g=args=>execFileSync('git',args,{encoding:'utf8',stdio:['pipe','pipe','pipe']});
  let now=665000, failSignals=false, accepted=[];
  Date.now=()=>now;
  process.env.TELEGRAM_BOT_TOKEN='test-placeholder';process.env.TELEGRAM_CHAT_ID='test-placeholder';delete process.env.GITHUB_STEP_SUMMARY;
  globalThis.fetch=async(url,opts)=>{
    const u=new URL(url);
    if(u.hostname==='api.telegram.org') {
      const text=JSON.parse(opts.body).text;
      if(failSignals&&text.includes('ID:'))throw Error('simulated delivery failure');
      accepted.push(text);return {ok:true,json:async()=>({ok:true})};
    }
    let j={retCode:0,time:now,result:{}};
    if(u.pathname.endsWith('/instruments-info'))j.result={list:[{symbol:'BTCUSDT',status:'Trading',contractType:'LinearPerpetual',quoteCoin:'USDT',settleCoin:'USDT'}]};
    if(u.pathname.endsWith('/kline')) {
      j.result={list:fixture().filter(c=>c.t<=Number(u.searchParams.get('end'))).reverse().slice(0,Number(u.searchParams.get('limit'))).map(c=>[c.t,c.open,c.high,c.low,c.close].map(String))};
    }
    return {ok:true,status:200,json:async()=>j};
  };
  try {
    g(['init','--bare',path.join(dir,'origin.git')]);fs.mkdirSync(path.join(dir,'work'));process.chdir(path.join(dir,'work'));
    g(['init','-b','main']);g(['remote','add','origin',path.join(dir,'origin.git')]);
    fs.writeFileSync('config.json',JSON.stringify({...config,timeframes:['1'],symbols:['BTCUSDT']}));
    await main();assert.equal(new StateStore().load().cursors['BTCUSDT/1'],600000);
    assert.equal(accepted.filter(t=>t.includes('ID:')).length,0);
    now=725000;failSignals=true;
    await assert.rejects(main(),/Telegram delivery/);
    assert.equal(new StateStore().load().outbox.length,1);
    failSignals=false;await main();
    assert.equal(new StateStore().load().outbox.length,0);
    assert.equal(accepted.filter(t=>t.includes('ID:')).length,1);
  } finally {
    process.chdir(cwd);globalThis.fetch=originalFetch;Date.now=originalNow;
    for(const [key,val] of [['TELEGRAM_BOT_TOKEN',oldToken],['TELEGRAM_CHAT_ID',oldChat],['GITHUB_STEP_SUMMARY',oldSummary]])if(val===undefined)delete process.env[key];else process.env[key]=val;
    fs.rmSync(dir,{recursive:true,force:true});
  }
});
