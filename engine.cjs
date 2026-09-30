(function(root){
function fresh(){return {index:-1,lastTime:0,buf:[],red:[],green:[],signals:[]};}
function process(st,c,swingLen=5,maxWait=150){
 if(c.t<=st.lastTime)return [];
 st.index++;st.lastTime=c.t;const b=st.index,prev=st.buf.at(-1),events=[];
 // red holds C0-red / three-green BUY setups; green holds the SELL mirror.
 for(const type of ['red','green']){
  const a=st[type],buy=type==='red';
  for(let i=a.length-1;i>=0;i--){const p=a[i];let remove=b-p.bar>maxWait;
   if(!remove){
    if(p.state===0){
     // Check wick violation BEFORE checking C0, including the C0-touch bar.
     if(prev&&(buy?c.high>prev.high:c.low<prev.low))remove=true;
     else if(buy?c.low<=p.c0:c.high>=p.c0){p.state=1;p.touchTime=c.t;}
    }else if(buy?c.high>p.c3:c.low<p.c3){
     events.push({side:buy?'BUY':'SELL',time:c.t,price:c.close,level:p.c3,flipped:true,setupTime:p.time,touchTime:p.touchTime});remove=true;
    }
   }
   if(remove)a.splice(i,1);
  }
 }
 st.buf.push(c);if(st.buf.length>swingLen+5)st.buf.shift();
 if(st.buf.length>=swingLen+5){
  const n=st.buf.length,[c0,c1,c2,c3,c4]=st.buf.slice(-5),prior=st.buf.slice(n-5-swingLen,n-5);
  const red=x=>x.close<x.open,green=x=>x.close>x.open;
  // Original pattern and swing filters are preserved.
  const buyPattern=red(c0)&&green(c1)&&green(c2)&&green(c3)&&red(c4)&&c1.low>c0.low&&c0.low<Math.min(...prior.map(x=>x.low))&&c3.high>Math.max(c4.high,c2.high,c1.high,c0.high);
  const sellPattern=green(c0)&&red(c1)&&red(c2)&&red(c3)&&green(c4)&&c1.high<c0.high&&c0.high>Math.max(...prior.map(x=>x.high))&&c3.low<Math.min(c4.low,c2.low,c1.low,c0.low);
  for(const [ok,buy]of [[buyPattern,true],[sellPattern,false]])if(ok){
   const touched=buy?c.low<=c0.low:c.high>=c0.high;
   st[buy?'red':'green'].push({bar:b-4,time:c0.t,c0:buy?c0.low:c0.high,c3:buy?c3.high:c3.low,state:touched?1:0,touchTime:touched?c.t:null});
  }
 }
 const unique=events.filter((x,i)=>events.findIndex(y=>y.side===x.side)===i);
 st.signals=[...unique,...st.signals].slice(0,10);return unique;
}
const api={fresh,process};if(typeof module!=='undefined')module.exports=api;else root.FlipEngine=api;
})(globalThis);

