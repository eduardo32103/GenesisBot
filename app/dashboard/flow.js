const SUPABASE_URL="https://nwduaycuofeggjtfdwsy.supabase.co";
const ANON_KEY="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im53ZHVheWN1b2ZlZ2dqdGZkd3N5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzYxODQzMDYsImV4cCI6MjA5MTc2MDMwNn0.hNcMPS6P41Hhe4xtIAeaHA4x8PVtCq3YYgmz65yPp6o";
const ENDPOINT=SUPABASE_URL+"/functions/v1/market-flow";
const CONTEXT_ENDPOINT=SUPABASE_URL+"/functions/v1/market-flow-context";
const EVIDENCE_ENDPOINT=SUPABASE_URL+"/functions/v1/market-flow-evidence";
let asset="BTC";
const el=function(id){return document.getElementById(id)};
const headers={Authorization:"Bearer "+ANON_KEY,apikey:ANON_KEY,"Content-Type":"application/json"};
function num(v){const x=Number(v);return Number.isFinite(x)?x:null}
function money(v){const x=num(v);if(x===null)return"—";return new Intl.NumberFormat("en-US",{style:"currency",currency:"USD",maximumFractionDigits:x>1000?0:2}).format(x)}
function compact(v){const x=num(v);if(x===null)return"—";return new Intl.NumberFormat("en-US",{notation:"compact",maximumFractionDigits:2}).format(x)}
function pct(v){const x=num(v);return x===null?"—":(x*100).toFixed(4)+"%"}
function bps(v){const x=num(v);return x===null?"—":(x>=0?"+":"")+x.toFixed(2)+" bps"}
function age(ts){const d=(Date.now()-new Date(ts).getTime())/60000;if(d<1)return"Ahora";if(d<60)return"hace "+Math.round(d)+" min";return"hace "+Math.round(d/60)+" h"}
function lineChart(node,values,zero){const nums=values.map(num).filter(function(v){return v!==null});if(nums.length<2){node.innerHTML="";return}let min=Math.min.apply(null,nums),max=Math.max.apply(null,nums);if(min===max){min-=1;max+=1}const pad=(max-min)*.12;min-=pad;max+=pad;const pts=nums.map(function(v,i){const x=8+i*(344/Math.max(nums.length-1,1));const y=108-((v-min)/(max-min))*96;return[x,y]});const poly=pts.map(function(p){return p.join(",")}).join(" ");let z="";if(zero&&min<0&&max>0){const zy=108-((0-min)/(max-min))*96;z='<line class="zero" x1="0" x2="360" y1="'+zy+'" y2="'+zy+'"/>'}node.innerHTML=z+'<polyline class="chart-line" points="'+poly+'"/>'}
function reading(latest,history){const f=num(latest.funding_rate)||0,oi=num(latest.open_interest)||0,prem=num(latest.coinbase_premium_bps);const old=history[0],oldOi=num(old&&old.open_interest);const oiMove=oldOi?((oi-oldOi)/oldOi)*100:null;const parts=[];if(Math.abs(f)<0.00005)parts.push("funding contenido");else if(f>0)parts.push("largos pagando funding elevado");else parts.push("cortos pagando funding elevado");if(oiMove!==null)parts.push("OI "+(oiMove>=0?"sube ":"baja ")+Math.abs(oiMove).toFixed(1)+"% en la ventana visible");if(asset==="BTC"&&prem!==null)parts.push(prem>1?"Coinbase cotiza con prima":prem<-1?"Coinbase cotiza con descuento":"prima Coinbase casi neutra");return parts.join("; ")+". Esto describe posicionamiento y presión relativa; todavía no implica ventaja predictiva hasta backtestearlo."}
async function request(method){const r=await fetch(ENDPOINT+"?asset="+asset+"&limit=144",{method:method||"GET",headers:headers});if(!r.ok)throw new Error("HTTP "+r.status);return r.json()}
async function load(force){el("freshness").textContent="Actualizando…";try{if(force)await request("POST");const data=await request("GET");const rows=data.snapshots||[],latest=rows[0];if(!latest)throw new Error("Sin snapshots");el("mark-price").textContent=money(latest.mark_price);el("funding").textContent=pct(latest.funding_rate);el("open-interest").textContent=compact(latest.open_interest)+(asset==="BTC"?" BTC":"");el("coinbase-premium").textContent=asset==="BTC"?bps(latest.coinbase_premium_bps):"Solo BTC";el("premium-card").style.opacity=asset==="BTC"?"1":".48";el("freshness").textContent=age(latest.observed_at);const chronological=rows.slice().reverse();lineChart(el("funding-chart"),chronological.map(function(x){return x.funding_rate}),true);lineChart(el("oi-chart"),chronological.map(function(x){return x.open_interest}),false);el("flow-reading").textContent=reading(latest,chronological)}catch(e){el("freshness").textContent="Error";el("flow-reading").textContent="No pude cargar flujo: "+e.message}}
document.querySelectorAll(".asset-tab").forEach(function(btn){btn.addEventListener("click",function(){asset=btn.dataset.asset;document.querySelectorAll(".asset-tab").forEach(function(x){x.classList.toggle("active",x===btn)});load(false)})});
function evidenceStatus(status){
  const map={rejected:["Rechazado","bad"],inconclusive:["Inconcluso","warn"],supported:["Validado","good"],collecting:["Recopilando","info"],insufficient:["Muestra corta","info"],blocked:["Bloqueado","muted"]};
  return map[status]||[status||"Pendiente","muted"];
}
async function loadEvidence(){
  try{
    const r=await fetch(EVIDENCE_ENDPOINT,{headers:headers});
    if(!r.ok)throw new Error("HTTP "+r.status);
    const data=await r.json(),rows=data.results||[];
    el("evidence-list").innerHTML=rows.map(function(x){
      const s=evidenceStatus(x.status),metric=num(x.metric_value),t=num(x.t_stat);
      let stats="";
      if(metric!==null)stats+="<b>"+(metric>=0?"+":"")+metric.toFixed(3)+"%</b>";
      if(t!==null)stats+="<span>t "+t.toFixed(2)+"</span>";
      if(x.sample_size)stats+="<span>n "+x.sample_size+"</span>";
      return '<article class="evidence-row"><div><span class="evidence-pill '+s[1]+'">'+s[0]+'</span><strong>'+x.title+'</strong></div><div class="evidence-stats">'+stats+'</div></article>';
    }).join("")||'<div class="evidence-row">Sin resultados todavía.</div>';
  }catch(e){el("evidence-list").innerHTML='<div class="evidence-row">No pude cargar evidencia.</div>'}
}

async function loadContext(force){
  if(asset==="SOL"){
    el("put-call-ratio").textContent="—";
    el("top-strikes").innerHTML='<div><span>Deribit</span><b>Sin feed SOL configurado</b></div>';
    el("etf-card").style.display="none";
    el("cot-card").style.display="none";
    return;
  }
  try{
    const method=force?"POST":"GET";
    const r=await fetch(CONTEXT_ENDPOINT+"?asset="+asset,{method:method,headers:headers});
    if(!r.ok)throw new Error("HTTP "+r.status);
    const data=await r.json(),bundle=data.bundle||{};
    const options=bundle.options||null;
    if(options){
      el("put-call-ratio").textContent=num(options.put_call_oi_ratio)===null?"—":Number(options.put_call_oi_ratio).toFixed(2);
      const top=(options.top_strikes||[]).slice(0,4);
      el("top-strikes").innerHTML=top.map(function(x){
        return '<div><span>'+money(x.strike)+' '+String(x.type||"").toUpperCase()+'</span><b>'+compact(x.open_interest)+' OI</b></div>'
      }).join("");
    }else{
      el("put-call-ratio").textContent="—";el("top-strikes").innerHTML="";
    }
    if(asset==="BTC"){
      const etf=(bundle.etf||[])[0],cot=(bundle.cot||[])[0];
      el("etf-card").style.display="";
      el("cot-card").style.display="";
      el("etf-flow").textContent=etf?(Number(etf.total_flow_usd_m)>=0?"+":"")+Number(etf.total_flow_usd_m).toFixed(1)+"M":"—";
      el("etf-date").textContent=etf?etf.flow_date+" · USD millones":"Flujo diario";
      el("cot-net").textContent=cot?(Number(cot.noncommercial_net)>=0?"+":"")+new Intl.NumberFormat("en-US").format(Number(cot.noncommercial_net)):"—";
      el("cot-date").textContent=cot?cot.report_date+" · neto no comercial":"COT semanal";
    }else{
      el("etf-card").style.display="none";
      el("cot-card").style.display="none";
    }
  }catch(e){
    el("put-call-ratio").textContent="Error";
  }
}
document.querySelectorAll(".asset-tab").forEach(function(btn){
  btn.addEventListener("click",function(){setTimeout(function(){loadContext(false)},0)})
});
el("refresh-button").addEventListener("click",function(){load(true);loadContext(true)});
load(false);loadContext(false);loadEvidence();
setInterval(function(){load(false);loadContext(false);loadEvidence()},60000);