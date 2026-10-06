"""Finite, research-only two-mechanism pilot. No broker, secrets, or DB access.
Frozen 2026-10-06 before market replay. Python standard library only.
"""
from __future__ import annotations
import argparse, csv, hashlib, io, json, math, statistics, sys, time, urllib.request, zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

START=1727740800
END=1740787200
MONTHS=('2024-10','2024-11','2024-12','2025-01','2025-02')
SYMBOLS=('BTCUSDT','ETHUSDT')
ROOT='https://data.binance.vision/data/futures/um/monthly/'
PROTOCOL={
 'id':'GENESIS_PUBLIC_CUSUM_DUALTHRUST_FIXED_RR3_20261006',
 'stage':'separate_reused_calendar_development_pilot_only',
 'registry_novelty':'unconfirmed; Supabase connection unauthorized; do not count as verified new families',
 'symbols':SYMBOLS,'months':MONTHS,'start':START,'end_exclusive':END,
 'source':'Binance official USD-M perpetual monthly 1m klines; SHA256 CHECKSUM required',
 'signals':'15m built only from exactly15 contiguous 1m bars; UTC calendar dates',
 'CUSUM':{'z':'current close difference / sample std of20 PREVIOUS close differences; target0',
          'k':0.5,'h':5,'reset':'both sums reset at UTC date boundary or either threshold crossing',
          'stop':'extreme of last4 completed same-session15m bars including signal',
          'source':'https://www.itl.nist.gov/div898/software/dataplot/refman1/auxillar/cusum.htm'},
 'DUAL_THRUST':{'lookback_days':4,'k_long':0.5,'k_short':0.5,
          'range':'max(highest_high-lowest_close,highest_close-lowest_low), previous4 full UTC days',
          'signal':'15m close crosses above day_open+0.5range or below day_open-0.5range',
          'stop':'extreme of last4 completed same-session15m bars including signal',
          'next_open':'must remain beyond the signal-side frozen band',
          'source':'https://www.quantconnect.com/research/15258/dual-thrust-trading-algorithm/'},
 'execution':'next exact15m open in SAME UTC date; cap1 accepted entry per date per asset; one concurrent position; no entry on exit minute of existing position',
 'target':'fixed +3R from reference opening fill; fixed structural stop; no partials/BE/trailing/time exits; adverse open gaps not clipped',
 'ambiguity':'1m stop-first if both barriers; no future entry-minute H/L in admission; missing1m aborts dataset, not missing-trade deletion',
 'costs':{'fee_per_leg':0.0005,'liquidity_buffer_per_leg':0.0001,'planned_RT_cost_over_R_max':0.15,
          'stress':2,'funding':'adverse-only reserve: sum abs(published funding rate)*trade-minute high / initial risk; inclusive charge boundaries. No funding credits. Not exact mark-price settlement.',
          'note':'Modeled reference fills; not brokerage-calibrated. No discount or leverage assumed.'},
 'gates':{'min_closed':300,'min_category_side':50,'pf1_overall_sides':1.20,'pf2_overall_sides_asset_sides':1.15,'min_mean1':0.05,'min_t1_descriptive':2.58,'positive_calendar_thirds_both_costs':True,'intraday_min':0.70,'pending_loss_sensitivity_positive':True},
 'full_verification_blockers':['only crypto category and2assets','no independent validation/holdout/unseen/years/forward stage','not broker-calibrated fills/funding/impact','registry deduplication unavailable'],
 'policy':'Two fixed hypotheses, not parameter sweep. No result-based parameter/asset/side rescue. Even pilot pass cannot open protected periods.',
 'protected_stages_opened':False,'broker_touched':False,'order_executed':False,'order_policy':'journal_only_no_broker','production_allowed':False,
}

def fingerprint(obj):
 return hashlib.sha256(json.dumps(obj,sort_keys=True,separators=(',',':')).encode()).hexdigest()

def fetch_one(key):
 symbol,month,kind=key
 filename=f'{symbol}-1m-{month}.zip' if kind=='klines' else f'{symbol}-fundingRate-{month}.zip'
 url=ROOT+(f'klines/{symbol}/1m/' if kind=='klines' else f'fundingRate/{symbol}/')+filename
 def get(u):
  req=urllib.request.Request(u,headers={'User-Agent':'GENESIS-research-audit/1.0'})
  with urllib.request.urlopen(req,timeout=35) as r: return r.read()
 data=get(url); chk=get(url+'.CHECKSUM').decode().split()[0].lower()
 actual=hashlib.sha256(data).hexdigest()
 if actual!=chk: raise ValueError('CHECKSUM mismatch '+filename)
 with zipfile.ZipFile(io.BytesIO(data)) as z:
  names=z.namelist()
  if len(names)!=1: raise ValueError('Unexpected archive contents')
  raw=z.read(names[0]).decode('utf-8-sig')
 rows=list(csv.reader(io.StringIO(raw)))
 if not rows: raise ValueError('Empty archive '+filename)
 if not rows[0][0].isdigit(): rows=rows[1:]
 manifest={'url':url,'sha256':actual,'bytes':len(data),'rows':len(rows),'checksum_verified':True}
 return key,rows,manifest

def validate(bars,step=60,require_period=True):
 if not bars: raise ValueError('No data')
 if require_period and (bars[0][0]!=START or bars[-1][0]!=END-step): raise ValueError('Incomplete requested period')
 for i,b in enumerate(bars):
  t,o,h,l,c=b
  if not isinstance(t,int) or t%step or not all(math.isfinite(x) and x>0 for x in (o,h,l,c)) or h<max(o,l,c) or l>min(o,h,c): raise ValueError('Invalid OHLC/time')
  if i and t-bars[i-1][0]!=step: raise ValueError('Missing/duplicate/unordered bar; dataset blocked')

def aggregate(bars):
 out=[]
 if len(bars)%15: raise ValueError('Incomplete15m')
 for i in range(0,len(bars),15):
  b=bars[i:i+15]
  if b[0][0]%900 or b[-1][0]-b[0][0]!=840: raise ValueError('Noncontiguous15m')
  out.append((b[0][0],b[0][1],max(x[2] for x in b),min(x[3] for x in b),b[-1][4]))
 return out

def signals(bars,arm):
 if arm not in ('CUSUM','DUAL_THRUST'): raise ValueError('Unregistered arm')
 orders={}; diffs=[]; p=n=0.; prior_day=None; prev_close=None; daybars=[]; days=[]; current_open=None; band=None
 for b in bars:
  t,o,h,l,c=b; day=t//86400
  if day!=prior_day:
   if daybars and len(daybars)==96:
    days.append((prior_day,max(x[2] for x in daybars),min(x[3] for x in daybars),daybars[-1][4]))
   daybars=[];p=n=0.;current_open=o;band=None
   if len(days)>=4 and [x[0] for x in days[-4:]]==list(range(day-4,day)):
    hist=days[-4:];r=max(max(x[1] for x in hist)-min(x[3] for x in hist),max(x[3] for x in hist)-min(x[2] for x in hist))
    if r>0: band=(o+0.5*r,o-0.5*r)
   prior_day=day
  side=0;boundary=None
  if arm=='CUSUM' and prev_close is not None and len(diffs)>=20:
   sd=statistics.stdev(diffs[-20:])
   if sd>0:
    z=(c-prev_close)/sd;p=max(0.,p+z-.5);n=max(0.,n-z-.5)
    if p>5 or n>5:
     side=1 if p>5 else -1;p=n=0.
  elif arm=='DUAL_THRUST' and band is not None:
   pc=daybars[-1][4] if daybars else current_open
   if pc<=band[0] and c>band[0]: side=1;boundary=band[0]
   elif pc>=band[1] and c<band[1]: side=-1;boundary=band[1]
  if prev_close is not None: diffs.append(c-prev_close)
  prev_close=c;daybars.append(b)
  if side and len(daybars)>=4 and (t+900)//86400==day:
   sl=min(x[3] for x in daybars[-4:]) if side==1 else max(x[2] for x in daybars[-4:])
   orders[t+900]={'side':side,'stop':sl,'boundary':boundary,'signal_t':t+900,'day':day}
 return orders

def trade_exit(b,pos):
 t,o,h,l,c=b;s=pos['side'];ep=pos['entry'];sl=pos['stop'];tp=pos['target'];risk=pos['risk']
 if s*(o-sl)<=0: xp=o; reason='stop_open';amb=False
 elif s*(o-tp)>=0: xp=tp;reason='target_open';amb=False
 else:
  hitstop=l<=sl if s==1 else h>=sl
  hittarget=h>=tp if s==1 else l<=tp
  if not hitstop and not hittarget: return None
  xp=sl if hitstop else tp;reason='stop_range' if hitstop else 'target_range';amb=hitstop and hittarget
 g=s*(xp-ep)/risk
 return {'exit_t':t,'exit_price':xp,'g':g,'reason':reason,'ambiguity':amb,'fee_R':.0005*(ep+xp)/risk,'liquidity_R':.0001*(ep+xp)/risk,'intraday':pos['entry_t']//86400==(t+(0 if reason.endswith('open') else 59))//86400}

def simulate(bars,orders,funding):
 pos=None;lastentry=None;trades=[];fund_by_t={t:r for t,r in funding};cost_veto=0;boundary_veto=0;qualified=0;occupancy_veto=0
 for b in bars:
  t,o,h,l,c=b;had=pos is not None;day=t//86400
  q=orders.get(t)
  if q:
   s=q['side'];sl=q['stop'];risk=s*(o-sl)
   if q['day']!=day: continue
   if q['boundary'] is not None and s*(o-q['boundary'])<=0: boundary_veto+=1
   elif risk>0 and .0012*o/risk<=.15:
    qualified+=1
    if not had and day!=lastentry:
     pos={'entry_t':t,'entry':o,'side':s,'stop':sl,'risk':risk,'target':o+s*3*risk,'funding_reserve_R':0.};lastentry=day
    else: occupancy_veto+=1
   else: cost_veto+=1
  if pos:
   if t in fund_by_t: pos['funding_reserve_R']+=abs(fund_by_t[t])*h/pos['risk']
   ex=trade_exit(b,pos)
   if ex:
    tr={**pos,**ex}
    friction=tr['fee_R']+tr['liquidity_R']+tr['funding_reserve_R']
    tr['net1']=tr['g']-friction;tr['net2']=tr['g']-2*friction
    trades.append(tr);pos=None
 if pos: trades.append({**pos,'exit_t':None,'exit_price':None,'g':None,'net1':None,'net2':None,'intraday':False,'reason':'pending','ambiguity':False})
 return trades,{'signals':len(orders),'cost_veto':cost_veto,'boundary_veto':boundary_veto,'cost_qualified':qualified,'occupancy_or_dailycap_veto':occupancy_veto}

def metrics(trades):
 closed=[t for t in trades if t['g'] is not None];n=len(closed);out={'entries':len(trades),'closed':n,'pending':len(trades)-n,'wins':sum(t['g']>0 for t in closed),'winrate':sum(t['g']>0 for t in closed)/n if n else None,'intraday_share':sum(t['intraday'] for t in closed)/len(trades) if trades else 0.,'ambiguous_1m':sum(t['ambiguity'] for t in closed),'stop_gaps':sum(t['g'] < -1-1e-8 for t in closed)}
 for m in (1,2):
  ns=[t[f'net{m}'] for t in closed];p=sum(max(x,0) for x in ns);loss=-sum(min(x,0) for x in ns)
  thirds=[sum(t[f'net{m}'] for t in closed if min(2,int(3*(t['entry_t']-START)/(END-START)))==k) for k in range(3)]
  tstat=statistics.mean(ns)/(statistics.stdev(ns)/math.sqrt(n)) if n>1 and statistics.stdev(ns)>0 else None
  sensitivity=sum(ns)+sum(-1-m*(.0012*t['entry']/t['risk']+t['funding_reserve_R']) for t in trades if t['g'] is None)
  out[str(m)]={'PF':p/loss if loss else None,'PF_infinite':loss==0 and p>0,'mean_R':statistics.mean(ns) if ns else None,'total_R':sum(ns),'calendar_thirds_R':thirds,'descriptive_t':tstat,'pending_nominal_loss_sensitivity_R':sensitivity}
 return out

def gates(cells):
 reasons=[];ov=cells['overall']
 if ov['closed']<300: reasons.append('closed_below300')
 for name,cell in cells.items():
  if name in ('long','short') and cell['closed']<50: reasons.append(name+':category_side_below50')
  if not cell['closed']: reasons.append(name+':no_closed_sample')
  for m,threshold in ((1,1.20),(2,1.15)):
   if m==1 and name not in ('overall','long','short'): continue
   pf=cell[str(m)]['PF']
   if pf is None or pf<threshold: reasons.append(name+f':PF{m}_below{threshold}')
 if ov['1']['mean_R'] is None or ov['1']['mean_R']<.05: reasons.append('mean1_below0.05')
 if ov['1']['descriptive_t'] is None or ov['1']['descriptive_t']<2.58: reasons.append('descriptive_t_below2.58')
 if ov['intraday_share']<.7: reasons.append('intraday_below70pct')
 for m in (1,2):
  if any(x<=0 for x in ov[str(m)]['calendar_thirds_R']): reasons.append(f'nonpositive_third_cost{m}')
  if ov[str(m)]['pending_nominal_loss_sensitivity_R']<=0: reasons.append(f'nonpositive_pending_sensitivity_cost{m}')
 return reasons

def tests():
 # Synthetic fixtures test execution; these are not market profitability evidence.
 names=[]
 def check(ok,name):
  if not ok: raise AssertionError(name)
  names.append(name)
 p={'entry_t':START,'entry':100.,'side':1,'stop':90.,'target':130.,'risk':10.,'funding_reserve_R':0.}
 q={**p,'side':-1,'stop':110.,'target':70.}
 check(trade_exit((START,100,130,95,125),p)['g']==3,'long_fixed3R')
 check(trade_exit((START,100,105,70,75),q)['g']==3,'short_fixed3R')
 check(trade_exit((START,100,131,89,100),p)['g']==-1,'both_barriers_stop_first')
 check(trade_exit((START,80,90,79,85),p)['g']==-2,'adverse_gap_not_clipped')
 check(trade_exit((START,140,145,130,141),p)['g']==3,'favorable_target_gap_capped')
 check(trade_exit((START,100,110,95,105),p) is None,'no_time_exit')
 order={START:{'side':1,'stop':90,'boundary':None,'day':START//86400,'signal_t':START}}
 ts,_=simulate([(START,100,131,89,100)],order,[])
 check(len(ts)==1 and ts[0]['g']==-1,'entry_admission_does_not_read_future_range')
 ts,_=simulate([(START,100,110,95,105)],order,[])
 check(ts[0]['g'] is None and ts[0]['net1'] is None,'pending_not_fabricated_loss')
 highfee={START:{'side':1,'stop':99.9,'boundary':None,'day':START//86400,'signal_t':START}}
 ts,diag=simulate([(START,100,110,95,105)],highfee,[])
 check(not ts and diag['cost_veto']==1,'planned_cost_veto')
 twice={**order,START+60:order[START]}
 ts,_=simulate([(START,100,130,95,125),(START+60,100,130,95,125)],twice,[])
 check(len(ts)==1,'cap_one_entry_day')
 try: validate([(START,100,101,99,100),(START+120,100,101,99,100)],require_period=False);ok=False
 except ValueError: ok=True
 check(ok,'missing_data_fails_closed')
 try: validate([(START,100,90,99,100)],require_period=False);ok=False
 except ValueError: ok=True
 check(ok,'invalid_ohlc_rejected')
 ts,_=simulate([(START,100,130,95,125)],order,[(START,.0001)])
 check(ts[0]['net2']<=ts[0]['net1']<ts[0]['g'],'stress_and_funding_reserve_nonoptimistic')
 # Reflection tests the signal logic, not asymmetric percent-based costs.
 import random
 rng=random.Random(20261006);bs=[];c=100.
 for i in range(96*8):
  o=c;c=o+rng.gauss(0,1);bs.append((START+900*i,o,max(o,c)+.1,min(o,c)-.1,c))
 reflected=[(t,300-o,300-l,300-h,300-c) for t,o,h,l,c in bs]
 for arm in ('CUSUM','DUAL_THRUST'):
  a=signals(bs,arm);b=signals(reflected,arm)
  check(bool(a) and set(a)==set(b) and all(a[t]['side']==-b[t]['side'] and abs(a[t]['stop']+b[t]['stop']-300)<1e-7 for t in a),arm+'_mirror')
  cut=96*6;prefix=signals(bs[:cut],arm);full=signals(bs,arm)
  check(prefix=={t:v for t,v in full.items() if t<=bs[cut-1][0]+900},arm+'_prefix_causality')
 return names

def main():
 parser=argparse.ArgumentParser();parser.add_argument('--test',action='store_true');parser.add_argument('--out',default='research_result.json');args=parser.parse_args()
 qa=tests();print('GENESIS_QA='+json.dumps({'passed':len(qa),'tests':qa}),flush=True)
 if args.test:return
 frozen=fingerprint(PROTOCOL);print('GENESIS_PREREG='+json.dumps({'protocol_sha256':frozen,'protocol':PROTOCOL}),flush=True)
 allrows={};funds={};manifest=[]
 keys=[(s,m,k) for s in SYMBOLS for m in MONTHS for k in ('klines','funding')]
 try:
  with ThreadPoolExecutor(max_workers=4) as pool:
   for key,rows,mf in pool.map(fetch_one,keys):
    s,m,k=key;manifest.append(mf)
    if k=='klines':
     rr=[(int(r[0])//1000,*map(float,r[1:5])) for r in rows]
     allrows.setdefault(s,[]).extend(rr)
    else:
     rr=[(int(r[0])//1000,float(r[2])) for r in rows];funds.setdefault(s,[]).extend(rr)
  for s in SYMBOLS:
   allrows[s].sort();funds[s].sort();validate(allrows[s])
   if any(t<START or t>=END for t,_ in funds[s]) or len(funds[s])<151*2: raise ValueError('Funding coverage incomplete')
  result={'protocol':PROTOCOL,'protocol_sha256':frozen,'tests_passed':len(qa),'manifest':manifest,'dataset_rows':{s:len(v) for s,v in allrows.items()},'arms':{}}
  for arm in ('CUSUM','DUAL_THRUST'):
   alltr=[];diagnostics={}
   for s in SYMBOLS:
    sig=signals(aggregate(allrows[s]),arm);tr,di=simulate(allrows[s],sig,funds[s]);alltr.extend({**t,'symbol':s} for t in tr);diagnostics[s]=di
   cells={'overall':metrics(alltr),'long':metrics([t for t in alltr if t['side']==1]),'short':metrics([t for t in alltr if t['side']==-1])}
   for s in SYMBOLS:
    cells[s]=metrics([t for t in alltr if t['symbol']==s])
    for side in (1,-1):cells[f'{s}:{side}']=metrics([t for t in alltr if t['symbol']==s and t['side']==side])
   failures=gates(cells)
   result['arms'][arm]={'status':'REJECTED_PILOT_GATES' if failures else 'PILOT_NUMERIC_PASS_NOT_VERIFIED','metrics':cells,'failures':failures,'diagnostics':diagnostics,'trades':alltr,'verified':False}
  Path(args.out).write_text(json.dumps(result,indent=2,allow_nan=False))
  summary={k:v for k,v in result.items() if k not in ('protocol','manifest','arms')}
  summary['source_archives']=len(manifest);summary['arms']={a:{k:v for k,v in r.items() if k!='trades'} for a,r in result['arms'].items()}
  print('GENESIS_RESULT='+json.dumps(summary,allow_nan=False),flush=True)
 except Exception as e:
  err={'status':'BLOCKED_INPUT_NOT_BACKTEST_REJECTION','error_type':type(e).__name__,'error':str(e),'verified':False,'performance_run':False,'protocol_sha256':frozen,'archives_retrieved':len(manifest),'tests_passed':len(qa)}
  Path(args.out).write_text(json.dumps(err,indent=2));print('GENESIS_RESULT='+json.dumps(err),flush=True);raise

if __name__=='__main__':main()
