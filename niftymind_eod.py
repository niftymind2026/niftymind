# =============================================================
# NIFTYMIND AI — DAILY EOD ENGINE
# Runs every weekday at 5:00 PM IST (11:30 AM UTC)
# Phases 1-7: Data + Engines + News + Market Context + Portfolio
# =============================================================

import os
import sys
import time
import json
import feedparser
import yfinance as yf
import pandas as pd
import numpy as np
import requests
from groq import Groq
from newsapi import NewsApiClient
from supabase import create_client
from datetime import datetime, date, timedelta

# =============================================================
# SECTION 1: CONFIGURATION
# =============================================================

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://kvbsxavluedkciodcujb.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
NEWSAPI_KEY  = os.environ.get("NEWSAPI_KEY", "")
GROQ_KEY     = os.environ.get("GROQ_KEY", "")

supabase    = create_client(SUPABASE_URL, SUPABASE_KEY)
newsapi     = NewsApiClient(api_key=NEWSAPI_KEY)
groq_client = Groq(api_key=GROQ_KEY)

TODAY      = str(date.today())
NOW        = datetime.today()
THIS_MONTH = NOW.replace(day=1).date()

print("=" * 60)
print("  NIFTYMIND AI — DAILY EOD ENGINE")
print(f"  Date: {TODAY}")
print("=" * 60)

# =============================================================
# SECTION 2: MARKET OPEN CHECK
# If NSE was closed today (holiday), exit cleanly
# No empty rows saved to database
# =============================================================

print("\n🔍 SECTION 2: MARKET OPEN CHECK")
print("-" * 40)

def is_market_open_today():
    try:
        nifty = yf.download("^NSEI", period="5d", interval="1d",
                            progress=False, auto_adjust=True)
        if nifty.empty:
            return False, 0, 0
        latest_date   = nifty.index[-1].date()
        latest_close  = float(nifty["Close"].iloc[-1].iloc[0])
        prev_close    = float(nifty["Close"].iloc[-2].iloc[0])
        week_ago      = float(nifty["Close"].iloc[0].iloc[0])
        daily_chg     = ((latest_close - prev_close) / prev_close) * 100
        weekly_chg    = ((latest_close - week_ago)   / week_ago)   * 100

        if latest_date < date.today():
            print(f"   Latest data: {latest_date} — NSE CLOSED today (holiday)")
            return False, latest_close, weekly_chg

        print(f"   ✅ NSE OPEN | Nifty:{latest_close:,.0f} Day:{daily_chg:+.2f}% Week:{weekly_chg:+.2f}%")
        return True, latest_close, weekly_chg

    except Exception as e:
        print(f"   ⚠️ Nifty check error: {e} — assuming market open")
        return True, 0, 0

market_open, nifty_close, nifty_weekly = is_market_open_today()

if not market_open:
    print(f"\n🏖️  Market closed today ({TODAY}) — skipping run. No data saved.")
    sys.exit(0)

# =============================================================
# SECTION 3: DAILY EOD DATA UPDATE
# =============================================================

print("\n📥 SECTION 3: DAILY EOD DATA UPDATE")
print("-" * 40)

stocks_response = supabase.table("stocks").select("symbol").eq("is_active", True).execute()
all_symbols     = [row["symbol"] for row in stocks_response.data]
print(f"Stocks to update: {len(all_symbols)}")

if len(all_symbols) == 0:
    print("   ❌ No active stocks — check Supabase RLS. Run:")
    print("   ALTER TABLE stocks DISABLE ROW LEVEL SECURITY;")
    sys.exit(1)

updated = 0
failed  = 0

for i, symbol in enumerate(all_symbols):
    try:
        ticker = yf.Ticker(symbol)
        hist   = ticker.history(period="5d", interval="1d", auto_adjust=True)
        if hist.empty:
            failed += 1
            continue

        latest_date = hist.index[-1].date()
        if latest_date < date.today() - timedelta(days=3):
            failed += 1
            continue

        latest = hist.iloc[-1]
        supabase.table("daily_candles").upsert([{
            "symbol": symbol, "date": str(latest_date),
            "open":   round(float(latest["Open"]),  2),
            "high":   round(float(latest["High"]),  2),
            "low":    round(float(latest["Low"]),   2),
            "close":  round(float(latest["Close"]), 2),
            "volume": int(latest["Volume"])
        }], on_conflict="symbol,date").execute()

        monthly_days = supabase.table("daily_candles") \
            .select("open,high,low,close,volume,date") \
            .eq("symbol", symbol).gte("date", str(THIS_MONTH)) \
            .order("date", desc=False).execute()

        if not monthly_days.data:
            failed += 1
            continue

        days         = monthly_days.data
        month_open   = float(days[0]["open"])
        month_high   = max(float(d["high"])  for d in days)
        month_low    = min(float(d["low"])   for d in days)
        month_close  = float(days[-1]["close"])
        month_volume = sum(int(d["volume"])  for d in days)

        existing = supabase.table("monthly_candles").select("id") \
            .eq("symbol", symbol).eq("date", str(THIS_MONTH)).execute()

        if existing.data:
            supabase.table("monthly_candles").update({
                "open": round(month_open,2), "high": round(month_high,2),
                "low":  round(month_low,2),  "close": round(month_close,2),
                "volume": month_volume, "is_complete": False
            }).eq("symbol", symbol).eq("date", str(THIS_MONTH)).execute()
        else:
            supabase.table("monthly_candles").insert({
                "symbol": symbol, "date": str(THIS_MONTH),
                "open": round(month_open,2), "high": round(month_high,2),
                "low":  round(month_low,2),  "close": round(month_close,2),
                "volume": month_volume, "is_complete": False
            }).execute()

        updated += 1
        if (i + 1) % 100 == 0:
            print(f"  Updated: {i+1}/{len(all_symbols)}")
        time.sleep(0.2)

    except Exception:
        failed += 1

print(f"✅ EOD Update: {updated} updated, {failed} failed")

if updated == 0:
    print("   ❌ Zero stocks updated — possible holiday or data issue. Exiting cleanly.")
    sys.exit(0)

# =============================================================
# SECTION 4: CORPORATE ACTION CHECKER
# =============================================================

print("\n🏢 SECTION 4: CORPORATE ACTION CHECK")
print("-" * 40)

flagged = []
for symbol in all_symbols:
    try:
        stored = supabase.table("monthly_candles").select("close,date") \
            .eq("symbol", symbol).eq("is_complete", True) \
            .order("date", desc=True).limit(1).execute()
        if not stored.data: continue

        stored_close = float(stored.data[0]["close"])
        hist = yf.Ticker(symbol).history(period="3mo", interval="1mo", auto_adjust=True)
        if hist.empty or len(hist) < 2: continue

        yahoo_close = float(hist["Close"].iloc[-2])
        diff_pct    = abs(stored_close - yahoo_close) / stored_close * 100

        if diff_pct > 10:
            flagged.append({"symbol": symbol, "diff_pct": round(diff_pct,1)})
            supabase.table("corporate_actions").insert({
                "symbol": symbol, "detected_date": TODAY,
                "action_type": "AUTO_DETECTED", "stored_price": stored_close,
                "yahoo_price": yahoo_close, "difference_pct": round(diff_pct,1),
                "status": "PENDING", "notes": f"Price diff {diff_pct:.1f}%"
            }).execute()
            supabase.table("stocks").update({"status":"PENDING_REVIEW","is_active":False}) \
                .eq("symbol", symbol).execute()
        time.sleep(0.3)
    except Exception:
        pass

print(f"✅ Corporate check: {len(flagged)} flagged")

# =============================================================
# SECTION 5: ENGINE 1 — EMA CALCULATOR
# =============================================================

print("\n📊 SECTION 5: ENGINE 1 — EMA CALCULATOR")
print("-" * 40)

def calculate_ema(prices, period):
    return pd.Series(prices).ewm(span=period, adjust=False).mean()

stocks   = supabase.table("stocks").select("symbol,name,sector") \
    .eq("data_quality","SUFFICIENT").eq("is_active",True).execute().data
qualified = []

for i, stock in enumerate(stocks):
    try:
        symbol  = stock["symbol"]
        candles = supabase.table("monthly_candles").select("date,open,high,low,close,volume") \
            .eq("symbol",symbol).order("date",desc=False).execute()
        if not candles.data or len(candles.data) < 55: continue

        closes       = [float(c["close"]) for c in candles.data]
        ema19_series = calculate_ema(closes, 19)
        ema55_series = calculate_ema(closes, 55)
        current_close = closes[-1]
        current_ema19 = round(ema19_series.iloc[-1], 2)
        current_ema55 = round(ema55_series.iloc[-1], 2)
        prev_ema19    = round(ema19_series.iloc[-2], 2)

        is_qualified = (current_ema19 > current_ema55 and
                        current_close > current_ema19 and
                        current_ema19 > prev_ema19)
        if is_qualified: qualified.append(symbol)

        supabase.table("signals").upsert({
            "symbol": symbol, "detected_date": TODAY, "pattern_type": "EMA_STRUCTURE",
            "direction": "BULLISH" if is_qualified else "BEARISH", "price": current_close,
            "technical_score": 25 if is_qualified else 0,
            "grade": "QUALIFIED" if is_qualified else "NOT_QUALIFIED"
        }).execute()

        if (i+1) % 50 == 0: print(f"  Processed: {i+1}/{len(stocks)}")
    except Exception:
        pass

qualified_symbols = qualified[:]
print(f"✅ EMA Engine: {len(qualified_symbols)} qualified out of {len(stocks)}")

# =============================================================
# SECTION 6: ENGINE 2 — PATTERN DETECTION
# =============================================================

print("\n📈 SECTION 6: ENGINE 2 — PATTERN DETECTION")
print("-" * 40)

def detect_swing_points(highs, lows, closes, lookback=3):
    sh, sl = [], []
    n = len(highs)
    for i in range(lookback, n-lookback):
        if all(highs[i]>highs[i-j] for j in range(1,lookback+1)) and \
           all(highs[i]>highs[i+j] for j in range(1,lookback+1)): sh.append({"index":i,"price":highs[i]})
        if all(lows[i]<lows[i-j]   for j in range(1,lookback+1)) and \
           all(lows[i]<lows[i+j]   for j in range(1,lookback+1)): sl.append({"index":i,"price":lows[i]})
    return sh, sl

def detect_trend(pts, min_points=2):
    if len(pts) < min_points: return "INSUFFICIENT"
    prices = [p["price"] for p in pts[-min_points:]]
    if all(prices[i]>prices[i-1] for i in range(1,len(prices))): return "UPTREND"
    if all(prices[i]<prices[i-1] for i in range(1,len(prices))): return "DOWNTREND"
    return "SIDEWAYS"

def detect_channel(sh, sl, closes):
    if len(sh)<2 or len(sl)<2: return "NO_CHANNEL"
    ht, lt = detect_trend(sh), detect_trend(sl)
    if ht=="UPTREND"   and lt=="UPTREND":   return "ASCENDING_CHANNEL"
    if ht=="DOWNTREND" and lt=="DOWNTREND": return "DESCENDING_CHANNEL"
    if ht=="SIDEWAYS"  and lt=="SIDEWAYS":  return "HORIZONTAL_CHANNEL"
    return "NO_CHANNEL"

pattern_results = []
for i, symbol in enumerate(qualified_symbols):
    try:
        candles = supabase.table("monthly_candles").select("date,open,high,low,close,volume") \
            .eq("symbol",symbol).order("date",desc=False).execute()
        if not candles.data or len(candles.data)<20: continue

        closes = [float(c["close"]) for c in candles.data]
        highs  = [float(c["high"])  for c in candles.data]
        lows   = [float(c["low"])   for c in candles.data]
        ema19  = list(calculate_ema(closes,19))
        ema55  = list(calculate_ema(closes,55))

        sh, sl    = detect_swing_points(highs, lows, closes)
        channel   = detect_channel(sh, sl, closes)
        low_trend = detect_trend(sl)

        current, prev = closes[-1], closes[-2]
        breakouts = []
        if sh and current>sh[-1]["price"] and prev<=sh[-1]["price"]: breakouts.append("BREAKOUT_ABOVE_HIGH")
        if sl and current<sl[-1]["price"] and prev>=sl[-1]["price"]: breakouts.append("BREAKDOWN_BELOW_LOW")
        if current>ema19[-1] and prev<ema19[-2]: breakouts.append("RECLAIMED_EMA19")

        ps = 0
        if low_trend=="UPTREND":                ps+=8
        if detect_trend(sh)=="UPTREND":         ps+=5
        if channel=="ASCENDING_CHANNEL":         ps+=7
        elif channel=="HORIZONTAL_CHANNEL":      ps+=3
        if "BREAKOUT_ABOVE_HIGH" in breakouts:   ps+=10
        if "RECLAIMED_EMA19"     in breakouts:   ps+=5
        if "BREAKDOWN_BELOW_LOW" in breakouts:   ps-=10
        ps = min(25, max(0, ps))

        supabase.table("signals").upsert({
            "symbol": symbol, "detected_date": TODAY,
            "pattern_type": channel if channel!="NO_CHANNEL" else "TREND_"+low_trend,
            "direction": "BULLISH" if ps>10 else "NEUTRAL",
            "price": closes[-1], "trendline_touches": len(sl),
            "technical_score": ps, "grade": "QUALIFIED"
        }).execute()
        pattern_results.append({"symbol": symbol, "pattern_score": ps})
        if (i+1)%25==0: print(f"  Processed: {i+1}/{len(qualified_symbols)}")
    except Exception:
        pass

print(f"✅ Pattern Engine: {len(pattern_results)} analyzed")

# =============================================================
# SECTION 7: ENGINE 3 — VPA ANALYSIS
# =============================================================

print("\n📊 SECTION 7: ENGINE 3 — VPA ANALYSIS")
print("-" * 40)

def analyze_vpa(closes, highs, lows, opens, volumes):
    signals, vpa_score = [], 0
    n = len(closes)
    if n < 12: return signals, vpa_score, 1.0
    avg_vol_10 = pd.Series(volumes).rolling(10).mean()
    curr_close=closes[-1]; curr_open=opens[-1]; curr_high=highs[-1]; curr_low=lows[-1]
    curr_vol=volumes[-1]; curr_avg=avg_vol_10.iloc[-1]
    vol_ratio   = curr_vol/curr_avg if curr_avg>0 else 1
    body        = abs(curr_close-curr_open); total_range=curr_high-curr_low
    if total_range==0: return signals, vpa_score, round(vol_ratio,2)
    body_pct=body/total_range; wick_ratio=1-body_pct; is_bullish=curr_close>curr_open

    if vol_ratio>2.0 and wick_ratio>0.6:
        pp=(curr_close-min(closes[-12:]))/(max(closes[-12:])-min(closes[-12:])+0.001)
        if pp<0.3: signals.append("BUYING_CLIMAX");  vpa_score+=6
        else:      signals.append("SELLING_CLIMAX"); vpa_score-=4
    elif is_bullish  and vol_ratio<0.7: signals.append("NO_DEMAND");           vpa_score-=2
    elif not is_bullish and vol_ratio<0.7: signals.append("NO_SUPPLY");        vpa_score+=4
    elif is_bullish  and vol_ratio>1.5: signals.append("VOLUME_CONFIRMED_UP"); vpa_score+=5
    elif not is_bullish and vol_ratio>1.5: signals.append("HIGH_VOL_SELLING"); vpa_score-=3

    if n>=4:
        lc=closes[-4:-1]; lh=highs[-4:-1]; ll=lows[-4:-1]; lv=volumes[-4:-1]
        lav=avg_vol_10.iloc[-4:-1].mean()
        if (max(lh)-min(ll))/closes[-4]*100<8 and np.mean(lv)>lav and lc[-1]>=lc[0]:
            signals.append("ACCUMULATION"); vpa_score+=6
    if n>=6:
        lc6=closes[-6:]; lv6=volumes[-6:]
        uv=[lv6[i] for i in range(1,6) if lc6[i]>lc6[i-1]]
        dv=[lv6[i] for i in range(1,6) if lc6[i]<lc6[i-1]]
        if uv and dv:
            if np.mean(uv)>np.mean(dv)*1.2:   signals.append("HEALTHY_UPTREND");   vpa_score+=4
            elif np.mean(dv)>np.mean(uv)*1.2: signals.append("WEAKENING_UPTREND"); vpa_score-=2

    return signals, min(15,max(0,vpa_score)), round(vol_ratio,2)

vpa_results = []
for i, symbol in enumerate(qualified_symbols):
    try:
        candles = supabase.table("monthly_candles").select("date,open,high,low,close,volume") \
            .eq("symbol",symbol).order("date",desc=False).execute()
        if not candles.data or len(candles.data)<12: continue
        closes=[float(c["close"]) for c in candles.data]; opens=[float(c["open"]) for c in candles.data]
        highs=[float(c["high"]) for c in candles.data];   lows=[float(c["low"]) for c in candles.data]
        volumes=[float(c["volume"]) for c in candles.data]
        _, vpa_score, _ = analyze_vpa(closes,highs,lows,opens,volumes)
        supabase.table("signals").upsert({
            "symbol":symbol,"detected_date":TODAY,"pattern_type":"VPA",
            "direction":"BULLISH" if vpa_score>=8 else "NEUTRAL",
            "price":closes[-1],"vpa_score":vpa_score,"grade":"QUALIFIED"
        }).execute()
        vpa_results.append({"symbol":symbol,"vpa_score":vpa_score})
        if (i+1)%25==0: print(f"  Processed: {i+1}/{len(qualified_symbols)}")
    except Exception:
        pass

print(f"✅ VPA Engine: {len(vpa_results)} analyzed")

# =============================================================
# SECTION 8: ENGINE 4 — CANDLESTICK SCANNER
# =============================================================

print("\n🕯️  SECTION 8: ENGINE 4 — CANDLESTICK SCANNER")
print("-" * 40)

def detect_single_candle(o,h,l,c):
    body=abs(c-o); r=h-l
    if r<0.001: return "NONE"
    bp=body/r; uw=h-max(o,c); lw=min(o,c)-l; bull=c>o
    if bp<0.1:
        if lw>uw*2: return "DRAGONFLY_DOJI"
        if uw>lw*2: return "GRAVESTONE_DOJI"
        return "DOJI"
    if lw>body*2 and uw<body*0.3: return "HAMMER" if bull else "HANGING_MAN"
    if uw>body*2 and lw<body*0.3: return "SHOOTING_STAR" if not bull else "INVERTED_HAMMER"
    if bp>0.85: return "BULL_MARUBOZU" if bull else "BEAR_MARUBOZU"
    return "NORMAL"

def detect_two_candle(p,c):
    po,ph,pl,pc=p; co,ch,cl,cc=c; pb=pc>po; cb=cc>co
    if not pb and cb and co<=pc and cc>=po: return "BULLISH_ENGULFING"
    if pb and not cb and co>=pc and cc<=po: return "BEARISH_ENGULFING"
    if not pb and cb and co<pl  and cc>(po+pc)/2: return "PIERCING_LINE"
    if pb and not cb and co>ph  and cc<(po+pc)/2: return "DARK_CLOUD_COVER"
    return "NONE"

def detect_three_candle(c1,c2,c3):
    o1,h1,l1,cl1=c1; o2,h2,l2,cl2=c2; o3,h3,l3,cl3=c3
    bp2=abs(cl2-o2)/(h2-l2) if (h2-l2)>0 else 0
    if cl1<o1 and bp2<0.3 and cl3>o3 and cl3>(o1+cl1)/2: return "MORNING_STAR"
    if cl1>o1 and bp2<0.3 and cl3<o3 and cl3<(o1+cl1)/2: return "EVENING_STAR"
    if cl1>o1 and cl2>o2 and cl3>o3 and cl2>cl1 and cl3>cl2: return "THREE_WHITE_SOLDIERS"
    if cl1<o1 and cl2<o2 and cl3<o3 and cl2<cl1 and cl3<cl2: return "THREE_BLACK_CROWS"
    return "NONE"

PS = {"HAMMER":3,"DRAGONFLY_DOJI":2,"BULL_MARUBOZU":2,"DOJI":1,
      "SHOOTING_STAR":-3,"GRAVESTONE_DOJI":-2,"HANGING_MAN":-2,"BEAR_MARUBOZU":-2,
      "BULLISH_ENGULFING":4,"PIERCING_LINE":3,"BEARISH_ENGULFING":-4,"DARK_CLOUD_COVER":-3,
      "MORNING_STAR":5,"THREE_WHITE_SOLDIERS":4,"EVENING_STAR":-5,"THREE_BLACK_CROWS":-4}

def backward_candle_scan(data, lookback=6):
    if len(data)<lookback+3: return 0,"NONE"
    w=data[-lookback:]; ts=0
    for c in w:
        p=detect_single_candle(c["open"],c["high"],c["low"],c["close"])
        if p not in ["NONE","NORMAL"]: ts+=PS.get(p,0)
    for i in range(1,len(w)):
        p=detect_two_candle((w[i-1]["open"],w[i-1]["high"],w[i-1]["low"],w[i-1]["close"]),
                            (w[i]["open"],  w[i]["high"],  w[i]["low"],  w[i]["close"]))
        if p!="NONE": ts+=PS.get(p,0)
    for i in range(2,len(w)):
        p=detect_three_candle((w[i-2]["open"],w[i-2]["high"],w[i-2]["low"],w[i-2]["close"]),
                              (w[i-1]["open"],w[i-1]["high"],w[i-1]["low"],w[i-1]["close"]),
                              (w[i]["open"],  w[i]["high"],  w[i]["low"],  w[i]["close"]))
        if p!="NONE": ts+=PS.get(p,0)
    sc=min(15,max(0,ts))
    cv="HIGH" if sc>=10 else "MEDIUM" if sc>=6 else "LOW" if sc>=3 else "NONE"
    return sc,cv

candle_results=[]
for i,symbol in enumerate(qualified_symbols):
    try:
        candles=supabase.table("monthly_candles").select("date,open,high,low,close") \
            .eq("symbol",symbol).order("date",desc=False).execute()
        if not candles.data or len(candles.data)<9: continue
        score,conviction=backward_candle_scan(candles.data)
        supabase.table("signals").upsert({
            "symbol":symbol,"detected_date":TODAY,"pattern_type":"CANDLE_SCAN",
            "direction":"BULLISH" if score>=6 else "NEUTRAL",
            "price":candles.data[-1]["close"],"candle_score":score,"grade":conviction
        }).execute()
        candle_results.append({"symbol":symbol,"score":score})
        if (i+1)%25==0: print(f"  Processed: {i+1}/{len(qualified_symbols)}")
    except Exception:
        pass

print(f"✅ Candle Engine: {len(candle_results)} scanned")

# =============================================================
# SECTION 9: ENGINE 5 — ATTRAOS (CHAOS THEORY)
# =============================================================

print("\n🌀 SECTION 9: ENGINE 5 — ATTRAOS")
print("-" * 40)

def reconstruct_phase_space(prices,d=3,tau=1):
    prices=np.array(prices); n=len(prices); m=n-(d-1)*tau
    if m<=0: return None
    ps=np.zeros((m,d))
    for i in range(m):
        for j in range(d): ps[i,j]=prices[i+j*tau]
    return ps

def detect_attractor_zone(ps,cp):
    if ps is None or len(ps)<10: return "INSUFFICIENT_DATA",0.5
    c=np.mean(ps,axis=0); d=[np.linalg.norm(p-c) for p in ps]
    avg=np.mean(d); std=np.std(d)
    rd=[np.linalg.norm(p-c) for p in ps[-3:]]
    dt=rd[-1]-rd[0] if len(rd)>=2 else 0
    if std/(avg+0.001)>0.5: return "CHAOS",0.3
    if dt>std*0.3:  return "EXPANSION",min(0.85,0.6+dt/(avg+0.001))
    if dt<-std*0.3: return "CONTRACTION",0.4
    return "TRANSITION",0.5

def calc_dir_prob(ps,prices,d=3):
    if ps is None or len(ps)<15: return 0.5
    prices=np.array(prices); n=len(ps); cur=ps[-1]
    dists=sorted([(np.linalg.norm(ps[i]-cur),i) for i in range(n-1)])
    nb=[idx for _,idx in dists if idx<n-3][:5]
    if not nb: return 0.5
    up=sum(1 for i in nb if i+d<len(prices) and prices[i+d]>prices[i+d-1])
    return round(up/len(nb),2)

def calc_chaos(prices):
    prices=np.array(prices); n=len(prices)
    if n<20: return 0.5
    ret=np.diff(prices)/prices[:-1]; signs=np.sign(ret)
    cons=np.mean([1 if signs[i]==signs[i-1] else 0 for i in range(1,len(signs))])
    rv=pd.Series(ret).rolling(6).std()
    vov=rv.std()/(rv.mean()+0.001)
    return round((1-cons)*0.5+min(1,vov)*0.5,2)

def attraos_score(zone,dp,chaos):
    s={"EXPANSION":10,"TRANSITION":5,"CONTRACTION":2,"CHAOS":0,"INSUFFICIENT_DATA":0}.get(zone,0)
    s+=10 if dp>=0.75 else 7 if dp>=0.60 else 4 if dp>=0.50 else 0
    s+=5  if chaos<0.3 else 3 if chaos<0.5 else 1 if chaos<0.7 else 0
    return min(25,s)

attraos_results=[]
for i,symbol in enumerate(qualified_symbols):
    try:
        candles=supabase.table("monthly_candles").select("date,close") \
            .eq("symbol",symbol).order("date",desc=False).execute()
        if not candles.data or len(candles.data)<24: continue
        prices=[float(c["close"]) for c in candles.data]
        ps=reconstruct_phase_space(prices)
        if ps is None or len(ps)<10: continue
        zone,_=detect_attractor_zone(ps,ps[-1])
        dp=calc_dir_prob(ps,prices)
        chaos=calc_chaos(prices)
        a_score=attraos_score(zone,dp,chaos)
        supabase.table("signals").upsert({
            "symbol":symbol,"detected_date":TODAY,"pattern_type":"ATTRAOS",
            "direction":"BULLISH" if a_score>=15 else "NEUTRAL",
            "price":prices[-1],"attraos_score":a_score,"grade":zone
        }).execute()
        attraos_results.append({"symbol":symbol,"attraos_score":a_score,"zone":zone})
        if (i+1)%25==0: print(f"  Processed: {i+1}/{len(qualified_symbols)}")
    except Exception:
        pass

print(f"✅ Attraos Engine: {len(attraos_results)} analyzed")

# =============================================================
# SECTION 10: MASTER SCORING ENGINE
# =============================================================

print("\n🏆 SECTION 10: MASTER SCORING ENGINE")
print("-" * 40)

master_results=[]
for symbol in qualified_symbols:
    try:
        si=supabase.table("stocks").select("name,sector").eq("symbol",symbol).execute()
        if not si.data: continue
        name=si.data[0]["name"]; sector=si.data[0]["sector"]

        def gs(pt,field):
            r=supabase.table("signals").select(field).eq("symbol",symbol) \
                .eq("pattern_type",pt).eq("detected_date",TODAY).execute()
            return r.data[0].get(field,0) or 0 if r.data else 0

        er=supabase.table("signals").select("technical_score,price") \
            .eq("symbol",symbol).eq("pattern_type","EMA_STRUCTURE").eq("detected_date",TODAY).execute()
        ema_score=er.data[0].get("technical_score",0) or 0 if er.data else 0
        cp=er.data[0].get("price",0) or 0 if er.data else 0

        pr=supabase.table("signals").select("technical_score").eq("symbol",symbol) \
            .eq("detected_date",TODAY).neq("pattern_type","EMA_STRUCTURE").neq("pattern_type","VPA") \
            .neq("pattern_type","CANDLE_SCAN").neq("pattern_type","ATTRAOS") \
            .neq("pattern_type","MASTER_SCORE").execute()
        pattern_score=pr.data[0].get("technical_score",0) or 0 if pr.data else 0

        vpa_score=gs("VPA","vpa_score"); candle_score=gs("CANDLE_SCAN","candle_score")
        a_score=gs("ATTRAOS","attraos_score")
        total=ema_score+pattern_score+vpa_score+candle_score+a_score
        grade="A+" if total>=70 else "A" if total>=55 else "B" if total>=40 else "C" if total>=25 else "SKIP"

        master_results.append({"symbol":symbol,"name":name,"sector":sector,
                                "price":cp,"total_score":total,"grade":grade})

        ex=supabase.table("signals").select("id").eq("symbol",symbol) \
            .eq("detected_date",TODAY).eq("pattern_type","MASTER_SCORE").execute()
        rd={"technical_score":ema_score,"vpa_score":vpa_score,"candle_score":candle_score,
            "attraos_score":a_score,"total_score":total,"grade":grade,"price":cp,
            "direction":"BULLISH" if total>=55 else "NEUTRAL"}
        if ex.data:
            supabase.table("signals").update(rd).eq("symbol",symbol) \
                .eq("detected_date",TODAY).eq("pattern_type","MASTER_SCORE").execute()
        else:
            supabase.table("signals").insert({**rd,"symbol":symbol,
                "detected_date":TODAY,"pattern_type":"MASTER_SCORE"}).execute()
    except Exception:
        pass

master_results.sort(key=lambda x: x["total_score"],reverse=True)
aplus=sum(1 for r in master_results if r["grade"]=="A+")
a=sum(1 for r in master_results if r["grade"]=="A")
print(f"✅ Master Scoring: {len(master_results)} scored | {aplus} A+ | {a} A grade")
if master_results:
    print(f"\n   TOP 5:")
    for idx,r in enumerate(master_results[:5],1):
        print(f"   {idx}. {r['symbol']:<18} {r['total_score']}/80  {r['grade']}")

# =============================================================
# SECTION 11: NEWS INTELLIGENCE ENGINE
# =============================================================

print("\n📰 SECTION 11: NEWS INTELLIGENCE ENGINE")
print("-" * 40)

ordered_stocks=[r for r in master_results if r["grade"] in ["A+","A"]]
print(f"Stocks for news: {len(ordered_stocks)}")

def clean_symbol(s): return s.replace(".NS","").replace(".BO","")

def fetch_newsapi(symbol):
    try:
        result=newsapi.get_everything(q=f'"{clean_symbol(symbol)}" stock India',
            language="en",sort_by="publishedAt",page_size=5,
            from_param=(date.today()-timedelta(days=7)).isoformat())
        if result.get("code")=="rateLimited": return []
        return [{"headline":a["title"],"source":a["source"]["name"],"published":a["publishedAt"][:10]}
                for a in result.get("articles",[]) if a.get("title")]
    except Exception: return []

def fetch_rss(symbol):
    try:
        q=clean_symbol(symbol).replace(" ","+")
        feed=feedparser.parse(f"https://news.google.com/rss/search?q={q}+stock+India&hl=en-IN&gl=IN&ceid=IN:en")
        return [{"headline":e.title,"source":"Google News","published":TODAY} for e in feed.entries[:5]]
    except Exception: return []

def fetch_news(symbol):
    a=fetch_newsapi(symbol)
    if a: return a,"NewsAPI"
    return fetch_rss(symbol),"RSS"

SENTIMENT_PROMPT="""You are a financial analyst AI for Indian equity markets.
Analyze these news headlines for stock: {symbol}
Headlines:
{headlines}
Return ONLY a JSON object:
{{"sentiment":<float 0.0-10.0>,"sentiment_label":"<BULLISH|BEARISH|NEUTRAL>",
"risk_events":[{{"event":"<desc>","risk_date":"<YYYY-MM-DD or null>","impact":"<HIGH|MEDIUM|LOW>"}}],
"summary":"<one sentence>"}}
Scoring: 8-10=bullish, 4-6=neutral, 0-2=bearish. Return JSON only."""

def score_sentiment(symbol,articles):
    ht="\n".join(f"{i+1}.[{a['source']}] {a['headline']}" for i,a in enumerate(articles)) if articles else "No news."
    try:
        r=groq_client.chat.completions.create(model="llama-3.1-8b-instant",
            messages=[{"role":"system","content":"Return only valid JSON."},
                      {"role":"user","content":SENTIMENT_PROMPT.format(symbol=clean_symbol(symbol),headlines=ht)}],
            temperature=0.1,max_tokens=500)
        raw=r.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw=raw.split("```")[1]
            if raw.startswith("json"): raw=raw[4:]
        return json.loads(raw.strip())
    except Exception:
        return {"sentiment":5.0,"sentiment_label":"NEUTRAL","risk_events":[],"summary":"Could not score."}

if ordered_stocks:
    supabase.table("news_items").delete().eq("published_date",TODAY).execute()
    time.sleep(1)
    news_errors=0
    for i,stock in enumerate(ordered_stocks):
        symbol=stock["symbol"]
        articles,source=fetch_news(symbol)
        scored=score_sentiment(symbol,articles)
        sv=float(scored.get("sentiment",5.0))
        sl_label=str(scored.get("sentiment_label","NEUTRAL"))
        re=scored.get("risk_events",[])
        summary=str(scored.get("summary","")) or None
        ret=str(re[0]["event"]) if re else None
        rdv=re[0].get("risk_date") if re else None
        if rdv in ("null","",None): rdv=None
        for article in articles:
            try:
                supabase.table("news_items").insert({
                    "symbol":str(symbol),"headline":str(article["headline"])[:500],
                    "source":str(article["source"]),"published_date":str(article["published"]),
                    "sentiment":sl_label,"sentiment_score":int(round(sv)),
                    "risk_event":ret,"risk_date":rdv,"summary":summary
                }).execute()
            except Exception: news_errors+=1
        try:
            supabase.table("signals").update({"news_score":int(round(sv))}) \
                .eq("symbol",symbol).eq("detected_date",TODAY).execute()
        except Exception: pass
        if (i+1)%20==0: print(f"  News: {i+1}/{len(ordered_stocks)} processed")
        time.sleep(1.5)
    print(f"✅ News Engine: {len(ordered_stocks)} scored, {news_errors} errors")
else:
    print("   ℹ️  No A+/A stocks — skipping news engine")

# =============================================================
# SECTION 12: MARKET CONTEXT ENGINE
# =============================================================

print("\n🌍 SECTION 12: MARKET CONTEXT ENGINE")
print("-" * 40)

def fetch_nifty_data():
    try:
        nifty=yf.download("^NSEI",period="5d",interval="1d",progress=False,auto_adjust=True)
        if nifty.empty: raise Exception("Empty")
        lc=float(nifty["Close"].iloc[-1].iloc[0]); pc=float(nifty["Close"].iloc[-2].iloc[0])
        wa=float(nifty["Close"].iloc[0].iloc[0])
        dc=((lc-pc)/pc)*100; wc=((lc-wa)/wa)*100
        trend="BULLISH" if wc>1.5 else "BEARISH" if wc<-1.5 else "SIDEWAYS"
        print(f"   Nifty 50 : {lc:,.0f} | Day:{dc:+.2f}% | Week:{wc:+.2f}% | {trend}")
        return {"nifty_close":lc,"nifty_change":dc,"nifty_trend":trend,"weekly_change":wc}
    except Exception as e:
        print(f"   ⚠️ Nifty error: {e}")
        return {"nifty_close":0,"nifty_change":0,"nifty_trend":"SIDEWAYS","weekly_change":0}

def fetch_vix_data():
    try:
        vix=yf.download("^INDIAVIX",period="3d",interval="1d",progress=False,auto_adjust=True)
        if vix.empty: raise Exception("Empty")
        vv=float(vix["Close"].iloc[-1].iloc[0])
        vl="VERY_LOW" if vv<13 else "LOW" if vv<16 else "MODERATE" if vv<20 else "HIGH" if vv<25 else "VERY_HIGH"
        print(f"   India VIX: {vv:.2f} | {vl}")
        return {"vix_value":vv,"vix_level":vl}
    except Exception as e:
        print(f"   ⚠️ VIX error: {e}")
        return {"vix_value":20,"vix_level":"MODERATE"}

def fetch_fii_dii():
    try:
        headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                 "Accept":"application/json, text/plain, */*",
                 "Referer":"https://www.nseindia.com/market-data/fii-dii-activity"}
        s=requests.Session()
        s.get("https://www.nseindia.com",headers=headers,timeout=10); time.sleep(2)
        s.get("https://www.nseindia.com/market-data/fii-dii-activity",headers=headers,timeout=10); time.sleep(1)
        data=s.get("https://www.nseindia.com/api/fiidiiTradeReact",headers=headers,timeout=10).json()
        fr=next((r for r in data if "FII" in r.get("category","").upper()),None)
        dr=next((r for r in data if "DII" in r.get("category","").upper()),None)
        fn=float(fr["netValue"]) if fr else 0.0; dn=float(dr["netValue"]) if dr else 0.0
        cb=fn+dn
        fs="STRONG_BUYING" if fn>500 else "BUYING" if fn>0 else "SELLING" if fn>-500 else "STRONG_SELLING"
        print(f"   FII:{fn:,.0f}Cr {fs} | DII:{dn:,.0f}Cr | Combined:{cb:,.0f}Cr")
        return {"fii_net":fn,"dii_net":dn,"combined_flow":cb,"fii_sentiment":fs}
    except Exception as e:
        print(f"   ⚠️ FII/DII error: {e}")
        return {"fii_net":0,"dii_net":0,"combined_flow":0,"fii_sentiment":"NEUTRAL"}

def calc_market_score(nifty,vix,fd):
    wc=nifty.get("weekly_change",0); vv=vix.get("vix_value",20); cb=fd.get("combined_flow",0)
    ns=10 if wc>3 else 8 if wc>1.5 else 6 if wc>0 else 4 if wc>-1.5 else 2 if wc>-3 else 0
    vs=10 if vv<13 else 8 if vv<16 else 6 if vv<20 else 3 if vv<25 else 0
    fs=10 if cb>2000 else 8 if cb>500 else 6 if cb>0 else 4 if cb>-500 else 2 if cb>-2000 else 0
    final=int(round((ns+vs+fs)/3))
    v="STRONG_BULL" if final>=8 else "BULL" if final>=6 else "NEUTRAL" if final>=4 else "BEAR" if final>=2 else "STRONG_BEAR"
    print(f"   Nifty:{ns}/10 VIX:{vs}/10 Flow:{fs}/10 → {final}/10 {v}")
    return {"final_score":final,"verdict":v,"nifty_score":ns,"vix_score":vs,"flow_score":fs}

nifty_data=fetch_nifty_data(); vix_data=fetch_vix_data()
fii_dii_data=fetch_fii_dii(); market=calc_market_score(nifty_data,vix_data,fii_dii_data)

# Save daily_summary only if real data exists
if len(master_results) > 0:
    try:
        supabase.table("daily_summary").upsert({
            "summary_date":TODAY,"portfolio_size":0,"cash_pct":100,
            "market_context":market["verdict"],
            "agent_thoughts":(f"Nifty:{nifty_data['nifty_trend']}({nifty_data['nifty_change']:+.2f}%)|"
                              f"VIX:{vix_data['vix_value']:.1f}({vix_data['vix_level']})|"
                              f"FII:₹{fii_dii_data['fii_net']:,.0f}Cr|"
                              f"Score:{market['final_score']}/10({market['verdict']})")
        },on_conflict="summary_date").execute()
        print(f"✅ daily_summary saved!")
    except Exception as e:
        print(f"   ⚠️ daily_summary error: {e}")

if ordered_stocks:
    ok=0
    for stock in ordered_stocks:
        try:
            supabase.table("signals").update({"market_context_score":market["final_score"]}) \
                .eq("symbol",stock["symbol"]).eq("detected_date",TODAY).execute()
            ok+=1
        except Exception: pass
    print(f"✅ Market context: {ok}/{len(ordered_stocks)} stocks updated")

# =============================================================
# SECTION 13: PORTFOLIO MANAGER (PHASE 7)
# =============================================================

print("\n💼 SECTION 13: PORTFOLIO MANAGER")
print("-" * 40)

MAX_POSITIONS=25; CAPITAL=1000000; POSITION_SIZE_PCT=0.04
STOP_LOSS_PCT=0.07; TARGET_PCT=0.20; MIN_SCORE_BUY=55
MIN_SCORE_HOLD=40;  MARKET_SCORE_MIN=4

port=supabase.table("portfolio").select("*").eq("status","ACTIVE").execute().data
held=[p["symbol"] for p in port]
print(f"Current holdings : {len(port)}/{MAX_POSITIONS}")

sm={r["symbol"]:r for r in master_results}
vs_map={"STRONG_BULL":9,"BULL":7,"NEUTRAL":5,"BEAR":3,"STRONG_BEAR":1}
msp=vs_map.get(market["verdict"],5)
print(f"Market today     : {market['verdict']} ({msp}/10)")

# ── EXITS ──────────────────────────────────────────
print("\n🔴 CHECKING EXITS...")
exits=[]
for pos in port:
    sym=pos["symbol"]; ep=float(pos["entry_price"]); sl=float(pos["stop_loss"])
    tgt=float(pos["target"]); qty=int(pos["quantity"])
    try:
        h=yf.Ticker(sym).history(period="2d",interval="1d",auto_adjust=True)
        cp=round(float(h["Close"].iloc[-1]),2) if not h.empty else ep
    except Exception: cp=ep
    pct=((cp-ep)/ep)*100
    sd=sm.get(sym,{}); cs=sd.get("total_score",0) or 0; cg=sd.get("grade","SKIP")
    er=None
    if cp<=sl: er=f"STOP_LOSS_HIT({cp}<={sl})"
    elif cp>=tgt: er=f"TARGET_HIT({cp}>={tgt})"
    elif cs<MIN_SCORE_HOLD and cg in ["C","SKIP"]: er=f"GRADE_DEGRADED(score:{cs})"
    elif market["verdict"]=="STRONG_BEAR" and pct<0: er=f"MARKET_PROTECTION({pct:.1f}%)"
    if er:
        rp=round((cp-ep)*qty,2)
        try:
            supabase.table("portfolio").update({
                "status":"EXITED","exit_date":TODAY,"exit_price":cp,
                "exit_reason":er,"realised_pnl":rp,
                "current_price":cp,"current_value":round(cp*qty,2),
                "unrealised_pnl":0,"unrealised_pnl_pct":0,
                "updated_at":datetime.now().isoformat()
            }).eq("symbol",sym).eq("status","ACTIVE").execute()
            exits.append({"symbol":sym,"pnl_pct":round(pct,2),"pnl_rs":rp,"reason":er})
            print(f"   EXIT: {sym:<18} {pct:+.1f}%  {er[:45]}")
        except Exception as e: print(f"   ⚠️ {sym}: {e}")
    else:
        try:
            supabase.table("portfolio").update({
                "current_price":cp,"current_value":round(cp*qty,2),
                "unrealised_pnl":round((cp-ep)*qty,2),"unrealised_pnl_pct":round(pct,2),
                "updated_at":datetime.now().isoformat()
            }).eq("symbol",sym).eq("status","ACTIVE").execute()
        except Exception: pass

print(f"   Exits today: {len(exits)}")

# ── ENTRIES ──────────────────────────────────────────
held=[p["symbol"] for p in port if p["symbol"] not in [e["symbol"] for e in exits]]
slots=MAX_POSITIONS-len(held)
print(f"\n🟢 CHECKING ENTRIES ({slots} slots available)...")
entries=[]

if market["verdict"]=="STRONG_BEAR":
    print("   ⚠️  STRONG_BEAR — no new entries"); slots=0
elif msp<MARKET_SCORE_MIN:
    print(f"   ⚠️  Market score {msp} < {MARKET_SCORE_MIN} — skipping"); slots=0

if slots>0:
    cands=[r for r in master_results if r["symbol"] not in held
           and r["grade"] in ["A+","A"] and (r["total_score"] or 0)>=MIN_SCORE_BUY]
    print(f"   Candidates: {len(cands)}")
    for stock in cands[:slots]:
        sym=stock["symbol"]; sc=stock["total_score"] or 0; gr=stock["grade"]
        price=float(stock.get("price") or 0)
        try:
            h=yf.Ticker(sym).history(period="2d",interval="1d",auto_adjust=True)
            if not h.empty: price=round(float(h["Close"].iloc[-1]),2)
        except Exception: pass
        if price<=0: continue
        qty=max(1,int((CAPITAL*POSITION_SIZE_PCT)/price))
        inv=round(qty*price,2); sl=round(price*(1-STOP_LOSS_PCT),2); tgt=round(price*(1+TARGET_PCT),2)
        try:
            supabase.table("portfolio").insert({
                "symbol":sym,"entry_date":TODAY,"entry_price":price,
                "quantity":qty,"invested_amount":inv,"current_price":price,"current_value":inv,
                "unrealised_pnl":0,"unrealised_pnl_pct":0,"stop_loss":sl,"target":tgt,
                "entry_grade":gr,"entry_score":sc,"status":"ACTIVE",
                "notes":f"Auto|Score:{sc}|Market:{market['verdict']}",
                "updated_at":datetime.now().isoformat()
            }).execute()
            entries.append({"symbol":sym,"price":price,"qty":qty,"sl":sl,"tgt":tgt,"score":sc})
            print(f"   BUY: {sym:<18} ₹{price:>8.2f} qty:{qty} SL:₹{sl} T:₹{tgt} [{gr}:{sc}]")
        except Exception as e: print(f"   ⚠️ {sym}: {e}")

print(f"   Entries today: {len(entries)}")

# ── Summary ──────────────────────────────────────────
fp=supabase.table("portfolio").select("*").eq("status","ACTIVE").execute().data
ti=sum(float(p.get("invested_amount") or 0) for p in fp)
tv=sum(float(p.get("current_value")   or 0) for p in fp)
tu=sum(float(p.get("unrealised_pnl")  or 0) for p in fp)
cr=CAPITAL-ti; cp_pct=round((cr/CAPITAL)*100,1)
pp=round((tu/ti*100) if ti>0 else 0,2)

print(f"\n{'='*55}")
print(f"  NIFTYMIND AI — PORTFOLIO {TODAY}")
print(f"{'='*55}")
print(f"  Holdings : {len(fp)}/{MAX_POSITIONS}  |  Market: {market['verdict']}")
print(f"  Invested : ₹{ti:,.0f}  |  Value: ₹{tv:,.0f}")
print(f"  P&L      : ₹{tu:+,.0f} ({pp:+.2f}%)")
print(f"  Cash     : ₹{cr:,.0f} ({cp_pct}%)")
print(f"  Entries  : {len(entries)}  |  Exits: {len(exits)}")
print(f"{'='*55}")

if fp:
    for p in sorted(fp,key=lambda x:float(x.get("unrealised_pnl_pct") or 0),reverse=True)[:10]:
        pct=float(p.get("unrealised_pnl_pct") or 0)
        print(f"  {p['symbol']:<18} ₹{p['entry_price']:>8} → ₹{str(p.get('current_price','?')):>8}  {pct:+.1f}%")

if len(master_results)>0:
    try:
        supabase.table("daily_summary").upsert({
            "summary_date":TODAY,"portfolio_size":len(fp),"cash_pct":cp_pct,
            "daily_pnl_pct":pp,"new_entries":len(entries),"exits":len(exits),
            "watching":len([r for r in master_results if r.get("grade") in ["A+","A"]]),
            "market_context":market["verdict"]
        },on_conflict="summary_date").execute()
        print(f"\n✅ daily_summary updated!")
    except Exception as e:
        print(f"   ⚠️ daily_summary error: {e}")

print(f"\n✅ ALL DONE! {len(entries)} entries | {len(exits)} exits | {len(fp)} holdings")
print(f"   Next auto-run: Tomorrow 5:00 PM IST")
