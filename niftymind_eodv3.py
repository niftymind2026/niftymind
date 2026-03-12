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
# SECTION 12: MARKET CONTEXT ENGINE (REDESIGNED)
# Nifty 60% | VIX 20% | FII+DII 20%
# Nifty = Daily 40% + Weekly 60% + Monthly bonus
# FII+DII = Weekly cumulative total
# =============================================================

print("\n🌍 SECTION 12: MARKET CONTEXT ENGINE")
print("-" * 40)

def fetch_nifty_full():
    """
    Fetch Nifty data for:
    - Daily change (today vs yesterday)
    - Weekly change (today vs 5 days ago)
    - Monthly trend (EMA19 vs EMA55 on monthly)
    """
    try:
        # Daily + weekly from daily data (1 month)
        daily = yf.download("^NSEI", period="1mo", interval="1d",
                            progress=False, auto_adjust=True)
        if daily.empty: raise Exception("Empty daily")

        closes      = [float(daily["Close"].iloc[i].iloc[0]) for i in range(len(daily))]
        today_close = closes[-1]
        prev_close  = closes[-2]
        week_ago    = closes[-6] if len(closes) >= 6 else closes[0]

        daily_chg  = ((today_close - prev_close) / prev_close) * 100
        weekly_chg = ((today_close - week_ago)   / week_ago)   * 100

        # Monthly trend from monthly data (3 years for EMA)
        monthly = yf.download("^NSEI", period="3y", interval="1mo",
                              progress=False, auto_adjust=True)
        monthly_trend = "SIDEWAYS"
        ema19_val = ema55_val = 0
        if not monthly.empty and len(monthly) >= 55:
            mc = [float(monthly["Close"].iloc[i].iloc[0]) for i in range(len(monthly))]
            ema19 = float(pd.Series(mc).ewm(span=19, adjust=False).mean().iloc[-1])
            ema55 = float(pd.Series(mc).ewm(span=55, adjust=False).mean().iloc[-1])
            ema19_val = round(ema19, 2)
            ema55_val = round(ema55, 2)
            monthly_trend = "BULL" if ema19 > ema55 else "BEAR"

        print(f"   Nifty 50  : {today_close:,.0f}")
        print(f"   Day       : {daily_chg:+.2f}%")
        print(f"   Week      : {weekly_chg:+.2f}%")
        print(f"   Monthly   : EMA19={ema19_val:,.0f} vs EMA55={ema55_val:,.0f} → {monthly_trend}")

        return {
            "close":         today_close,
            "daily_chg":     daily_chg,
            "weekly_chg":    weekly_chg,
            "monthly_trend": monthly_trend,
            "ema19":         ema19_val,
            "ema55":         ema55_val
        }

    except Exception as e:
        print(f"   ⚠️ Nifty error: {e}")
        return {"close":0,"daily_chg":0,"weekly_chg":0,"monthly_trend":"SIDEWAYS","ema19":0,"ema55":0}


def score_nifty(data):
    """
    Nifty score = max 6.0 points
    Daily  40% = max 2.4 pts
    Weekly 60% = max 3.6 pts
    Monthly bonus/penalty = +0.5 / -0.5
    """
    dc = data["daily_chg"]
    wc = data["weekly_chg"]
    mt = data["monthly_trend"]

    # Daily score (max 2.4)
    if   dc >  1.5: ds = 2.4
    elif dc >  0.5: ds = 1.8
    elif dc > -0.5: ds = 1.2
    elif dc > -1.5: ds = 0.6
    else:           ds = 0.0

    # Weekly score (max 3.6)
    if   wc >  3.0: ws = 3.6
    elif wc >  1.5: ws = 2.7
    elif wc >  0.0: ws = 1.8
    elif wc > -1.5: ws = 0.9
    else:           ws = 0.0

    # Monthly trend bonus
    mb = +0.5 if mt == "BULL" else -0.5

    raw   = ds + ws + mb
    final = round(min(6.0, max(0.0, raw)), 2)

    print(f"   Nifty Score: Day({dc:+.2f}%→{ds}) + Week({wc:+.2f}%→{ws}) + Monthly({mt}→{mb:+}) = {final}/6.0")
    return final, ds, ws, mb


def fetch_vix():
    """VIX score = max 2.0 points"""
    try:
        vix = yf.download("^INDIAVIX", period="3d", interval="1d",
                          progress=False, auto_adjust=True)
        if vix.empty: raise Exception("Empty")
        vv = float(vix["Close"].iloc[-1].iloc[0])
        if   vv < 13: vs = 2.0; lbl = "VERY_LOW"
        elif vv < 16: vs = 1.6; lbl = "LOW"
        elif vv < 20: vs = 1.2; lbl = "MODERATE"
        elif vv < 25: vs = 0.6; lbl = "HIGH"
        elif vv < 30: vs = 0.2; lbl = "VERY_HIGH"
        else:         vs = 0.0; lbl = "EXTREME"
        print(f"   VIX Score : {vv:.2f} ({lbl}) = {vs}/2.0")
        return vs, vv, lbl
    except Exception as e:
        print(f"   ⚠️ VIX error: {e} — using MODERATE fallback")
        return 1.2, 20.0, "MODERATE"


def fetch_fii_dii_weekly():
    """
    FII+DII weekly cumulative score = max 2.0 points
    Fetches last 5 trading days and sums combined net flow
    Falls back to NEUTRAL (1.0) if API fails
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept":     "application/json, text/plain, */*",
            "Referer":    "https://www.nseindia.com/market-data/fii-dii-activity"
        }
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        time.sleep(2)
        session.get("https://www.nseindia.com/market-data/fii-dii-activity",
                    headers=headers, timeout=10)
        time.sleep(1)
        data = session.get("https://www.nseindia.com/api/fiidiiTradeReact",
                           headers=headers, timeout=10).json()

        # Today's data
        fii_row = next((r for r in data if "FII" in r.get("category","").upper()), None)
        dii_row = next((r for r in data if "DII" in r.get("category","").upper()), None)
        today_fii = float(fii_row["netValue"]) if fii_row else 0.0
        today_dii = float(dii_row["netValue"]) if dii_row else 0.0
        today_combined = today_fii + today_dii

        # Try to get last 5 days cumulative from agent_memory
        # Store today's value and sum last 5 days from DB
        try:
            supabase.table("agent_memory").upsert({
                "memory_type": "FII_DII_DAILY",
                "symbol":      "MARKET",
                "context":     TODAY,
                "outcome":     str(round(today_combined, 2)),
                "lesson":      f"FII:{today_fii:.0f} DII:{today_dii:.0f}"
            }).execute()
        except Exception:
            pass

        # Fetch last 5 records from agent_memory for weekly cumulative
        hist = supabase.table("agent_memory") \
            .select("outcome") \
            .eq("memory_type", "FII_DII_DAILY") \
            .eq("symbol", "MARKET") \
            .order("context", desc=True) \
            .limit(5) \
            .execute()

        if hist.data and len(hist.data) >= 3:
            weekly_total = sum(float(r["outcome"]) for r in hist.data)
        else:
            weekly_total = today_combined  # fallback to today only

        # Score based on weekly cumulative
        if   weekly_total >  10000: fs = 2.0
        elif weekly_total >   5000: fs = 1.6
        elif weekly_total >   2000: fs = 1.2
        elif weekly_total >      0: fs = 0.8
        elif weekly_total >  -2000: fs = 0.4
        elif weekly_total >  -5000: fs = 0.2
        else:                       fs = 0.0

        print(f"   FII/DII   : Today={today_combined:,.0f}Cr | Weekly={weekly_total:,.0f}Cr = {fs}/2.0")
        return fs, today_fii, today_dii, weekly_total

    except Exception as e:
        print(f"   ⚠️ FII/DII error: {e} — using NEUTRAL fallback (1.0)")
        return 1.0, 0.0, 0.0, 0.0  # NEUTRAL as decided


def calc_market_score_v2(nifty_score, vix_score, fii_score, nifty_data, vix_val, weekly_flow):
    """
    Final market context score:
    Nifty 60% + VIX 20% + FII+DII 20% = 10 points total
    Already pre-weighted in individual scores (max 6 + max 2 + max 2 = 10)
    """
    total = round(nifty_score + vix_score + fii_score, 2)

    if   total >= 8.0: verdict = "STRONG_BULL"
    elif total >= 6.0: verdict = "BULL"
    elif total >= 4.0: verdict = "NEUTRAL"
    elif total >= 2.0: verdict = "BEAR"
    else:              verdict = "STRONG_BEAR"

    emoji = "🟢🟢" if verdict=="STRONG_BULL" else "🟢" if verdict=="BULL" else \
            "🟡"   if verdict=="NEUTRAL"     else "🔴" if verdict=="BEAR" else "🔴🔴"

    print(f"\n   {'─'*45}")
    print(f"   MARKET CONTEXT SCORE: {total}/10.0  {emoji} {verdict}")
    print(f"   Nifty : {nifty_score}/6.0  (Day+Week+Monthly)")
    print(f"   VIX   : {vix_score}/2.0  ({vix_val:.1f})")
    print(f"   FII+DII: {fii_score}/2.0  (Weekly ₹{weekly_flow:,.0f}Cr)")
    print(f"   {'─'*45}")

    return {
        "final_score":  total,
        "verdict":      verdict,
        "nifty_score":  nifty_score,
        "vix_score":    vix_score,
        "fii_score":    fii_score,
        "weekly_flow":  weekly_flow
    }


# ── Run Market Context ──────────────────────────────
nifty_data   = fetch_nifty_full()
nifty_score, daily_pts, weekly_pts, monthly_bonus = score_nifty(nifty_data)
vix_score, vix_val, vix_lbl = fetch_vix()
fii_score, today_fii, today_dii, weekly_flow = fetch_fii_dii_weekly()
market = calc_market_score_v2(nifty_score, vix_score, fii_score,
                               nifty_data, vix_val, weekly_flow)

# Save initial daily_summary
if len(master_results) > 0:
    try:
        supabase.table("daily_summary").upsert({
            "summary_date":   TODAY,
            "portfolio_size": 0,
            "cash_pct":       100,
            "market_context": market["verdict"],
            "agent_thoughts": (
                f"Nifty:{nifty_data['close']:,.0f}|"
                f"Day:{nifty_data['daily_chg']:+.2f}%|"
                f"Week:{nifty_data['weekly_chg']:+.2f}%|"
                f"Monthly:{nifty_data['monthly_trend']}|"
                f"VIX:{vix_val:.1f}({vix_lbl})|"
                f"FII:₹{today_fii:,.0f}Cr|"
                f"DII:₹{today_dii:,.0f}Cr|"
                f"WeeklyFlow:₹{weekly_flow:,.0f}Cr|"
                f"Score:{market['final_score']}/10({market['verdict']})"
            )
        }, on_conflict="summary_date").execute()
        print(f"✅ daily_summary saved!")
    except Exception as e:
        print(f"   ⚠️ daily_summary error: {e}")

# Update market_context_score on all today's signals
if master_results:
    ok = 0
    market_score_int = int(round(market["final_score"]))
    for r in master_results:
        try:
            supabase.table("signals") \
                .update({"market_context_score": market_score_int}) \
                .eq("symbol", r["symbol"]).eq("detected_date", TODAY).execute()
            ok += 1
        except Exception:
            pass
    print(f"✅ Market context score applied to {ok} stocks")

# =============================================================
# SECTION 13: PORTFOLIO MANAGER — RANKING BASED (REDESIGNED)
# Daily re-rank all 500 stocks
# Sell any holding ranked below 40
# Replace with next best ranked stock
# Equal 4% allocation | Stop loss 7% | Target 20%
# Market score reduces effective score (never hard blocks)
# =============================================================

print("\n💼 SECTION 13: PORTFOLIO MANAGER")
print("-" * 40)

MAX_POSITIONS    = 25
CAPITAL          = 1000000    # Rs 10 Lakh
POSITION_SIZE    = 0.04       # 4% per stock = Rs 40,000
STOP_LOSS_PCT    = 0.07       # 7%
TARGET_PCT       = 0.20       # 20%
RANK_SELL_CUTOFF = 40         # sell if rank drops below this
MIN_SCORE_ENTRY  = 55         # A grade minimum to enter

# ── BUILD FULL RANKED LIST ──────────────────────────
# Add market score to each stock's effective score
# Market score (0-10) → bonus/penalty on total score
# STRONG_BULL(+3) BULL(+2) NEUTRAL(0) BEAR(-3) STRONG_BEAR(-6)
market_bonus = {
    "STRONG_BULL": +3,
    "BULL":        +2,
    "NEUTRAL":      0,
    "BEAR":        -3,
    "STRONG_BEAR": -6
}.get(market["verdict"], 0)

print(f"Market bonus/penalty : {market_bonus:+d} pts ({market['verdict']})")

# Build ranked list — all scored stocks today
ranked_all = []
for r in master_results:
    effective_score = (r["total_score"] or 0) + market_bonus
    ranked_all.append({
        "symbol":          r["symbol"],
        "name":            r["name"],
        "sector":          r["sector"],
        "price":           r["price"],
        "raw_score":       r["total_score"] or 0,
        "effective_score": effective_score,
        "grade":           r["grade"]
    })

# Sort by effective score descending → this is the rank
ranked_all.sort(key=lambda x: x["effective_score"], reverse=True)

# Assign ranks
for i, r in enumerate(ranked_all):
    r["rank"] = i + 1

rank_map = {r["symbol"]: r for r in ranked_all}

print(f"Total ranked stocks  : {len(ranked_all)}")
print(f"\n   TOP 10 TODAY:")
for r in ranked_all[:10]:
    print(f"   Rank {r['rank']:>3}  {r['symbol']:<18} "
          f"Raw:{r['raw_score']}  Eff:{r['effective_score']}  {r['grade']}")

# ── LOAD CURRENT PORTFOLIO ──────────────────────────
port = supabase.table("portfolio").select("*").eq("status", "ACTIVE").execute().data
print(f"\nCurrent holdings: {len(port)}/{MAX_POSITIONS}")

exits   = []
entries = []

# ── EXITS — 4 reasons ──────────────────────────────
print("\n🔴 CHECKING EXITS...")

for pos in port:
    sym = pos["symbol"]
    ep  = float(pos["entry_price"])
    sl  = float(pos["stop_loss"])
    tgt = float(pos["target"])
    qty = int(pos["quantity"])

    # Get current price
    try:
        h  = yf.Ticker(sym).history(period="2d", interval="1d", auto_adjust=True)
        cp = round(float(h["Close"].iloc[-1]), 2) if not h.empty else ep
    except Exception:
        cp = ep

    pct       = ((cp - ep) / ep) * 100
    stock_rank = rank_map.get(sym, {}).get("rank", 9999)
    exit_reason = None

    # Priority order for exits
    if cp <= sl:
        exit_reason = f"STOP_LOSS_HIT (₹{cp} ≤ ₹{sl})"
    elif cp >= tgt:
        exit_reason = f"TARGET_HIT (₹{cp} ≥ ₹{tgt})"
    elif stock_rank > RANK_SELL_CUTOFF:
        exit_reason = f"RANK_DROPPED (rank:{stock_rank} > {RANK_SELL_CUTOFF})"
    # Note: no hard STRONG_BEAR block — market score already penalises ranks

    if exit_reason:
        realised = round((cp - ep) * qty, 2)
        try:
            supabase.table("portfolio").update({
                "status":           "EXITED",
                "exit_date":        TODAY,
                "exit_price":       cp,
                "exit_reason":      exit_reason,
                "realised_pnl":     realised,
                "current_price":    cp,
                "current_value":    round(cp * qty, 2),
                "unrealised_pnl":   0,
                "unrealised_pnl_pct": 0,
                "updated_at":       datetime.now().isoformat()
            }).eq("symbol", sym).eq("status", "ACTIVE").execute()
            exits.append({
                "symbol": sym, "pnl_pct": round(pct, 2),
                "pnl_rs": realised, "reason": exit_reason, "rank": stock_rank
            })
            print(f"   EXIT: {sym:<18} Rank:{stock_rank:>4}  {pct:+.1f}%  {exit_reason[:40]}")
        except Exception as e:
            print(f"   ⚠️ Exit error {sym}: {e}")
    else:
        # Update current price for holds
        try:
            supabase.table("portfolio").update({
                "current_price":      cp,
                "current_value":      round(cp * qty, 2),
                "unrealised_pnl":     round((cp - ep) * qty, 2),
                "unrealised_pnl_pct": round(pct, 2),
                "updated_at":         datetime.now().isoformat()
            }).eq("symbol", sym).eq("status", "ACTIVE").execute()
        except Exception:
            pass

print(f"   Exits today: {len(exits)}")

# ── ENTRIES — fill slots with top ranked stocks ──────
exited_symbols = [e["symbol"] for e in exits]
held_symbols   = [p["symbol"] for p in port if p["symbol"] not in exited_symbols]
slots          = MAX_POSITIONS - len(held_symbols)

print(f"\n🟢 CHECKING ENTRIES ({slots} slots available)...")

# Candidates: top ranked, not already held, min effective score
candidates = [
    r for r in ranked_all
    if r["symbol"] not in held_symbols
    and r["effective_score"] >= MIN_SCORE_ENTRY
    and r["grade"] in ["A+", "A"]
]

print(f"   Candidates (rank 1-40, score≥{MIN_SCORE_ENTRY}): {len(candidates)}")

for stock in candidates[:slots]:
    sym   = stock["symbol"]
    rank  = stock["rank"]
    score = stock["effective_score"]
    grade = stock["grade"]
    price = float(stock.get("price") or 0)

    # Get fresh price
    try:
        h = yf.Ticker(sym).history(period="2d", interval="1d", auto_adjust=True)
        if not h.empty:
            price = round(float(h["Close"].iloc[-1]), 2)
    except Exception:
        pass

    if price <= 0:
        continue

    qty = max(1, int((CAPITAL * POSITION_SIZE) / price))
    inv = round(qty * price, 2)
    sl  = round(price * (1 - STOP_LOSS_PCT), 2)
    tgt = round(price * (1 + TARGET_PCT), 2)

    try:
        supabase.table("portfolio").insert({
            "symbol":          sym,
            "entry_date":      TODAY,
            "entry_price":     price,
            "quantity":        qty,
            "invested_amount": inv,
            "current_price":   price,
            "current_value":   inv,
            "unrealised_pnl":  0,
            "unrealised_pnl_pct": 0,
            "stop_loss":       sl,
            "target":          tgt,
            "entry_grade":     grade,
            "entry_score":     score,
            "status":          "ACTIVE",
            "notes":           f"Rank:{rank}|Raw:{stock['raw_score']}|Eff:{score}|Mkt:{market['verdict']}({market_bonus:+d})",
            "updated_at":      datetime.now().isoformat()
        }).execute()
        entries.append({
            "symbol": sym, "rank": rank, "price": price,
            "qty": qty, "sl": sl, "tgt": tgt, "score": score
        })
        print(f"   BUY: Rank:{rank:>3}  {sym:<18} ₹{price:>8.2f}  "
              f"qty:{qty}  SL:₹{sl}  T:₹{tgt}  [{grade}:{score}]")
    except Exception as e:
        print(f"   ⚠️ Entry error {sym}: {e}")

print(f"   Entries today: {len(entries)}")

# ── FINAL PORTFOLIO SUMMARY ──────────────────────────
fp  = supabase.table("portfolio").select("*").eq("status", "ACTIVE").execute().data
ti  = sum(float(p.get("invested_amount") or 0) for p in fp)
tv  = sum(float(p.get("current_value")   or 0) for p in fp)
tu  = sum(float(p.get("unrealised_pnl")  or 0) for p in fp)
cr  = CAPITAL - ti
cpc = round((cr / CAPITAL) * 100, 1)
pp  = round((tu / ti * 100) if ti > 0 else 0, 2)

print(f"\n{'='*60}")
print(f"  NIFTYMIND AI — PORTFOLIO SUMMARY  {TODAY}")
print(f"{'='*60}")
print(f"  Holdings  : {len(fp)}/{MAX_POSITIONS}")
print(f"  Invested  : ₹{ti:>12,.0f}")
print(f"  Value     : ₹{tv:>12,.0f}")
print(f"  P&L       : ₹{tu:>+12,.0f}  ({pp:+.2f}%)")
print(f"  Cash      : ₹{cr:>12,.0f}  ({cpc}%)")
print(f"  Entries   : {len(entries)}   Exits: {len(exits)}")
print(f"{'─'*60}")
print(f"  MARKET    : {market['final_score']}/10  {market['verdict']}")
print(f"  Nifty     : {market['nifty_score']}/6.0  "
      f"(Day:{nifty_data['daily_chg']:+.2f}% Wk:{nifty_data['weekly_chg']:+.2f}% "
      f"Monthly:{nifty_data['monthly_trend']})")
print(f"  VIX       : {market['vix_score']}/2.0  ({vix_val:.1f} {vix_lbl})")
print(f"  FII+DII   : {market['fii_score']}/2.0  (Weekly ₹{weekly_flow:,.0f}Cr)")
print(f"{'─'*60}")

if fp:
    print(f"  {'SYMBOL':<18} {'ENTRY':>8} {'NOW':>8} {'P&L':>8}  RANK")
    print(f"  {'─'*55}")
    for p in sorted(fp, key=lambda x: float(x.get("unrealised_pnl_pct") or 0), reverse=True):
        pct  = float(p.get("unrealised_pnl_pct") or 0)
        rank = rank_map.get(p["symbol"], {}).get("rank", "?")
        cp_  = p.get("current_price", "?")
        print(f"  {p['symbol']:<18} ₹{float(p['entry_price']):>7.2f} "
              f"₹{str(cp_):>7}  {pct:>+6.1f}%  #{rank}")

print(f"{'='*60}")

# Update daily_summary with full portfolio data
if len(master_results) > 0:
    try:
        supabase.table("daily_summary").upsert({
            "summary_date":   TODAY,
            "portfolio_size": len(fp),
            "cash_pct":       cpc,
            "daily_pnl_pct":  pp,
            "new_entries":    len(entries),
            "exits":          len(exits),
            "watching":       len([r for r in ranked_all if r["rank"] <= 40]),
            "market_context": market["verdict"],
            "full_summary": (
                f"Date:{TODAY} | "
                f"Holdings:{len(fp)}/{MAX_POSITIONS} | "
                f"P&L:₹{tu:+,.0f}({pp:+.2f}%) | "
                f"Cash:₹{cr:,.0f}({cpc}%) | "
                f"Entries:{len(entries)} Exits:{len(exits)} | "
                f"Market:{market['final_score']}/10 {market['verdict']} | "
                f"Nifty:{market['nifty_score']}/6(Day:{nifty_data['daily_chg']:+.2f}% "
                f"Wk:{nifty_data['weekly_chg']:+.2f}% {nifty_data['monthly_trend']}) | "
                f"VIX:{market['vix_score']}/2({vix_val:.1f} {vix_lbl}) | "
                f"FII+DII:{market['fii_score']}/2(₹{weekly_flow:,.0f}Cr weekly)"
            )
        }, on_conflict="summary_date").execute()
        print(f"\n✅ daily_summary updated with full breakdown!")
    except Exception as e:
        print(f"   ⚠️ daily_summary error: {e}")

print(f"\n✅ ALL DONE!")
print(f"   Entries:{len(entries)}  Exits:{len(exits)}  Holdings:{len(fp)}")
print(f"   Market: {market['final_score']}/10 {market['verdict']}")
print(f"   Next auto-run: Tomorrow 5:00 PM IST")
