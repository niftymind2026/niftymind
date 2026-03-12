# =============================================================
# NIFTYMIND AI — DAILY EOD ENGINE  v4
# Runs every weekday at 5:00 PM IST (11:30 AM UTC)
# Phases 1-7 + Full Dataset Building
# =============================================================

import os
import sys
import time
import json
import traceback
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
# SECTION 1: CONFIGURATION + ERROR TRACKER
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

# ── Central Error Log ─────────────────────────────────────────
# Every error is captured here and printed in final report
# Nothing is silently ignored
ERROR_LOG = []

def log_error(section, context, error):
    """Log every error with full traceback — nothing hidden"""
    tb  = traceback.format_exc()
    msg = f"[{section}] {context}: {error}"
    ERROR_LOG.append({"section": section, "context": context,
                      "error": str(error), "traceback": tb})
    print(f"   ⚠️  ERROR — {msg}")

def section_header(icon, num, title):
    print(f"\n{icon} SECTION {num}: {title}")
    print("-" * 50)

print("=" * 60)
print("  NIFTYMIND AI — DAILY EOD ENGINE  v4")
print(f"  Date     : {TODAY}")
print(f"  Run time : {NOW.strftime('%H:%M:%S')} UTC")
print("=" * 60)

# Validate all API keys present
missing_keys = []
if not SUPABASE_KEY: missing_keys.append("SUPABASE_KEY")
if not NEWSAPI_KEY:  missing_keys.append("NEWSAPI_KEY")
if not GROQ_KEY:     missing_keys.append("GROQ_KEY")
if missing_keys:
    print(f"\n❌ MISSING API KEYS: {', '.join(missing_keys)}")
    print("   Add these to GitHub Secrets and retry.")
    sys.exit(1)

print("✅ All API keys present")

# =============================================================
# SECTION 2: MARKET OPEN CHECK
# =============================================================

section_header("🔍", 2, "MARKET OPEN CHECK")

def is_market_open_today():
    try:
        nifty = yf.download("^NSEI", period="5d", interval="1d",
                            progress=False, auto_adjust=True)
        if nifty.empty:
            log_error("S2", "Nifty download", "Empty response from Yahoo Finance")
            return False, 0, 0

        latest_date  = nifty.index[-1].date()
        latest_close = float(nifty["Close"].iloc[-1].iloc[0])
        prev_close   = float(nifty["Close"].iloc[-2].iloc[0])
        week_ago     = float(nifty["Close"].iloc[0].iloc[0])
        daily_chg    = ((latest_close - prev_close) / prev_close) * 100
        weekly_chg   = ((latest_close - week_ago)   / week_ago)   * 100

        if latest_date < date.today():
            print(f"   Latest data: {latest_date} (not today) — NSE CLOSED (holiday)")
            return False, latest_close, weekly_chg

        print(f"   ✅ NSE OPEN | Nifty:{latest_close:,.0f} "
              f"Day:{daily_chg:+.2f}% Week:{weekly_chg:+.2f}%")
        return True, latest_close, weekly_chg

    except Exception as e:
        log_error("S2", "Market open check", e)
        print("   ⚠️  Could not verify — assuming market OPEN and continuing")
        return True, 0, 0

market_open, nifty_close_s2, nifty_weekly_s2 = is_market_open_today()

if not market_open:
    print(f"\n🏖️  Market closed today ({TODAY}) — clean exit. Zero rows saved.")
    sys.exit(0)

# =============================================================
# SECTION 3: DAILY EOD DATA UPDATE
# =============================================================

section_header("📥", 3, "DAILY EOD DATA UPDATE")

try:
    stocks_response = supabase.table("stocks").select("symbol,name,sector") \
        .eq("is_active", True).execute()
    all_stocks  = stocks_response.data
    all_symbols = [row["symbol"] for row in all_stocks]
    print(f"Active stocks: {len(all_symbols)}")
except Exception as e:
    log_error("S3", "Fetch stocks list", e)
    print("❌ FATAL: Cannot fetch stocks from Supabase.")
    print("   Fix: ALTER TABLE stocks DISABLE ROW LEVEL SECURITY;")
    sys.exit(1)

if len(all_symbols) == 0:
    print("❌ FATAL: Zero active stocks found.")
    print("   Fix: ALTER TABLE stocks DISABLE ROW LEVEL SECURITY;")
    sys.exit(1)

updated       = 0
failed        = 0
failed_symbols = []

for i, symbol in enumerate(all_symbols):
    try:
        ticker = yf.Ticker(symbol)
        hist   = ticker.history(period="5d", interval="1d", auto_adjust=True)

        if hist.empty:
            failed += 1
            failed_symbols.append(f"{symbol}(empty)")
            continue

        latest_date = hist.index[-1].date()
        if latest_date < date.today() - timedelta(days=3):
            failed += 1
            failed_symbols.append(f"{symbol}(stale:{latest_date})")
            continue

        latest = hist.iloc[-1]
        supabase.table("daily_candles").upsert([{
            "symbol": symbol,
            "date":   str(latest_date),
            "open":   round(float(latest["Open"]),  2),
            "high":   round(float(latest["High"]),  2),
            "low":    round(float(latest["Low"]),   2),
            "close":  round(float(latest["Close"]), 2),
            "volume": int(latest["Volume"])
        }], on_conflict="symbol,date").execute()

        # Recalculate current month candle
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

        payload = {
            "open": round(month_open,2), "high": round(month_high,2),
            "low":  round(month_low,2),  "close": round(month_close,2),
            "volume": month_volume, "is_complete": False
        }
        if existing.data:
            supabase.table("monthly_candles").update(payload) \
                .eq("symbol", symbol).eq("date", str(THIS_MONTH)).execute()
        else:
            supabase.table("monthly_candles").insert(
                {**payload, "symbol": symbol, "date": str(THIS_MONTH)}
            ).execute()

        updated += 1
        if (i + 1) % 100 == 0:
            print(f"  Progress: {i+1}/{len(all_symbols)} "
                  f"(✅{updated} ❌{failed})")
        time.sleep(0.2)

    except Exception as e:
        failed += 1
        failed_symbols.append(f"{symbol}({type(e).__name__})")
        log_error("S3", f"Update {symbol}", e)

print(f"✅ EOD Update: {updated} updated | {failed} failed")
if failed > 50:
    print(f"   ⚠️  HIGH FAILURE COUNT ({failed}) — possible Yahoo Finance rate limit")
if failed_symbols[:5]:
    print(f"   First failures: {', '.join(failed_symbols[:5])}")

if updated == 0:
    print("❌ FATAL: Zero stocks updated — market data unavailable. Exiting.")
    sys.exit(0)

# =============================================================
# SECTION 4: CORPORATE ACTION CHECKER
# =============================================================

section_header("🏢", 4, "CORPORATE ACTION CHECK")

flagged    = []
ca_checked = 0

for symbol in all_symbols:
    try:
        stored = supabase.table("monthly_candles").select("close,date") \
            .eq("symbol", symbol).eq("is_complete", True) \
            .order("date", desc=True).limit(1).execute()

        if not stored.data:
            continue

        stored_close = float(stored.data[0]["close"])
        hist         = yf.Ticker(symbol).history(
            period="3mo", interval="1mo", auto_adjust=True)

        if hist.empty or len(hist) < 2:
            continue

        yahoo_close = float(hist["Close"].iloc[-2])
        diff_pct    = abs(stored_close - yahoo_close) / stored_close * 100

        if diff_pct > 10:
            flagged.append({"symbol": symbol, "diff_pct": round(diff_pct, 1)})
            try:
                supabase.table("corporate_actions").insert({
                    "symbol":         symbol,
                    "detected_date":  TODAY,
                    "action_type":    "AUTO_DETECTED",
                    "stored_price":   stored_close,
                    "yahoo_price":    yahoo_close,
                    "difference_pct": round(diff_pct, 1),
                    "status":         "PENDING",
                    "notes":          f"Price diff {diff_pct:.1f}% — needs review"
                }).execute()
                supabase.table("stocks").update({
                    "status": "PENDING_REVIEW", "is_active": False
                }).eq("symbol", symbol).execute()
                print(f"   🚨 FLAGGED: {symbol} — {diff_pct:.1f}% price diff "
                      f"(stored:₹{stored_close} yahoo:₹{yahoo_close:.2f})")
            except Exception as e:
                log_error("S4", f"Save corporate action {symbol}", e)

        ca_checked += 1
        time.sleep(0.3)

    except Exception as e:
        log_error("S4", f"Check {symbol}", e)

print(f"✅ Corporate check: {ca_checked} checked | {len(flagged)} flagged & sidelined")

# =============================================================
# SECTION 5: ENGINE 1 — EMA CALCULATOR
# Saves: ema19_val, ema55_val, ema_slope to signals
# =============================================================

section_header("📊", 5, "ENGINE 1 — EMA CALCULATOR")

def calculate_ema(prices, period):
    return pd.Series(prices).ewm(span=period, adjust=False).mean()

try:
    ema_stocks = supabase.table("stocks").select("symbol,name,sector") \
        .eq("data_quality", "SUFFICIENT").eq("is_active", True).execute().data
except Exception as e:
    log_error("S5", "Fetch stocks for EMA", e)
    ema_stocks = []

qualified      = []
ema_errors     = []
# Store EMA values for later use in signals detail saving
ema_data_cache = {}  # symbol → {ema19, ema55, slope}

for i, stock in enumerate(ema_stocks):
    try:
        symbol  = stock["symbol"]
        candles = supabase.table("monthly_candles") \
            .select("date,open,high,low,close,volume") \
            .eq("symbol", symbol).order("date", desc=False).execute()

        if not candles.data or len(candles.data) < 55:
            continue

        closes       = [float(c["close"]) for c in candles.data]
        ema19_series = calculate_ema(closes, 19)
        ema55_series = calculate_ema(closes, 55)

        current_close = closes[-1]
        current_ema19 = round(float(ema19_series.iloc[-1]), 2)
        current_ema55 = round(float(ema55_series.iloc[-1]), 2)
        prev_ema19    = round(float(ema19_series.iloc[-2]), 2)
        ema_slope     = round(current_ema19 - prev_ema19, 2)

        is_qualified  = (current_ema19 > current_ema55 and
                         current_close > current_ema19 and
                         current_ema19 > prev_ema19)

        if is_qualified:
            qualified.append(symbol)

        # Cache for later use in signals detail
        ema_data_cache[symbol] = {
            "ema19": current_ema19,
            "ema55": current_ema55,
            "slope": ema_slope
        }

        supabase.table("signals").upsert({
            "symbol":          symbol,
            "detected_date":   TODAY,
            "pattern_type":    "EMA_STRUCTURE",
            "direction":       "BULLISH" if is_qualified else "BEARISH",
            "price":           current_close,
            "technical_score": 25 if is_qualified else 0,
            "grade":           "QUALIFIED" if is_qualified else "NOT_QUALIFIED",
            "ema19_val":       current_ema19,
            "ema55_val":       current_ema55,
            "ema_slope":       ema_slope
        }).execute()

        if (i + 1) % 50 == 0:
            print(f"  Progress: {i+1}/{len(ema_stocks)} | Qualified: {len(qualified)}")

    except Exception as e:
        ema_errors.append(symbol)
        log_error("S5", f"EMA calc {symbol}", e)

qualified_symbols = qualified[:]
print(f"✅ EMA Engine: {len(qualified_symbols)} qualified | "
      f"{len(ema_stocks)-len(qualified_symbols)} not qualified | "
      f"{len(ema_errors)} errors")

if len(qualified_symbols) == 0:
    print("   ⚠️  ZERO qualified stocks — check data quality or monthly candles table")

# =============================================================
# SECTION 6: ENGINE 2 — PATTERN DETECTION
# Saves: channel_type to signals
# =============================================================

section_header("📈", 6, "ENGINE 2 — PATTERN DETECTION")

def detect_swing_points(highs, lows, closes, lookback=3):
    sh, sl = [], []
    n = len(highs)
    for i in range(lookback, n - lookback):
        if (all(highs[i] > highs[i-j] for j in range(1, lookback+1)) and
                all(highs[i] > highs[i+j] for j in range(1, lookback+1))):
            sh.append({"index": i, "price": highs[i]})
        if (all(lows[i] < lows[i-j] for j in range(1, lookback+1)) and
                all(lows[i] < lows[i+j] for j in range(1, lookback+1))):
            sl.append({"index": i, "price": lows[i]})
    return sh, sl

def detect_trend(pts, min_points=2):
    if len(pts) < min_points: return "INSUFFICIENT"
    prices = [p["price"] for p in pts[-min_points:]]
    if all(prices[i] > prices[i-1] for i in range(1, len(prices))): return "UPTREND"
    if all(prices[i] < prices[i-1] for i in range(1, len(prices))): return "DOWNTREND"
    return "SIDEWAYS"

def detect_channel(sh, sl):
    if len(sh) < 2 or len(sl) < 2: return "NO_CHANNEL"
    ht, lt = detect_trend(sh), detect_trend(sl)
    if ht == "UPTREND"   and lt == "UPTREND":   return "ASCENDING_CHANNEL"
    if ht == "DOWNTREND" and lt == "DOWNTREND": return "DESCENDING_CHANNEL"
    if ht == "SIDEWAYS"  and lt == "SIDEWAYS":  return "HORIZONTAL_CHANNEL"
    return "NO_CHANNEL"

pattern_results    = []
pattern_errors     = []
channel_cache      = {}  # symbol → channel_type

for i, symbol in enumerate(qualified_symbols):
    try:
        candles = supabase.table("monthly_candles") \
            .select("date,open,high,low,close,volume") \
            .eq("symbol", symbol).order("date", desc=False).execute()

        if not candles.data or len(candles.data) < 20:
            continue

        closes = [float(c["close"]) for c in candles.data]
        highs  = [float(c["high"])  for c in candles.data]
        lows   = [float(c["low"])   for c in candles.data]
        ema19  = list(calculate_ema(closes, 19))
        ema55  = list(calculate_ema(closes, 55))

        sh, sl    = detect_swing_points(highs, lows, closes)
        channel   = detect_channel(sh, sl)
        low_trend = detect_trend(sl)

        current, prev = closes[-1], closes[-2]
        breakouts = []
        if sh and current > sh[-1]["price"] and prev <= sh[-1]["price"]:
            breakouts.append("BREAKOUT_ABOVE_HIGH")
        if sl and current < sl[-1]["price"] and prev >= sl[-1]["price"]:
            breakouts.append("BREAKDOWN_BELOW_LOW")
        if current > ema19[-1] and prev < ema19[-2]:
            breakouts.append("RECLAIMED_EMA19")

        ps = 0
        if low_trend == "UPTREND":                 ps += 8
        if detect_trend(sh) == "UPTREND":          ps += 5
        if channel == "ASCENDING_CHANNEL":          ps += 7
        elif channel == "HORIZONTAL_CHANNEL":       ps += 3
        if "BREAKOUT_ABOVE_HIGH" in breakouts:      ps += 10
        if "RECLAIMED_EMA19"     in breakouts:      ps += 5
        if "BREAKDOWN_BELOW_LOW" in breakouts:      ps -= 10
        ps = min(25, max(0, ps))

        channel_cache[symbol] = channel

        supabase.table("signals").upsert({
            "symbol":            symbol,
            "detected_date":     TODAY,
            "pattern_type":      channel if channel != "NO_CHANNEL" else "TREND_" + low_trend,
            "direction":         "BULLISH" if ps > 10 else "NEUTRAL",
            "price":             closes[-1],
            "trendline_touches": len(sl),
            "technical_score":   ps,
            "grade":             "QUALIFIED",
            "channel_type":      channel
        }).execute()

        pattern_results.append({"symbol": symbol, "pattern_score": ps, "channel": channel})

        if (i + 1) % 25 == 0:
            print(f"  Progress: {i+1}/{len(qualified_symbols)}")

    except Exception as e:
        pattern_errors.append(symbol)
        log_error("S6", f"Pattern {symbol}", e)

print(f"✅ Pattern Engine: {len(pattern_results)} analyzed | {len(pattern_errors)} errors")

# =============================================================
# SECTION 7: ENGINE 3 — VPA ANALYSIS
# Saves: vol_ratio to signals
# =============================================================

section_header("📊", 7, "ENGINE 3 — VPA ANALYSIS")

def analyze_vpa(closes, highs, lows, opens, volumes):
    signals, vpa_score = [], 0
    n = len(closes)
    if n < 12: return signals, vpa_score, 1.0

    avg_vol_10  = pd.Series(volumes).rolling(10).mean()
    curr_close  = closes[-1]; curr_open = opens[-1]
    curr_high   = highs[-1];  curr_low  = lows[-1]
    curr_vol    = volumes[-1]; curr_avg  = avg_vol_10.iloc[-1]
    vol_ratio   = curr_vol / curr_avg if curr_avg > 0 else 1
    body        = abs(curr_close - curr_open)
    total_range = curr_high - curr_low
    if total_range == 0: return signals, vpa_score, round(vol_ratio, 2)

    body_pct   = body / total_range
    wick_ratio = 1 - body_pct
    is_bullish = curr_close > curr_open

    if vol_ratio > 2.0 and wick_ratio > 0.6:
        pp = (curr_close - min(closes[-12:])) / \
             (max(closes[-12:]) - min(closes[-12:]) + 0.001)
        if pp < 0.3: signals.append("BUYING_CLIMAX");  vpa_score += 6
        else:        signals.append("SELLING_CLIMAX"); vpa_score -= 4
    elif is_bullish     and vol_ratio < 0.7: signals.append("NO_DEMAND");           vpa_score -= 2
    elif not is_bullish and vol_ratio < 0.7: signals.append("NO_SUPPLY");           vpa_score += 4
    elif is_bullish     and vol_ratio > 1.5: signals.append("VOLUME_CONFIRMED_UP"); vpa_score += 5
    elif not is_bullish and vol_ratio > 1.5: signals.append("HIGH_VOL_SELLING");    vpa_score -= 3

    if n >= 4:
        lc  = closes[-4:-1];  lh = highs[-4:-1]
        ll  = lows[-4:-1];    lv = volumes[-4:-1]
        lav = avg_vol_10.iloc[-4:-1].mean()
        if ((max(lh) - min(ll)) / closes[-4] * 100 < 8 and
                np.mean(lv) > lav and lc[-1] >= lc[0]):
            signals.append("ACCUMULATION"); vpa_score += 6

    if n >= 6:
        lc6 = closes[-6:]; lv6 = volumes[-6:]
        uv  = [lv6[i] for i in range(1, 6) if lc6[i] > lc6[i-1]]
        dv  = [lv6[i] for i in range(1, 6) if lc6[i] < lc6[i-1]]
        if uv and dv:
            if np.mean(uv) > np.mean(dv) * 1.2:
                signals.append("HEALTHY_UPTREND");   vpa_score += 4
            elif np.mean(dv) > np.mean(uv) * 1.2:
                signals.append("WEAKENING_UPTREND"); vpa_score -= 2

    return signals, min(15, max(0, vpa_score)), round(vol_ratio, 2)

vpa_results   = []
vpa_errors    = []
vol_ratio_cache = {}  # symbol → vol_ratio

for i, symbol in enumerate(qualified_symbols):
    try:
        candles = supabase.table("monthly_candles") \
            .select("date,open,high,low,close,volume") \
            .eq("symbol", symbol).order("date", desc=False).execute()

        if not candles.data or len(candles.data) < 12:
            continue

        closes  = [float(c["close"])  for c in candles.data]
        opens   = [float(c["open"])   for c in candles.data]
        highs   = [float(c["high"])   for c in candles.data]
        lows    = [float(c["low"])    for c in candles.data]
        volumes = [float(c["volume"]) for c in candles.data]

        vpa_sigs, vpa_score, vol_ratio = analyze_vpa(closes, highs, lows, opens, volumes)

        vol_ratio_cache[symbol] = vol_ratio

        supabase.table("signals").upsert({
            "symbol":        symbol,
            "detected_date": TODAY,
            "pattern_type":  "VPA",
            "direction":     "BULLISH" if vpa_score >= 8 else "NEUTRAL",
            "price":         closes[-1],
            "vpa_score":     vpa_score,
            "grade":         "QUALIFIED",
            "vol_ratio":     vol_ratio
        }).execute()

        vpa_results.append({"symbol": symbol, "vpa_score": vpa_score})

        if (i + 1) % 25 == 0:
            print(f"  Progress: {i+1}/{len(qualified_symbols)}")

    except Exception as e:
        vpa_errors.append(symbol)
        log_error("S7", f"VPA {symbol}", e)

print(f"✅ VPA Engine: {len(vpa_results)} analyzed | {len(vpa_errors)} errors")

# =============================================================
# SECTION 8: ENGINE 4 — CANDLESTICK SCANNER
# =============================================================

section_header("🕯️", 8, "ENGINE 4 — CANDLESTICK SCANNER")

def detect_single_candle(o, h, l, c):
    body = abs(c - o); r = h - l
    if r < 0.001: return "NONE"
    bp = body / r; uw = h - max(o, c); lw = min(o, c) - l; bull = c > o
    if bp < 0.1:
        if lw > uw * 2: return "DRAGONFLY_DOJI"
        if uw > lw * 2: return "GRAVESTONE_DOJI"
        return "DOJI"
    if lw > body * 2 and uw < body * 0.3: return "HAMMER"       if bull else "HANGING_MAN"
    if uw > body * 2 and lw < body * 0.3: return "SHOOTING_STAR" if not bull else "INVERTED_HAMMER"
    if bp > 0.85: return "BULL_MARUBOZU" if bull else "BEAR_MARUBOZU"
    return "NORMAL"

def detect_two_candle(p, c):
    po, ph, pl, pc = p; co, ch, cl, cc = c; pb = pc > po; cb = cc > co
    if not pb and cb and co <= pc and cc >= po: return "BULLISH_ENGULFING"
    if pb and not cb and co >= pc and cc <= po: return "BEARISH_ENGULFING"
    if not pb and cb and co < pl  and cc > (po + pc) / 2: return "PIERCING_LINE"
    if pb and not cb and co > ph  and cc < (po + pc) / 2: return "DARK_CLOUD_COVER"
    return "NONE"

def detect_three_candle(c1, c2, c3):
    o1, h1, l1, cl1 = c1; o2, h2, l2, cl2 = c2; o3, h3, l3, cl3 = c3
    bp2 = abs(cl2 - o2) / (h2 - l2) if (h2 - l2) > 0 else 0
    if cl1 < o1 and bp2 < 0.3 and cl3 > o3 and cl3 > (o1+cl1)/2: return "MORNING_STAR"
    if cl1 > o1 and bp2 < 0.3 and cl3 < o3 and cl3 < (o1+cl1)/2: return "EVENING_STAR"
    if cl1>o1 and cl2>o2 and cl3>o3 and cl2>cl1 and cl3>cl2: return "THREE_WHITE_SOLDIERS"
    if cl1<o1 and cl2<o2 and cl3<o3 and cl2<cl1 and cl3<cl2: return "THREE_BLACK_CROWS"
    return "NONE"

CANDLE_SCORES = {
    "HAMMER": 3, "DRAGONFLY_DOJI": 2, "BULL_MARUBOZU": 2, "DOJI": 1,
    "SHOOTING_STAR": -3, "GRAVESTONE_DOJI": -2, "HANGING_MAN": -2, "BEAR_MARUBOZU": -2,
    "BULLISH_ENGULFING": 4, "PIERCING_LINE": 3,
    "BEARISH_ENGULFING": -4, "DARK_CLOUD_COVER": -3,
    "MORNING_STAR": 5, "THREE_WHITE_SOLDIERS": 4,
    "EVENING_STAR": -5, "THREE_BLACK_CROWS": -4
}

def backward_candle_scan(data, lookback=6):
    if len(data) < lookback + 3: return 0, "NONE"
    w = data[-lookback:]; ts = 0
    for cd in w:
        p = detect_single_candle(cd["open"], cd["high"], cd["low"], cd["close"])
        if p not in ["NONE", "NORMAL"]: ts += CANDLE_SCORES.get(p, 0)
    for i in range(1, len(w)):
        p = detect_two_candle(
            (w[i-1]["open"], w[i-1]["high"], w[i-1]["low"], w[i-1]["close"]),
            (w[i]["open"],   w[i]["high"],   w[i]["low"],   w[i]["close"]))
        if p != "NONE": ts += CANDLE_SCORES.get(p, 0)
    for i in range(2, len(w)):
        p = detect_three_candle(
            (w[i-2]["open"], w[i-2]["high"], w[i-2]["low"], w[i-2]["close"]),
            (w[i-1]["open"], w[i-1]["high"], w[i-1]["low"], w[i-1]["close"]),
            (w[i]["open"],   w[i]["high"],   w[i]["low"],   w[i]["close"]))
        if p != "NONE": ts += CANDLE_SCORES.get(p, 0)
    sc = min(15, max(0, ts))
    cv = "HIGH" if sc >= 10 else "MEDIUM" if sc >= 6 else "LOW" if sc >= 3 else "NONE"
    return sc, cv

candle_results = []
candle_errors  = []

for i, symbol in enumerate(qualified_symbols):
    try:
        candles = supabase.table("monthly_candles") \
            .select("date,open,high,low,close") \
            .eq("symbol", symbol).order("date", desc=False).execute()

        if not candles.data or len(candles.data) < 9:
            continue

        score, conviction = backward_candle_scan(candles.data)

        supabase.table("signals").upsert({
            "symbol":        symbol,
            "detected_date": TODAY,
            "pattern_type":  "CANDLE_SCAN",
            "direction":     "BULLISH" if score >= 6 else "NEUTRAL",
            "price":         candles.data[-1]["close"],
            "candle_score":  score,
            "grade":         conviction
        }).execute()

        candle_results.append({"symbol": symbol, "score": score})

        if (i + 1) % 25 == 0:
            print(f"  Progress: {i+1}/{len(qualified_symbols)}")

    except Exception as e:
        candle_errors.append(symbol)
        log_error("S8", f"Candle {symbol}", e)

print(f"✅ Candle Engine: {len(candle_results)} scanned | {len(candle_errors)} errors")

# =============================================================
# SECTION 9: ENGINE 5 — ATTRAOS (CHAOS THEORY)
# Saves: attraos_zone, direction_prob, chaos_score to signals
# =============================================================

section_header("🌀", 9, "ENGINE 5 — ATTRAOS (CHAOS THEORY)")

def reconstruct_phase_space(prices, d=3, tau=1):
    prices = np.array(prices); n = len(prices); m = n - (d - 1) * tau
    if m <= 0: return None
    ps = np.zeros((m, d))
    for i in range(m):
        for j in range(d): ps[i, j] = prices[i + j * tau]
    return ps

def detect_attractor_zone(ps, cp):
    if ps is None or len(ps) < 10: return "INSUFFICIENT_DATA", 0.5
    centroid  = np.mean(ps, axis=0)
    distances = [np.linalg.norm(p - centroid) for p in ps]
    avg       = np.mean(distances); std = np.std(distances)
    rd        = [np.linalg.norm(p - centroid) for p in ps[-3:]]
    dt        = rd[-1] - rd[0] if len(rd) >= 2 else 0
    if std / (avg + 0.001) > 0.5:  return "CHAOS",       0.3
    if dt > std * 0.3:              return "EXPANSION",   min(0.85, 0.6 + dt/(avg+0.001))
    if dt < -std * 0.3:             return "CONTRACTION", 0.4
    return "TRANSITION", 0.5

def calc_dir_prob(ps, prices, d=3):
    if ps is None or len(ps) < 15: return 0.5
    prices = np.array(prices); n = len(ps); cur = ps[-1]
    dists  = sorted([(np.linalg.norm(ps[i] - cur), i) for i in range(n - 1)])
    nb     = [idx for _, idx in dists if idx < n - 3][:5]
    if not nb: return 0.5
    up = sum(1 for i in nb if i + d < len(prices) and prices[i+d] > prices[i+d-1])
    return round(up / len(nb), 2)

def calc_chaos_score(prices):
    prices = np.array(prices); n = len(prices)
    if n < 20: return 0.5
    ret   = np.diff(prices) / prices[:-1]; signs = np.sign(ret)
    cons  = np.mean([1 if signs[i] == signs[i-1] else 0 for i in range(1, len(signs))])
    rv    = pd.Series(ret).rolling(6).std()
    vov   = rv.std() / (rv.mean() + 0.001)
    return round((1 - cons) * 0.5 + min(1, vov) * 0.5, 2)

def attraos_score_calc(zone, dp, chaos):
    s  = {"EXPANSION": 10, "TRANSITION": 5, "CONTRACTION": 2,
          "CHAOS": 0, "INSUFFICIENT_DATA": 0}.get(zone, 0)
    s += 10 if dp >= 0.75 else 7 if dp >= 0.60 else 4 if dp >= 0.50 else 0
    s += 5  if chaos < 0.3 else 3 if chaos < 0.5 else 1 if chaos < 0.7 else 0
    return min(25, s)

attraos_results = []
attraos_errors  = []
attraos_cache   = {}  # symbol → {zone, dir_prob, chaos}

for i, symbol in enumerate(qualified_symbols):
    try:
        candles = supabase.table("monthly_candles").select("date,close") \
            .eq("symbol", symbol).order("date", desc=False).execute()

        if not candles.data or len(candles.data) < 24:
            continue

        prices = [float(c["close"]) for c in candles.data]
        ps     = reconstruct_phase_space(prices)
        if ps is None or len(ps) < 10:
            continue

        zone, _  = detect_attractor_zone(ps, ps[-1])
        dp       = calc_dir_prob(ps, prices)
        chaos    = calc_chaos_score(prices)
        a_score  = attraos_score_calc(zone, dp, chaos)

        attraos_cache[symbol] = {"zone": zone, "dir_prob": dp, "chaos": chaos}

        supabase.table("signals").upsert({
            "symbol":         symbol,
            "detected_date":  TODAY,
            "pattern_type":   "ATTRAOS",
            "direction":      "BULLISH" if a_score >= 15 else "NEUTRAL",
            "price":          prices[-1],
            "attraos_score":  a_score,
            "grade":          zone,
            "attraos_zone":   zone,
            "direction_prob": dp,
            "chaos_score":    chaos
        }).execute()

        attraos_results.append({"symbol": symbol, "attraos_score": a_score, "zone": zone})

        if (i + 1) % 25 == 0:
            print(f"  Progress: {i+1}/{len(qualified_symbols)}")

    except Exception as e:
        attraos_errors.append(symbol)
        log_error("S9", f"Attraos {symbol}", e)

print(f"✅ Attraos Engine: {len(attraos_results)} analyzed | {len(attraos_errors)} errors")

# =============================================================
# SECTION 10: MASTER SCORING ENGINE
# Also seeds pattern_outcomes for forward return tracking
# =============================================================

section_header("🏆", 10, "MASTER SCORING ENGINE")

master_results   = []
master_errors    = []
# Full score breakdown cache for trade_outcomes
score_breakdown  = {}  # symbol → all individual scores

for symbol in qualified_symbols:
    try:
        si = supabase.table("stocks").select("name,sector") \
            .eq("symbol", symbol).execute()
        if not si.data: continue
        name   = si.data[0]["name"]
        sector = si.data[0]["sector"]

        def gs(pt, field):
            r = supabase.table("signals").select(field) \
                .eq("symbol", symbol).eq("pattern_type", pt) \
                .eq("detected_date", TODAY).execute()
            return float(r.data[0].get(field, 0) or 0) if r.data else 0.0

        er = supabase.table("signals").select("technical_score,price") \
            .eq("symbol", symbol).eq("pattern_type", "EMA_STRUCTURE") \
            .eq("detected_date", TODAY).execute()
        ema_score = float(er.data[0].get("technical_score", 0) or 0) if er.data else 0.0
        cp        = float(er.data[0].get("price", 0) or 0) if er.data else 0.0

        pr = supabase.table("signals").select("technical_score") \
            .eq("symbol", symbol).eq("detected_date", TODAY) \
            .neq("pattern_type", "EMA_STRUCTURE").neq("pattern_type", "VPA") \
            .neq("pattern_type", "CANDLE_SCAN").neq("pattern_type", "ATTRAOS") \
            .neq("pattern_type", "MASTER_SCORE").execute()
        pattern_score = float(pr.data[0].get("technical_score", 0) or 0) if pr.data else 0.0

        vpa_score    = gs("VPA",        "vpa_score")
        candle_score = gs("CANDLE_SCAN", "candle_score")
        a_score      = gs("ATTRAOS",     "attraos_score")

        total = ema_score + pattern_score + vpa_score + candle_score + a_score
        grade = ("A+" if total >= 70 else "A" if total >= 55 else
                 "B"  if total >= 40 else "C" if total >= 25 else "SKIP")

        master_results.append({
            "symbol": symbol, "name": name, "sector": sector,
            "price": cp, "total_score": total, "grade": grade
        })

        # Cache full breakdown for trade_outcomes
        score_breakdown[symbol] = {
            "ema_score":     ema_score,
            "pattern_score": pattern_score,
            "vpa_score":     vpa_score,
            "candle_score":  candle_score,
            "attraos_score": a_score,
            "total_score":   total,
            "grade":         grade
        }

        ex = supabase.table("signals").select("id") \
            .eq("symbol", symbol).eq("detected_date", TODAY) \
            .eq("pattern_type", "MASTER_SCORE").execute()

        row = {
            "technical_score": ema_score,   "vpa_score":    vpa_score,
            "candle_score":    candle_score, "attraos_score": a_score,
            "total_score":     total,        "grade":         grade,
            "price":           cp,           "direction":     "BULLISH" if total >= 55 else "NEUTRAL"
        }
        if ex.data:
            supabase.table("signals").update(row) \
                .eq("symbol", symbol).eq("detected_date", TODAY) \
                .eq("pattern_type", "MASTER_SCORE").execute()
        else:
            supabase.table("signals").insert(
                {**row, "symbol": symbol, "detected_date": TODAY,
                 "pattern_type": "MASTER_SCORE"}
            ).execute()

    except Exception as e:
        master_errors.append(symbol)
        log_error("S10", f"Master score {symbol}", e)

master_results.sort(key=lambda x: x["total_score"], reverse=True)
aplus = sum(1 for r in master_results if r["grade"] == "A+")
a_cnt = sum(1 for r in master_results if r["grade"] == "A")
b_cnt = sum(1 for r in master_results if r["grade"] == "B")

print(f"✅ Master Scoring: {len(master_results)} scored | "
      f"A+:{aplus} A:{a_cnt} B:{b_cnt} | {len(master_errors)} errors")

if master_results:
    print(f"\n   TOP 10 TODAY:")
    for idx, r in enumerate(master_results[:10], 1):
        print(f"   {idx:>2}. {r['symbol']:<18} {r['total_score']:>5.1f}/80  "
              f"{r['grade']:<4}  {r['sector']}")

# ── Seed pattern_outcomes for forward return tracking ─────────
# Every A+/A stock detected today gets a row
# Phase 8 will fill in 1/3/6 month prices later
po_seeded = 0
po_errors = []
for r in master_results:
    if r["grade"] not in ["A+", "A"]:
        continue
    try:
        # Avoid duplicates
        existing = supabase.table("pattern_outcomes") \
            .select("id").eq("symbol", r["symbol"]) \
            .eq("pattern_detected_date", TODAY).execute()
        if existing.data:
            continue

        supabase.table("pattern_outcomes").insert({
            "symbol":                      r["symbol"],
            "pattern_detected_date":       TODAY,
            "pattern_type":                "MASTER_SCORE_" + r["grade"],
            "score_at_detection":          r["total_score"],
            "price_at_detection":          r["price"],
            "market_verdict_at_detection": "PENDING",  # filled by S12
            "checked_1month":              False,
            "checked_3month":              False,
            "checked_6month":              False
        }).execute()
        po_seeded += 1
    except Exception as e:
        po_errors.append(r["symbol"])
        log_error("S10", f"Pattern outcome seed {r['symbol']}", e)

print(f"✅ Pattern outcomes seeded: {po_seeded} rows | {len(po_errors)} errors")

# =============================================================
# SECTION 11: NEWS INTELLIGENCE ENGINE
# =============================================================

section_header("📰", 11, "NEWS INTELLIGENCE ENGINE")

ordered_stocks = [r for r in master_results if r["grade"] in ["A+", "A"]]
print(f"Stocks for news analysis: {len(ordered_stocks)}")

def clean_symbol(s):
    return s.replace(".NS", "").replace(".BO", "")

def fetch_newsapi(symbol):
    try:
        result = newsapi.get_everything(
            q=f'"{clean_symbol(symbol)}" stock India',
            language="en", sort_by="publishedAt", page_size=5,
            from_param=(date.today() - timedelta(days=7)).isoformat()
        )
        if result.get("code") == "rateLimited":
            log_error("S11", "NewsAPI", "Rate limited")
            return []
        return [
            {"headline": a["title"], "source": a["source"]["name"],
             "published": a["publishedAt"][:10]}
            for a in result.get("articles", []) if a.get("title")
        ]
    except Exception as e:
        log_error("S11", f"NewsAPI {symbol}", e)
        return []

def fetch_rss(symbol):
    try:
        q    = clean_symbol(symbol).replace(" ", "+")
        feed = feedparser.parse(
            f"https://news.google.com/rss/search?q={q}+stock+India"
            f"&hl=en-IN&gl=IN&ceid=IN:en")
        return [
            {"headline": e.title, "source": "Google News", "published": TODAY}
            for e in feed.entries[:5]
        ]
    except Exception as e:
        log_error("S11", f"RSS {symbol}", e)
        return []

def fetch_news(symbol):
    articles = fetch_newsapi(symbol)
    if articles: return articles, "NewsAPI"
    return fetch_rss(symbol), "RSS"

SENTIMENT_PROMPT = """You are a financial analyst AI for Indian equity markets.
Analyze these news headlines for stock: {symbol}
Headlines:
{headlines}
Return ONLY a JSON object:
{{"sentiment":<float 0.0-10.0>,"sentiment_label":"<BULLISH|BEARISH|NEUTRAL>",
"risk_events":[{{"event":"<desc>","risk_date":"<YYYY-MM-DD or null>","impact":"<HIGH|MEDIUM|LOW>"}}],
"summary":"<one sentence>"}}
Scoring: 8-10=bullish, 4-6=neutral, 0-2=bearish. Return JSON only."""

def score_sentiment(symbol, articles):
    ht = ("\n".join(f"{i+1}.[{a['source']}] {a['headline']}"
                    for i, a in enumerate(articles))
          if articles else "No recent news found.")
    try:
        r = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": "Return only valid JSON."},
                {"role": "user",
                 "content": SENTIMENT_PROMPT.format(
                     symbol=clean_symbol(symbol), headlines=ht)}
            ],
            temperature=0.1, max_tokens=500
        )
        raw = r.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"): raw = raw[4:]
        return json.loads(raw.strip())
    except Exception as e:
        log_error("S11", f"Groq sentiment {symbol}", e)
        return {"sentiment": 5.0, "sentiment_label": "NEUTRAL",
                "risk_events": [], "summary": "Could not score."}

if ordered_stocks:
    try:
        supabase.table("news_items").delete().eq("published_date", TODAY).execute()
    except Exception as e:
        log_error("S11", "Delete old news", e)
    time.sleep(1)

    news_ok     = 0
    news_errors = 0

    for i, stock in enumerate(ordered_stocks):
        symbol   = stock["symbol"]
        articles, source = fetch_news(symbol)
        scored   = score_sentiment(symbol, articles)
        sv       = float(scored.get("sentiment", 5.0))
        sl_label = str(scored.get("sentiment_label", "NEUTRAL"))
        re_list  = scored.get("risk_events", [])
        summary  = str(scored.get("summary", "")) or None
        ret      = str(re_list[0]["event"]) if re_list else None
        rdv      = re_list[0].get("risk_date") if re_list else None
        if rdv in ("null", "", None): rdv = None

        for article in articles:
            try:
                supabase.table("news_items").insert({
                    "symbol":          str(symbol),
                    "headline":        str(article["headline"])[:500],
                    "source":          str(article["source"]),
                    "published_date":  str(article["published"]),
                    "sentiment":       sl_label,
                    "sentiment_score": int(round(sv)),
                    "risk_event":      ret,
                    "risk_date":       rdv,
                    "summary":         summary
                }).execute()
                news_ok += 1
            except Exception as e:
                news_errors += 1
                log_error("S11", f"Insert news {symbol}", e)

        try:
            supabase.table("signals").update({"news_score": int(round(sv))}) \
                .eq("symbol", symbol).eq("detected_date", TODAY).execute()
        except Exception as e:
            log_error("S11", f"Update news_score {symbol}", e)

        if (i + 1) % 20 == 0:
            print(f"  Progress: {i+1}/{len(ordered_stocks)} | "
                  f"articles:{news_ok} errors:{news_errors}")
        time.sleep(1.5)

    print(f"✅ News Engine: {len(ordered_stocks)} stocks | "
          f"{news_ok} articles saved | {news_errors} errors")
else:
    print("   ℹ️  No A+/A stocks today — skipping news engine")

# =============================================================
# SECTION 12: MARKET CONTEXT ENGINE (REDESIGNED v4)
# Nifty 60% | VIX 20% | FII+DII 20%
# Saves full row to market_data table
# =============================================================

section_header("🌍", 12, "MARKET CONTEXT ENGINE")

def fetch_nifty_full():
    try:
        daily = yf.download("^NSEI", period="1mo", interval="1d",
                            progress=False, auto_adjust=True)
        if daily.empty: raise Exception("Empty daily Nifty response")

        closes      = [float(daily["Close"].iloc[i].iloc[0]) for i in range(len(daily))]
        today_close = closes[-1]
        prev_close  = closes[-2]
        week_ago    = closes[-6] if len(closes) >= 6 else closes[0]
        daily_chg   = ((today_close - prev_close) / prev_close) * 100
        weekly_chg  = ((today_close - week_ago)   / week_ago)   * 100

        # Monthly trend via EMA19 vs EMA55
        monthly = yf.download("^NSEI", period="3y", interval="1mo",
                               progress=False, auto_adjust=True)
        monthly_trend = "SIDEWAYS"
        ema19_val = ema55_val = 0.0
        if not monthly.empty and len(monthly) >= 55:
            mc    = [float(monthly["Close"].iloc[i].iloc[0]) for i in range(len(monthly))]
            ema19 = float(pd.Series(mc).ewm(span=19, adjust=False).mean().iloc[-1])
            ema55 = float(pd.Series(mc).ewm(span=55, adjust=False).mean().iloc[-1])
            ema19_val     = round(ema19, 2)
            ema55_val     = round(ema55, 2)
            monthly_trend = "BULL" if ema19 > ema55 else "BEAR"

        print(f"   Nifty    : {today_close:,.0f} | "
              f"Day:{daily_chg:+.2f}% | Week:{weekly_chg:+.2f}%")
        print(f"   Monthly  : EMA19={ema19_val:,.0f} EMA55={ema55_val:,.0f} → {monthly_trend}")

        return {"close": today_close, "daily_chg": daily_chg,
                "weekly_chg": weekly_chg, "monthly_trend": monthly_trend,
                "ema19": ema19_val, "ema55": ema55_val}

    except Exception as e:
        log_error("S12", "Nifty full fetch", e)
        return {"close": 0, "daily_chg": 0, "weekly_chg": 0,
                "monthly_trend": "SIDEWAYS", "ema19": 0, "ema55": 0}

def score_nifty(data):
    dc = data["daily_chg"]; wc = data["weekly_chg"]; mt = data["monthly_trend"]
    ds = (2.4 if dc >  1.5 else 1.8 if dc >  0.5 else
          1.2 if dc > -0.5 else 0.6 if dc > -1.5 else 0.0)
    ws = (3.6 if wc >  3.0 else 2.7 if wc >  1.5 else
          1.8 if wc >  0.0 else 0.9 if wc > -1.5 else 0.0)
    mb = +0.5 if mt == "BULL" else -0.5
    final = round(min(6.0, max(0.0, ds + ws + mb)), 2)
    print(f"   Nifty Score: Day({dc:+.2f}%→{ds}) + "
          f"Week({wc:+.2f}%→{ws}) + Monthly({mt}→{mb:+}) = {final}/6.0")
    return final, ds, ws, mb

def fetch_vix():
    try:
        vix = yf.download("^INDIAVIX", period="3d", interval="1d",
                          progress=False, auto_adjust=True)
        if vix.empty: raise Exception("Empty VIX response")
        vv  = float(vix["Close"].iloc[-1].iloc[0])
        vs  = (2.0 if vv < 13 else 1.6 if vv < 16 else 1.2 if vv < 20 else
               0.6 if vv < 25 else 0.2 if vv < 30 else 0.0)
        lbl = ("VERY_LOW" if vv < 13 else "LOW" if vv < 16 else "MODERATE"
               if vv < 20 else "HIGH" if vv < 25 else "VERY_HIGH")
        print(f"   VIX Score: {vv:.2f} ({lbl}) = {vs}/2.0")
        return vs, vv, lbl
    except Exception as e:
        log_error("S12", "VIX fetch", e)
        print("   ⚠️  VIX unavailable — using MODERATE fallback (1.2)")
        return 1.2, 20.0, "MODERATE"

def fetch_fii_dii_weekly():
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept":     "application/json, text/plain, */*",
            "Referer":    "https://www.nseindia.com/market-data/fii-dii-activity"
        }
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        time.sleep(2)
        session.get("https://www.nseindia.com/market-data/fii-dii-activity",
                    headers=headers, timeout=10)
        time.sleep(1)
        data    = session.get("https://www.nseindia.com/api/fiidiiTradeReact",
                              headers=headers, timeout=10).json()

        fii_row     = next((r for r in data if "FII" in r.get("category","").upper()), None)
        dii_row     = next((r for r in data if "DII" in r.get("category","").upper()), None)
        today_fii   = float(fii_row["netValue"]) if fii_row else 0.0
        today_dii   = float(dii_row["netValue"]) if dii_row else 0.0
        today_comb  = today_fii + today_dii

        # Store today in agent_memory for weekly cumulative
        try:
            supabase.table("agent_memory").upsert({
                "memory_type": "FII_DII_DAILY",
                "symbol":      "MARKET",
                "context":     TODAY,
                "outcome":     str(round(today_comb, 2)),
                "lesson":      f"FII:{today_fii:.0f} DII:{today_dii:.0f}"
            }).execute()
        except Exception as e:
            log_error("S12", "Save FII/DII to agent_memory", e)

        # Last 5 days cumulative
        hist = supabase.table("agent_memory").select("outcome") \
            .eq("memory_type", "FII_DII_DAILY").eq("symbol", "MARKET") \
            .order("context", desc=True).limit(5).execute()

        weekly_total = (sum(float(r["outcome"]) for r in hist.data)
                        if hist.data and len(hist.data) >= 3 else today_comb)

        fs = (2.0 if weekly_total >  10000 else 1.6 if weekly_total >  5000 else
              1.2 if weekly_total >   2000 else 0.8 if weekly_total >      0 else
              0.4 if weekly_total >  -2000 else 0.2 if weekly_total > -5000 else 0.0)

        print(f"   FII/DII  : Today=₹{today_comb:,.0f}Cr | "
              f"Weekly=₹{weekly_total:,.0f}Cr = {fs}/2.0")
        return fs, today_fii, today_dii, weekly_total

    except Exception as e:
        log_error("S12", "FII/DII fetch", e)
        print("   ⚠️  FII/DII unavailable — using NEUTRAL fallback (1.0/2.0)")
        return 1.0, 0.0, 0.0, 0.0

# Market breadth — how many stocks above EMA19/55
stocks_above_ema19 = sum(
    1 for sym in qualified_symbols
    if sym in ema_data_cache
    and ema_data_cache[sym]["ema19"] > 0
)
stocks_above_ema55 = sum(
    1 for sym in ema_data_cache
    if (ema_data_cache[sym]["ema19"] > ema_data_cache[sym]["ema55"])
)
print(f"   Breadth  : {stocks_above_ema19} stocks above EMA19 | "
      f"{stocks_above_ema55} EMA19>EMA55")

nifty_data                       = fetch_nifty_full()
nifty_score, daily_pts, wk_pts, mo_bonus = score_nifty(nifty_data)
vix_score, vix_val, vix_lbl      = fetch_vix()
fii_score, today_fii, today_dii, weekly_flow = fetch_fii_dii_weekly()

total_market  = round(nifty_score + vix_score + fii_score, 2)
market_verdict = ("STRONG_BULL" if total_market >= 8.0 else
                  "BULL"        if total_market >= 6.0 else
                  "NEUTRAL"     if total_market >= 4.0 else
                  "BEAR"        if total_market >= 2.0 else "STRONG_BEAR")

emoji = ("🟢🟢" if market_verdict == "STRONG_BULL" else
         "🟢"   if market_verdict == "BULL"        else
         "🟡"   if market_verdict == "NEUTRAL"     else
         "🔴"   if market_verdict == "BEAR"        else "🔴🔴")

market = {
    "final_score":   total_market,
    "verdict":       market_verdict,
    "nifty_score":   nifty_score,
    "vix_score":     vix_score,
    "fii_score":     fii_score,
    "weekly_flow":   weekly_flow
}

print(f"\n   {'─'*48}")
print(f"   MARKET SCORE: {total_market}/10  {emoji}  {market_verdict}")
print(f"   Nifty : {nifty_score}/6.0  "
      f"(Day:{nifty_data['daily_chg']:+.2f}% "
      f"Wk:{nifty_data['weekly_chg']:+.2f}% "
      f"Monthly:{nifty_data['monthly_trend']})")
print(f"   VIX   : {vix_score}/2.0  ({vix_val:.1f} {vix_lbl})")
print(f"   FII+DII: {fii_score}/2.0  (Weekly ₹{weekly_flow:,.0f}Cr)")
print(f"   {'─'*48}")

# ── Save to market_data table ─────────────────────────────────
try:
    supabase.table("market_data").upsert({
        "date":               TODAY,
        "nifty_close":        nifty_data["close"],
        "nifty_daily_chg":    round(nifty_data["daily_chg"], 4),
        "nifty_weekly_chg":   round(nifty_data["weekly_chg"], 4),
        "nifty_monthly_trend":nifty_data["monthly_trend"],
        "nifty_ema19":        nifty_data["ema19"],
        "nifty_ema55":        nifty_data["ema55"],
        "vix_value":          vix_val,
        "vix_level":          vix_lbl,
        "fii_net":            today_fii,
        "dii_net":            today_dii,
        "combined_flow":      round(today_fii + today_dii, 2),
        "weekly_cumulative":  round(weekly_flow, 2),
        "market_score":       total_market,
        "market_verdict":     market_verdict,
        "stocks_above_ema19": stocks_above_ema19,
        "stocks_above_ema55": stocks_above_ema55
    }, on_conflict="date").execute()
    print(f"✅ market_data saved for {TODAY}")
except Exception as e:
    log_error("S12", "Save market_data", e)

# ── Update pattern_outcomes with today's market verdict ───────
try:
    supabase.table("pattern_outcomes") \
        .update({"market_verdict_at_detection": market_verdict}) \
        .eq("pattern_detected_date", TODAY).execute()
except Exception as e:
    log_error("S12", "Update pattern_outcomes market verdict", e)

# ── Save initial daily_summary ────────────────────────────────
if len(master_results) > 0:
    try:
        supabase.table("daily_summary").upsert({
            "summary_date":   TODAY,
            "portfolio_size": 0,
            "cash_pct":       100,
            "market_context": market_verdict,
            "agent_thoughts": (
                f"Nifty:{nifty_data['close']:,.0f}|"
                f"Day:{nifty_data['daily_chg']:+.2f}%|"
                f"Week:{nifty_data['weekly_chg']:+.2f}%|"
                f"Monthly:{nifty_data['monthly_trend']}|"
                f"VIX:{vix_val:.1f}({vix_lbl})|"
                f"FII:₹{today_fii:,.0f}Cr|DII:₹{today_dii:,.0f}Cr|"
                f"WeeklyFlow:₹{weekly_flow:,.0f}Cr|"
                f"Score:{total_market}/10({market_verdict})"
            )
        }, on_conflict="summary_date").execute()
    except Exception as e:
        log_error("S12", "Save daily_summary initial", e)

# ── Apply market context score to all signals ─────────────────
mkt_score_int = int(round(total_market))
mkt_ok = 0
for r in master_results:
    try:
        supabase.table("signals") \
            .update({"market_context_score": mkt_score_int}) \
            .eq("symbol", r["symbol"]).eq("detected_date", TODAY).execute()
        mkt_ok += 1
    except Exception as e:
        log_error("S12", f"Update market_context_score {r['symbol']}", e)

print(f"✅ Market context applied to {mkt_ok}/{len(master_results)} stocks")

# =============================================================
# SECTION 13: PORTFOLIO MANAGER — RANKING BASED
# Rank all 500 by effective score daily
# Sell if rank > 40, Buy top ranked available
# Saves trade_outcomes on every exit
# Updates max_profit / max_drawdown on holds
# =============================================================

section_header("💼", 13, "PORTFOLIO MANAGER")

MAX_POSITIONS    = 25
CAPITAL          = 1_000_000   # Rs 10 Lakh
POSITION_SIZE    = 0.04        # 4% per stock
STOP_LOSS_PCT    = 0.07        # 7%
TARGET_PCT       = 0.20        # 20%
RANK_SELL_CUTOFF = 40
MIN_SCORE_ENTRY  = 55          # A grade

# ── Build full ranked list ─────────────────────────────────────
market_bonus = {
    "STRONG_BULL": +3, "BULL": +2, "NEUTRAL": 0,
    "BEAR": -3, "STRONG_BEAR": -6
}.get(market_verdict, 0)

print(f"Market bonus/penalty : {market_bonus:+d} pts ({market_verdict})")

ranked_all = []
for r in master_results:
    eff = (r["total_score"] or 0) + market_bonus
    ranked_all.append({
        "symbol":          r["symbol"],
        "name":            r["name"],
        "sector":          r["sector"],
        "price":           r["price"],
        "raw_score":       r["total_score"] or 0,
        "effective_score": eff,
        "grade":           r["grade"]
    })

ranked_all.sort(key=lambda x: x["effective_score"], reverse=True)
for i, r in enumerate(ranked_all):
    r["rank"] = i + 1

rank_map = {r["symbol"]: r for r in ranked_all}

print(f"Total ranked : {len(ranked_all)} stocks")
print(f"\n   TOP 10:")
for r in ranked_all[:10]:
    print(f"   #{r['rank']:>3}  {r['symbol']:<18} "
          f"Raw:{r['raw_score']:>5.1f}  Eff:{r['effective_score']:>5.1f}  {r['grade']}")

# ── Update rank + effective_score in signals ──────────────────
rank_ok = 0
for r in ranked_all:
    try:
        supabase.table("signals") \
            .update({"rank_today":      r["rank"],
                     "effective_score": r["effective_score"]}) \
            .eq("symbol", r["symbol"]).eq("detected_date", TODAY).execute()
        rank_ok += 1
    except Exception as e:
        log_error("S13", f"Update rank {r['symbol']}", e)

print(f"✅ Ranks saved to signals: {rank_ok}/{len(ranked_all)}")

# ── Load current portfolio ─────────────────────────────────────
try:
    port = supabase.table("portfolio").select("*").eq("status", "ACTIVE").execute().data
except Exception as e:
    log_error("S13", "Fetch portfolio", e)
    port = []

print(f"\nCurrent holdings: {len(port)}/{MAX_POSITIONS}")

exits   = []
entries = []

# ── EXITS ──────────────────────────────────────────────────────
print("\n🔴 CHECKING EXITS...")

for pos in port:
    sym  = pos["symbol"]
    ep   = float(pos["entry_price"])
    sl   = float(pos["stop_loss"])
    tgt  = float(pos["target"])
    qty  = int(pos["quantity"])

    try:
        h  = yf.Ticker(sym).history(period="2d", interval="1d", auto_adjust=True)
        cp = round(float(h["Close"].iloc[-1]), 2) if not h.empty else ep
    except Exception as e:
        log_error("S13", f"Fetch price for exit check {sym}", e)
        cp = ep

    pct        = ((cp - ep) / ep) * 100
    stock_rank = rank_map.get(sym, {}).get("rank", 9999)
    exit_reason = None

    if   cp <= sl:                      exit_reason = f"STOP_LOSS_HIT (₹{cp}≤₹{sl})"
    elif cp >= tgt:                     exit_reason = f"TARGET_HIT (₹{cp}≥₹{tgt})"
    elif stock_rank > RANK_SELL_CUTOFF: exit_reason = f"RANK_DROPPED (#{stock_rank}>{RANK_SELL_CUTOFF})"

    if exit_reason:
        realised = round((cp - ep) * qty, 2)
        try:
            supabase.table("portfolio").update({
                "status":             "EXITED",
                "exit_date":          TODAY,
                "exit_price":         cp,
                "exit_reason":        exit_reason,
                "realised_pnl":       realised,
                "current_price":      cp,
                "current_value":      round(cp * qty, 2),
                "unrealised_pnl":     0,
                "unrealised_pnl_pct": 0,
                "updated_at":         datetime.now().isoformat()
            }).eq("symbol", sym).eq("status", "ACTIVE").execute()

            exits.append({"symbol": sym, "pnl_pct": round(pct, 2),
                          "pnl_rs": realised, "reason": exit_reason,
                          "rank": stock_rank})
            print(f"   EXIT: {sym:<18} #{stock_rank:>4}  "
                  f"{pct:+.1f}%  ₹{realised:+,.0f}  {exit_reason[:40]}")

            # ── Save to trade_outcomes ─────────────────────────
            try:
                entry_date_str = str(pos.get("entry_date", ""))
                exit_score     = rank_map.get(sym, {}).get("raw_score", 0)
                exit_rank      = stock_rank
                sb             = score_breakdown.get(sym, {})

                # Calculate days held
                try:
                    days_held = (date.today() -
                                 date.fromisoformat(entry_date_str)).days
                except Exception:
                    days_held = 0

                outcome = ("WIN"    if realised > 0 else
                           "LOSS"   if realised < 0 else "BREAKEVEN")

                supabase.table("trade_outcomes").insert({
                    "symbol":               sym,
                    "entry_date":           entry_date_str,
                    "exit_date":            TODAY,
                    "days_held":            days_held,
                    "entry_score":          float(pos.get("entry_score", 0) or 0),
                    "entry_rank":           int(pos.get("entry_rank", 0) or 0),
                    "entry_grade":          str(pos.get("entry_grade", "")),
                    "entry_market_verdict": str(pos.get("notes", "")).split("|Mkt:")[-1].split("|")[0]
                                            if "|Mkt:" in str(pos.get("notes","")) else "",
                    "entry_market_score":   float(pos.get("entry_market_score", 0) or 0),
                    "entry_ema_score":      float(sb.get("ema_score", 0)),
                    "entry_pattern_score":  float(sb.get("pattern_score", 0)),
                    "entry_vpa_score":      float(sb.get("vpa_score", 0)),
                    "entry_candle_score":   float(sb.get("candle_score", 0)),
                    "entry_attraos_score":  float(sb.get("attraos_score", 0)),
                    "entry_news_score":     float(sb.get("news_score", 0) if "news_score" in sb else 0),
                    "exit_reason":          exit_reason,
                    "exit_score":           float(exit_score),
                    "exit_rank":            exit_rank,
                    "pnl_pct":              round(pct, 4),
                    "pnl_rs":               realised,
                    "sector":               rank_map.get(sym, {}).get("sector", ""),
                    "outcome":              outcome
                }).execute()
                print(f"         → trade_outcomes saved ({outcome} ₹{realised:+,.0f})")
            except Exception as e:
                log_error("S13", f"Save trade_outcomes {sym}", e)

        except Exception as e:
            log_error("S13", f"Process exit {sym}", e)

    else:
        # Hold — update price + max profit/drawdown tracking
        try:
            curr_max_profit = float(pos.get("max_profit_pct", 0) or 0)
            curr_max_dd     = float(pos.get("max_drawdown_pct", 0) or 0)
            new_max_profit  = max(curr_max_profit, pct)
            new_max_dd      = min(curr_max_dd, pct)  # most negative seen

            supabase.table("portfolio").update({
                "current_price":      cp,
                "current_value":      round(cp * qty, 2),
                "unrealised_pnl":     round((cp - ep) * qty, 2),
                "unrealised_pnl_pct": round(pct, 2),
                "max_profit_pct":     round(new_max_profit, 2),
                "max_drawdown_pct":   round(new_max_dd, 2),
                "updated_at":         datetime.now().isoformat()
            }).eq("symbol", sym).eq("status", "ACTIVE").execute()
        except Exception as e:
            log_error("S13", f"Update hold {sym}", e)

print(f"   Exits today: {len(exits)}")

# ── ENTRIES ────────────────────────────────────────────────────
exited_syms = {e["symbol"] for e in exits}
held_syms   = {p["symbol"] for p in port if p["symbol"] not in exited_syms}
slots       = MAX_POSITIONS - len(held_syms)

print(f"\n🟢 CHECKING ENTRIES ({slots} slots available)...")

candidates = [
    r for r in ranked_all
    if r["symbol"] not in held_syms
    and r["effective_score"] >= MIN_SCORE_ENTRY
    and r["grade"] in ["A+", "A"]
]
print(f"   Candidates (eff≥{MIN_SCORE_ENTRY}, A+/A, not held): {len(candidates)}")

for stock in candidates[:slots]:
    sym   = stock["symbol"]
    rank  = stock["rank"]
    score = stock["effective_score"]
    grade = stock["grade"]
    price = float(stock.get("price") or 0)

    try:
        h = yf.Ticker(sym).history(period="2d", interval="1d", auto_adjust=True)
        if not h.empty:
            price = round(float(h["Close"].iloc[-1]), 2)
    except Exception as e:
        log_error("S13", f"Fetch entry price {sym}", e)

    if price <= 0:
        print(f"   ⚠️  SKIP {sym} — price is zero/unavailable")
        continue

    qty = max(1, int((CAPITAL * POSITION_SIZE) / price))
    inv = round(qty * price, 2)
    sl  = round(price * (1 - STOP_LOSS_PCT), 2)
    tgt = round(price * (1 + TARGET_PCT), 2)

    try:
        supabase.table("portfolio").insert({
            "symbol":            sym,
            "entry_date":        TODAY,
            "entry_price":       price,
            "quantity":          qty,
            "invested_amount":   inv,
            "current_price":     price,
            "current_value":     inv,
            "unrealised_pnl":    0,
            "unrealised_pnl_pct":0,
            "stop_loss":         sl,
            "target":            tgt,
            "entry_grade":       grade,
            "entry_score":       score,
            "entry_rank":        rank,
            "entry_market_score":total_market,
            "max_profit_pct":    0,
            "max_drawdown_pct":  0,
            "status":            "ACTIVE",
            "notes":             (f"Rank:{rank}|Raw:{stock['raw_score']}|"
                                  f"Eff:{score}|Mkt:{market_verdict}({market_bonus:+d})"),
            "updated_at":        datetime.now().isoformat()
        }).execute()
        entries.append({"symbol": sym, "rank": rank, "price": price,
                        "qty": qty, "sl": sl, "tgt": tgt, "score": score})
        print(f"   BUY: #{rank:>3}  {sym:<18} ₹{price:>8.2f}  "
              f"qty:{qty}  SL:₹{sl}  T:₹{tgt}  [{grade}:{score:.0f}]")
    except Exception as e:
        log_error("S13", f"Insert portfolio entry {sym}", e)

print(f"   Entries today: {len(entries)}")

# ── Portfolio summary ──────────────────────────────────────────
try:
    fp  = supabase.table("portfolio").select("*").eq("status","ACTIVE").execute().data
except Exception as e:
    log_error("S13", "Final portfolio fetch", e)
    fp = []

ti  = sum(float(p.get("invested_amount") or 0) for p in fp)
tv  = sum(float(p.get("current_value")   or 0) for p in fp)
tu  = sum(float(p.get("unrealised_pnl")  or 0) for p in fp)
cr  = CAPITAL - ti
cpc = round((cr / CAPITAL) * 100, 1)
pp  = round((tu / ti * 100) if ti > 0 else 0, 2)

print(f"\n{'='*62}")
print(f"  NIFTYMIND AI — PORTFOLIO SUMMARY  {TODAY}")
print(f"{'='*62}")
print(f"  Holdings  : {len(fp)}/{MAX_POSITIONS}  (Entries:{len(entries)} Exits:{len(exits)})")
print(f"  Invested  : ₹{ti:>12,.0f}")
print(f"  Value     : ₹{tv:>12,.0f}")
print(f"  P&L       : ₹{tu:>+12,.0f}  ({pp:+.2f}%)")
print(f"  Cash      : ₹{cr:>12,.0f}  ({cpc}%)")
print(f"{'─'*62}")
print(f"  MARKET    : {total_market}/10  {emoji}  {market_verdict}")
print(f"  Nifty     : {nifty_score}/6.0  "
      f"(Day:{nifty_data['daily_chg']:+.2f}% "
      f"Wk:{nifty_data['weekly_chg']:+.2f}% "
      f"Monthly:{nifty_data['monthly_trend']})")
print(f"  VIX       : {vix_score}/2.0  ({vix_val:.1f} {vix_lbl})")
print(f"  FII+DII   : {fii_score}/2.0  (Weekly ₹{weekly_flow:,.0f}Cr)")
print(f"{'─'*62}")

if fp:
    print(f"  {'SYMBOL':<18} {'ENTRY':>8} {'NOW':>8} "
          f"{'P&L%':>7}  {'MAX+':>6} {'MAX-':>6}  RANK")
    print(f"  {'─'*58}")
    for p in sorted(fp, key=lambda x: float(x.get("unrealised_pnl_pct") or 0),
                    reverse=True):
        pct   = float(p.get("unrealised_pnl_pct") or 0)
        mp    = float(p.get("max_profit_pct")   or 0)
        md    = float(p.get("max_drawdown_pct") or 0)
        rank_ = rank_map.get(p["symbol"], {}).get("rank", "?")
        print(f"  {p['symbol']:<18} ₹{float(p['entry_price']):>7.2f} "
              f"₹{str(p.get('current_price','?')):>7}  "
              f"{pct:>+6.1f}%  {mp:>+5.1f}% {md:>+5.1f}%  #{rank_}")

print(f"{'='*62}")

# ── Update daily_summary with final data ──────────────────────
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
            "market_context": market_verdict,
            "full_summary": (
                f"Date:{TODAY}|Holdings:{len(fp)}/{MAX_POSITIONS}|"
                f"P&L:₹{tu:+,.0f}({pp:+.2f}%)|Cash:₹{cr:,.0f}({cpc}%)|"
                f"Entries:{len(entries)} Exits:{len(exits)}|"
                f"Market:{total_market}/10 {market_verdict}|"
                f"Nifty:{nifty_score}/6"
                f"(Day:{nifty_data['daily_chg']:+.2f}% "
                f"Wk:{nifty_data['weekly_chg']:+.2f}% "
                f"{nifty_data['monthly_trend']})|"
                f"VIX:{vix_score}/2({vix_val:.1f} {vix_lbl})|"
                f"FII+DII:{fii_score}/2(₹{weekly_flow:,.0f}Cr weekly)|"
                f"Breadth:{stocks_above_ema19} above EMA19"
            )
        }, on_conflict="summary_date").execute()
        print(f"\n✅ daily_summary updated")
    except Exception as e:
        log_error("S13", "Update daily_summary final", e)

# =============================================================
# FINAL REPORT — ALL ERRORS SURFACED HERE
# =============================================================

print(f"\n{'='*62}")
print(f"  NIFTYMIND AI — RUN COMPLETE  {TODAY}")
print(f"{'='*62}")
print(f"  Stocks processed : {updated}/{len(all_symbols)}")
print(f"  Qualified stocks : {len(qualified_symbols)}")
print(f"  A+/A grade       : {aplus + a_cnt}")
print(f"  Portfolio        : {len(fp)}/{MAX_POSITIONS}")
print(f"  Entries today    : {len(entries)}")
print(f"  Exits today      : {len(exits)}")
print(f"  Market score     : {total_market}/10  {market_verdict}")
print(f"  Pattern seeds    : {po_seeded} (forward tracking)")
print(f"{'─'*62}")

if ERROR_LOG:
    print(f"\n  ⚠️  ERRORS ENCOUNTERED: {len(ERROR_LOG)}")
    print(f"  {'─'*58}")
    for i, err in enumerate(ERROR_LOG, 1):
        print(f"  {i:>2}. [{err['section']}] {err['context']}")
        print(f"      → {err['error'][:80]}")
    print(f"\n  ACTION NEEDED: Review errors above before next run")
else:
    print(f"\n  ✅ ZERO ERRORS — clean run!")

print(f"{'='*62}")
print(f"  Next auto-run: Tomorrow 5:00 PM IST")
print(f"{'='*62}")
