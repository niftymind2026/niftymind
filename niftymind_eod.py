# =============================================================
# NIFTYMIND AI — DAILY EOD ENGINE
# Runs every day at 3:45 PM IST (10:15 AM UTC)
# Phase 1-5 combined: Data + Engines + News + Market Context
# =============================================================

import os
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

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
newsapi  = NewsApiClient(api_key=NEWSAPI_KEY)
groq     = Groq(api_key=GROQ_KEY)

TODAY      = str(date.today())
NOW        = datetime.today()
THIS_MONTH = NOW.replace(day=1).date()

print("=" * 60)
print("  NIFTYMIND AI — DAILY EOD ENGINE")
print(f"  Date: {TODAY}")
print("=" * 60)

# =============================================================
# SECTION 2: DAILY EOD DATA UPDATE
# Fetches today's candle for all 500 stocks
# Recalculates current month candle
# =============================================================

print("\n📥 SECTION 2: DAILY EOD DATA UPDATE")
print("-" * 40)

stocks_response = supabase.table("stocks") \
    .select("symbol") \
    .eq("is_active", True) \
    .execute()

all_symbols = [row["symbol"] for row in stocks_response.data]
print(f"Stocks to update: {len(all_symbols)}")

updated = 0
failed  = 0

for i, symbol in enumerate(all_symbols):
    try:
        ticker = yf.Ticker(symbol)
        hist   = ticker.history(period="5d", interval="1d", auto_adjust=True)

        if hist.empty:
            failed += 1
            continue

        latest      = hist.iloc[-1]
        latest_date = hist.index[-1].date()

        supabase.table("daily_candles").upsert([{
            "symbol": symbol,
            "date":   str(latest_date),
            "open":   round(float(latest["Open"]), 2),
            "high":   round(float(latest["High"]), 2),
            "low":    round(float(latest["Low"]), 2),
            "close":  round(float(latest["Close"]), 2),
            "volume": int(latest["Volume"])
        }], on_conflict="symbol,date").execute()

        # Recalculate current month candle from daily data
        monthly_days = supabase.table("daily_candles") \
            .select("open,high,low,close,volume,date") \
            .eq("symbol", symbol) \
            .gte("date", str(THIS_MONTH)) \
            .order("date", desc=False) \
            .execute()

        if not monthly_days.data:
            failed += 1
            continue

        days         = monthly_days.data
        month_open   = float(days[0]["open"])
        month_high   = max(float(d["high"]) for d in days)
        month_low    = min(float(d["low"])  for d in days)
        month_close  = float(days[-1]["close"])
        month_volume = sum(int(d["volume"]) for d in days)

        existing = supabase.table("monthly_candles") \
            .select("id") \
            .eq("symbol", symbol) \
            .eq("date", str(THIS_MONTH)) \
            .execute()

        if existing.data:
            supabase.table("monthly_candles").update({
                "open": round(month_open, 2), "high": round(month_high, 2),
                "low":  round(month_low, 2),  "close": round(month_close, 2),
                "volume": month_volume, "is_complete": False
            }).eq("symbol", symbol).eq("date", str(THIS_MONTH)).execute()
        else:
            supabase.table("monthly_candles").insert({
                "symbol": symbol, "date": str(THIS_MONTH),
                "open": round(month_open, 2), "high": round(month_high, 2),
                "low":  round(month_low, 2),  "close": round(month_close, 2),
                "volume": month_volume, "is_complete": False
            }).execute()

        updated += 1
        if (i + 1) % 100 == 0:
            print(f"  Updated: {i+1}/{len(all_symbols)}")
        time.sleep(0.2)

    except Exception:
        failed += 1

print(f"✅ EOD Update: {updated} updated, {failed} failed")

# =============================================================
# SECTION 3: CORPORATE ACTION CHECKER
# Flags stocks with >10% price discrepancy
# =============================================================

print("\n🏢 SECTION 3: CORPORATE ACTION CHECK")
print("-" * 40)

flagged = []
checked = 0

for symbol in all_symbols:
    try:
        stored = supabase.table("monthly_candles") \
            .select("close, date") \
            .eq("symbol", symbol) \
            .eq("is_complete", True) \
            .order("date", desc=True) \
            .limit(1) \
            .execute()

        if not stored.data:
            continue

        stored_close = float(stored.data[0]["close"])
        ticker = yf.Ticker(symbol)
        hist   = ticker.history(period="3mo", interval="1mo", auto_adjust=True)

        if hist.empty or len(hist) < 2:
            continue

        yahoo_close = float(hist["Close"].iloc[-2])
        diff_pct    = abs(stored_close - yahoo_close) / stored_close * 100

        if diff_pct > 10:
            flagged.append({"symbol": symbol, "diff_pct": round(diff_pct, 1)})
            supabase.table("corporate_actions").insert({
                "symbol": symbol, "detected_date": TODAY,
                "action_type": "AUTO_DETECTED",
                "stored_price": stored_close, "yahoo_price": yahoo_close,
                "difference_pct": round(diff_pct, 1),
                "status": "PENDING", "notes": f"Price diff {diff_pct:.1f}%"
            }).execute()
            supabase.table("stocks").update({
                "status": "PENDING_REVIEW", "is_active": False
            }).eq("symbol", symbol).execute()

        checked += 1
        time.sleep(0.3)

    except Exception:
        pass

print(f"✅ Corporate check: {checked} checked, {len(flagged)} flagged")
if flagged:
    for item in flagged:
        print(f"   ⚠️  {item['symbol']} — {item['diff_pct']}% diff → SIDELINED")

# =============================================================
# SECTION 4: ENGINE 1 — EMA CALCULATOR
# EMA 19 + EMA 55 structure qualification
# =============================================================

print("\n📊 SECTION 4: ENGINE 1 — EMA CALCULATOR")
print("-" * 40)

def calculate_ema(prices, period):
    return pd.Series(prices).ewm(span=period, adjust=False).mean()

stocks_response = supabase.table("stocks") \
    .select("symbol, name, sector") \
    .eq("data_quality", "SUFFICIENT") \
    .eq("is_active", True) \
    .execute()

stocks    = stocks_response.data
qualified = []
errors    = []

for i, stock in enumerate(stocks):
    try:
        symbol = stock["symbol"]
        candles = supabase.table("monthly_candles") \
            .select("date,open,high,low,close,volume") \
            .eq("symbol", symbol) \
            .order("date", desc=False) \
            .execute()

        if not candles.data or len(candles.data) < 55:
            continue

        closes       = [float(c["close"]) for c in candles.data]
        ema19_series = calculate_ema(closes, 19)
        ema55_series = calculate_ema(closes, 55)

        current_close = closes[-1]
        current_ema19 = round(ema19_series.iloc[-1], 2)
        current_ema55 = round(ema55_series.iloc[-1], 2)
        prev_ema19    = round(ema19_series.iloc[-2], 2)

        is_qualified = (
            current_ema19 > current_ema55 and
            current_close > current_ema19 and
            current_ema19 > prev_ema19
        )

        if is_qualified:
            qualified.append(symbol)

        supabase.table("signals").upsert({
            "symbol": symbol, "detected_date": TODAY,
            "pattern_type": "EMA_STRUCTURE",
            "direction": "BULLISH" if is_qualified else "BEARISH",
            "price": current_close,
            "technical_score": 25 if is_qualified else 0,
            "grade": "QUALIFIED" if is_qualified else "NOT_QUALIFIED"
        }).execute()

        if (i + 1) % 50 == 0:
            print(f"  Processed: {i+1}/{len(stocks)}")

    except Exception as e:
        errors.append(symbol)

print(f"✅ EMA Engine: {len(qualified)} qualified, {len(errors)} errors")

# =============================================================
# SECTION 5: ENGINE 2 — PATTERN DETECTION
# Swing points, trendlines, channels, breakouts
# =============================================================

print("\n📈 SECTION 5: ENGINE 2 — PATTERN DETECTION")
print("-" * 40)

def detect_swing_points(highs, lows, closes, lookback=3):
    swing_highs, swing_lows = [], []
    n = len(highs)
    for i in range(lookback, n - lookback):
        if all(highs[i] > highs[i-j] for j in range(1, lookback+1)) and \
           all(highs[i] > highs[i+j] for j in range(1, lookback+1)):
            swing_highs.append({"index": i, "price": highs[i]})
        if all(lows[i] < lows[i-j] for j in range(1, lookback+1)) and \
           all(lows[i] < lows[i+j] for j in range(1, lookback+1)):
            swing_lows.append({"index": i, "price": lows[i]})
    return swing_highs, swing_lows

def detect_trend(swing_points, min_points=2):
    if len(swing_points) < min_points:
        return "INSUFFICIENT"
    prices = [p["price"] for p in swing_points[-min_points:]]
    if all(prices[i] > prices[i-1] for i in range(1, len(prices))):
        return "UPTREND"
    elif all(prices[i] < prices[i-1] for i in range(1, len(prices))):
        return "DOWNTREND"
    return "SIDEWAYS"

def detect_channel(swing_highs, swing_lows, closes):
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return "NO_CHANNEL"
    ht = detect_trend(swing_highs)
    lt = detect_trend(swing_lows)
    if ht == "UPTREND"   and lt == "UPTREND":   return "ASCENDING_CHANNEL"
    if ht == "DOWNTREND" and lt == "DOWNTREND": return "DESCENDING_CHANNEL"
    if ht == "SIDEWAYS"  and lt == "SIDEWAYS":  return "HORIZONTAL_CHANNEL"
    return "NO_CHANNEL"

def detect_breakout(closes, highs, lows, swing_highs, swing_lows, ema19, ema55):
    signals = []
    current, prev = closes[-1], closes[-2]
    if swing_highs:
        recent_high = swing_highs[-1]["price"]
        if current > recent_high and prev <= recent_high:
            signals.append("BREAKOUT_ABOVE_HIGH")
    if swing_lows:
        recent_low = swing_lows[-1]["price"]
        if current < recent_low and prev >= recent_low:
            signals.append("BREAKDOWN_BELOW_LOW")
    if current > ema19[-1] and prev < ema19[-2]:
        signals.append("RECLAIMED_EMA19")
    return signals

qualified_symbols = [
    r["symbol"] for r in
    supabase.table("signals")
    .select("symbol")
    .eq("pattern_type", "EMA_STRUCTURE")
    .eq("grade", "QUALIFIED")
    .eq("detected_date", TODAY)
    .execute().data
]

pattern_results = []
errors          = []

for i, symbol in enumerate(qualified_symbols):
    try:
        candles = supabase.table("monthly_candles") \
            .select("date,open,high,low,close,volume") \
            .eq("symbol", symbol) \
            .order("date", desc=False) \
            .execute()

        if not candles.data or len(candles.data) < 20:
            continue

        closes  = [float(c["close"])  for c in candles.data]
        highs   = [float(c["high"])   for c in candles.data]
        lows    = [float(c["low"])    for c in candles.data]

        ema19 = list(calculate_ema(closes, 19))
        ema55 = list(calculate_ema(closes, 55))

        swing_highs, swing_lows = detect_swing_points(highs, lows, closes)
        channel  = detect_channel(swing_highs, swing_lows, closes)
        low_trend = detect_trend(swing_lows)
        breakout_signals = detect_breakout(closes, highs, lows, swing_highs, swing_lows, ema19, ema55)

        pattern_score = 0
        if low_trend == "UPTREND":           pattern_score += 8
        if detect_trend(swing_highs) == "UPTREND": pattern_score += 5
        if channel == "ASCENDING_CHANNEL":   pattern_score += 7
        elif channel == "HORIZONTAL_CHANNEL":pattern_score += 3
        if "BREAKOUT_ABOVE_HIGH" in breakout_signals: pattern_score += 10
        if "RECLAIMED_EMA19"     in breakout_signals: pattern_score += 5
        if "BREAKDOWN_BELOW_LOW" in breakout_signals: pattern_score -= 10
        pattern_score = min(25, max(0, pattern_score))

        supabase.table("signals").upsert({
            "symbol": symbol, "detected_date": TODAY,
            "pattern_type": channel if channel != "NO_CHANNEL" else "TREND_" + low_trend,
            "direction": "BULLISH" if pattern_score > 10 else "NEUTRAL",
            "price": closes[-1], "trendline_touches": len(swing_lows),
            "technical_score": pattern_score, "grade": "QUALIFIED"
        }).execute()

        pattern_results.append({"symbol": symbol, "pattern_score": pattern_score})

        if (i + 1) % 25 == 0:
            print(f"  Processed: {i+1}/{len(qualified_symbols)}")

    except Exception as e:
        errors.append(symbol)

print(f"✅ Pattern Engine: {len(pattern_results)} analyzed, {len(errors)} errors")

# =============================================================
# SECTION 6: ENGINE 3 — VPA ANALYSIS
# Volume Price Analysis
# =============================================================

print("\n📊 SECTION 6: ENGINE 3 — VPA ANALYSIS")
print("-" * 40)

def analyze_vpa(closes, highs, lows, opens, volumes):
    signals, vpa_score = [], 0
    n = len(closes)
    if n < 12:
        return signals, vpa_score, 1.0

    vol_series = pd.Series(volumes)
    avg_vol_10 = vol_series.rolling(10).mean()

    curr_close = closes[-1]; curr_open  = opens[-1]
    curr_high  = highs[-1];  curr_low   = lows[-1]
    curr_vol   = volumes[-1]; curr_avg  = avg_vol_10.iloc[-1]

    vol_ratio   = curr_vol / curr_avg if curr_avg > 0 else 1
    body        = abs(curr_close - curr_open)
    total_range = curr_high - curr_low
    if total_range == 0:
        return signals, vpa_score, round(vol_ratio, 2)

    body_pct   = body / total_range
    upper_wick = curr_high - max(curr_close, curr_open)
    lower_wick = min(curr_close, curr_open) - curr_low
    wick_ratio = 1 - body_pct
    is_bullish = curr_close > curr_open

    if vol_ratio > 2.0 and wick_ratio > 0.6:
        price_pos = (curr_close - min(closes[-12:])) / (max(closes[-12:]) - min(closes[-12:]) + 0.001)
        if price_pos < 0.3:
            signals.append("BUYING_CLIMAX"); vpa_score += 6
        else:
            signals.append("SELLING_CLIMAX"); vpa_score -= 4
    elif is_bullish and vol_ratio < 0.7:
        signals.append("NO_DEMAND"); vpa_score -= 2
    elif not is_bullish and vol_ratio < 0.7:
        signals.append("NO_SUPPLY"); vpa_score += 4
    elif is_bullish and vol_ratio > 1.5:
        signals.append("VOLUME_CONFIRMED_UP"); vpa_score += 5
    elif not is_bullish and vol_ratio > 1.5:
        signals.append("HIGH_VOL_SELLING"); vpa_score -= 3

    if n >= 4:
        last3_closes = closes[-4:-1]; last3_highs = highs[-4:-1]
        last3_lows   = lows[-4:-1];   last3_vols  = volumes[-4:-1]
        last3_avg_vol = avg_vol_10.iloc[-4:-1].mean()
        price_range_pct = (max(last3_highs) - min(last3_lows)) / closes[-4] * 100
        avg_vol_3m = np.mean(last3_vols)
        price_stable = last3_closes[-1] >= last3_closes[0]
        if price_range_pct < 8 and avg_vol_3m > last3_avg_vol and price_stable:
            signals.append("ACCUMULATION"); vpa_score += 6

    if n >= 6:
        last6_closes = closes[-6:]; last6_vols = volumes[-6:]
        up_vols   = [last6_vols[i] for i in range(1,6) if last6_closes[i] > last6_closes[i-1]]
        down_vols = [last6_vols[i] for i in range(1,6) if last6_closes[i] < last6_closes[i-1]]
        if up_vols and down_vols:
            if np.mean(up_vols) > np.mean(down_vols) * 1.2:
                signals.append("HEALTHY_UPTREND"); vpa_score += 4
            elif np.mean(down_vols) > np.mean(up_vols) * 1.2:
                signals.append("WEAKENING_UPTREND"); vpa_score -= 2

    return signals, min(15, max(0, vpa_score)), round(vol_ratio, 2)

vpa_results = []
errors      = []

for i, symbol in enumerate(qualified_symbols):
    try:
        candles = supabase.table("monthly_candles") \
            .select("date,open,high,low,close,volume") \
            .eq("symbol", symbol) \
            .order("date", desc=False) \
            .execute()

        if not candles.data or len(candles.data) < 12:
            continue

        closes  = [float(c["close"])  for c in candles.data]
        opens   = [float(c["open"])   for c in candles.data]
        highs   = [float(c["high"])   for c in candles.data]
        lows    = [float(c["low"])    for c in candles.data]
        volumes = [float(c["volume"]) for c in candles.data]

        vpa_signals, vpa_score, vol_ratio = analyze_vpa(closes, highs, lows, opens, volumes)

        supabase.table("signals").upsert({
            "symbol": symbol, "detected_date": TODAY,
            "pattern_type": "VPA",
            "direction": "BULLISH" if vpa_score >= 8 else "NEUTRAL",
            "price": closes[-1], "vpa_score": vpa_score, "grade": "QUALIFIED"
        }).execute()

        vpa_results.append({"symbol": symbol, "vpa_score": vpa_score})

        if (i + 1) % 25 == 0:
            print(f"  Processed: {i+1}/{len(qualified_symbols)}")

    except Exception as e:
        errors.append(symbol)

print(f"✅ VPA Engine: {len(vpa_results)} analyzed, {len(errors)} errors")

# =============================================================
# SECTION 7: ENGINE 4 — CANDLESTICK SCANNER
# 20 patterns, 6-month backward scan
# =============================================================

print("\n🕯️  SECTION 7: ENGINE 4 — CANDLESTICK SCANNER")
print("-" * 40)

def detect_single_candle(o, h, l, c):
    body = abs(c - o); total_range = h - l
    if total_range < 0.001: return "NONE"
    body_pct = body / total_range
    upper_wick = h - max(o, c); lower_wick = min(o, c) - l
    is_bullish = c > o
    if body_pct < 0.1:
        if lower_wick > upper_wick * 2: return "DRAGONFLY_DOJI"
        if upper_wick > lower_wick * 2: return "GRAVESTONE_DOJI"
        return "DOJI"
    if lower_wick > body * 2 and upper_wick < body * 0.3:
        return "HAMMER" if is_bullish else "HANGING_MAN"
    if upper_wick > body * 2 and lower_wick < body * 0.3:
        return "SHOOTING_STAR" if not is_bullish else "INVERTED_HAMMER"
    if body_pct > 0.85:
        return "BULL_MARUBOZU" if is_bullish else "BEAR_MARUBOZU"
    return "NORMAL"

def detect_two_candle(prev, curr):
    po, ph, pl, pc = prev; co, ch, cl, cc = curr
    prev_bull = pc > po; curr_bull = cc > co
    if not prev_bull and curr_bull and co <= pc and cc >= po: return "BULLISH_ENGULFING"
    if prev_bull and not curr_bull and co >= pc and cc <= po: return "BEARISH_ENGULFING"
    if not prev_bull and curr_bull and co < pl and cc > (po + pc) / 2: return "PIERCING_LINE"
    if prev_bull and not curr_bull and co > ph and cc < (po + pc) / 2: return "DARK_CLOUD_COVER"
    return "NONE"

def detect_three_candle(c1, c2, c3):
    o1, h1, l1, cl1 = c1; o2, h2, l2, cl2 = c2; o3, h3, l3, cl3 = c3
    body2 = abs(cl2 - o2); range2 = h2 - l2
    body_pct2 = body2 / range2 if range2 > 0 else 0
    if cl1 < o1 and body_pct2 < 0.3 and cl3 > o3 and cl3 > (o1 + cl1) / 2: return "MORNING_STAR"
    if cl1 > o1 and body_pct2 < 0.3 and cl3 < o3 and cl3 < (o1 + cl1) / 2: return "EVENING_STAR"
    if cl1 > o1 and cl2 > o2 and cl3 > o3 and cl2 > cl1 and cl3 > cl2: return "THREE_WHITE_SOLDIERS"
    if cl1 < o1 and cl2 < o2 and cl3 < o3 and cl2 < cl1 and cl3 < cl2: return "THREE_BLACK_CROWS"
    return "NONE"

PATTERN_SCORES = {
    "HAMMER": 3, "DRAGONFLY_DOJI": 2, "BULL_MARUBOZU": 2, "DOJI": 1,
    "SHOOTING_STAR": -3, "GRAVESTONE_DOJI": -2, "HANGING_MAN": -2, "BEAR_MARUBOZU": -2,
    "BULLISH_ENGULFING": 4, "PIERCING_LINE": 3, "BEARISH_ENGULFING": -4, "DARK_CLOUD_COVER": -3,
    "MORNING_STAR": 5, "THREE_WHITE_SOLDIERS": 4, "EVENING_STAR": -5, "THREE_BLACK_CROWS": -4,
}

def backward_candle_scan(candle_data, lookback=6):
    n = len(candle_data)
    if n < lookback + 3: return [], 0, "NONE"
    window = candle_data[-lookback:]
    total_score = 0

    for candle in window:
        p = detect_single_candle(candle["open"], candle["high"], candle["low"], candle["close"])
        if p not in ["NONE", "NORMAL"]:
            total_score += PATTERN_SCORES.get(p, 0)

    for i in range(1, len(window)):
        prev = (window[i-1]["open"], window[i-1]["high"], window[i-1]["low"], window[i-1]["close"])
        curr = (window[i]["open"],   window[i]["high"],   window[i]["low"],   window[i]["close"])
        p = detect_two_candle(prev, curr)
        if p != "NONE": total_score += PATTERN_SCORES.get(p, 0)

    for i in range(2, len(window)):
        c1 = (window[i-2]["open"], window[i-2]["high"], window[i-2]["low"], window[i-2]["close"])
        c2 = (window[i-1]["open"], window[i-1]["high"], window[i-1]["low"], window[i-1]["close"])
        c3 = (window[i]["open"],   window[i]["high"],   window[i]["low"],   window[i]["close"])
        p = detect_three_candle(c1, c2, c3)
        if p != "NONE": total_score += PATTERN_SCORES.get(p, 0)

    candle_score = min(15, max(0, total_score))
    conviction = "HIGH" if candle_score >= 10 else "MEDIUM" if candle_score >= 6 else "LOW" if candle_score >= 3 else "NONE"
    return [], candle_score, conviction

candle_results = []
errors         = []

for i, symbol in enumerate(qualified_symbols):
    try:
        candles = supabase.table("monthly_candles") \
            .select("date,open,high,low,close") \
            .eq("symbol", symbol) \
            .order("date", desc=False) \
            .execute()

        if not candles.data or len(candles.data) < 9:
            continue

        _, score, conviction = backward_candle_scan(candles.data, lookback=6)

        supabase.table("signals").upsert({
            "symbol": symbol, "detected_date": TODAY,
            "pattern_type": "CANDLE_SCAN",
            "direction": "BULLISH" if score >= 6 else "NEUTRAL",
            "price": candles.data[-1]["close"],
            "candle_score": score, "grade": conviction
        }).execute()

        candle_results.append({"symbol": symbol, "score": score})

        if (i + 1) % 25 == 0:
            print(f"  Processed: {i+1}/{len(qualified_symbols)}")

    except Exception as e:
        errors.append(symbol)

print(f"✅ Candle Engine: {len(candle_results)} scanned, {len(errors)} errors")

# =============================================================
# SECTION 8: ENGINE 5 — ATTRAOS (CHAOS THEORY)
# Phase space reconstruction + attractor zone detection
# =============================================================

print("\n🌀 SECTION 8: ENGINE 5 — ATTRAOS")
print("-" * 40)

def reconstruct_phase_space(prices, d=3, tau=1):
    prices = np.array(prices)
    n = len(prices)
    m = n - (d - 1) * tau
    if m <= 0: return None
    phase_space = np.zeros((m, d))
    for i in range(m):
        for j in range(d):
            phase_space[i, j] = prices[i + j * tau]
    return phase_space

def detect_attractor_zone(phase_space, current_point):
    if phase_space is None or len(phase_space) < 10:
        return "INSUFFICIENT_DATA", 0.5
    centroid   = np.mean(phase_space, axis=0)
    distances  = [np.linalg.norm(p - centroid) for p in phase_space]
    avg_dist   = np.mean(distances)
    std_dist   = np.std(distances)
    recent_dists = [np.linalg.norm(p - centroid) for p in phase_space[-3:]]
    dist_trend = recent_dists[-1] - recent_dists[0] if len(recent_dists) >= 2 else 0
    if std_dist / (avg_dist + 0.001) > 0.5:
        return "CHAOS", 0.3
    elif dist_trend > std_dist * 0.3:
        return "EXPANSION", min(0.85, 0.6 + dist_trend / (avg_dist + 0.001))
    elif dist_trend < -std_dist * 0.3:
        return "CONTRACTION", 0.4
    else:
        return "TRANSITION", 0.5

def calculate_direction_probability(phase_space, prices, d=3):
    if phase_space is None or len(phase_space) < 15: return 0.5
    prices    = np.array(prices)
    n_points  = len(phase_space)
    current   = phase_space[-1]
    distances = sorted([(np.linalg.norm(phase_space[i] - current), i) for i in range(n_points - 1)])
    neighbors = [idx for _, idx in distances if idx < n_points - 3][:5]
    if not neighbors: return 0.5
    up_count = sum(1 for idx in neighbors if idx + d < len(prices) and prices[idx + d] > prices[idx + d - 1])
    return round(up_count / len(neighbors), 2)

def calculate_chaos_score(prices):
    prices  = np.array(prices)
    n = len(prices)
    if n < 20: return 0.5
    returns = np.diff(prices) / prices[:-1]
    signs   = np.sign(returns)
    consistency = np.mean([1 if signs[i] == signs[i-1] else 0 for i in range(1, len(signs))])
    rolling_vol = pd.Series(returns).rolling(6).std()
    vol_of_vol  = rolling_vol.std() / (rolling_vol.mean() + 0.001)
    return round((1 - consistency) * 0.5 + min(1, vol_of_vol) * 0.5, 2)

def attraos_score(zone, direction_prob, chaos):
    score  = {"EXPANSION": 10, "TRANSITION": 5, "CONTRACTION": 2, "CHAOS": 0, "INSUFFICIENT_DATA": 0}.get(zone, 0)
    score += 10 if direction_prob >= 0.75 else 7 if direction_prob >= 0.60 else 4 if direction_prob >= 0.50 else 0
    score += 5 if chaos < 0.3 else 3 if chaos < 0.5 else 1 if chaos < 0.7 else 0
    return min(25, score)

attraos_results = []
errors          = []

for i, symbol in enumerate(qualified_symbols):
    try:
        candles = supabase.table("monthly_candles") \
            .select("date, close") \
            .eq("symbol", symbol) \
            .order("date", desc=False) \
            .execute()

        if not candles.data or len(candles.data) < 24:
            continue

        prices      = [float(c["close"]) for c in candles.data]
        phase_space = reconstruct_phase_space(prices, d=3, tau=1)

        if phase_space is None or len(phase_space) < 10:
            continue

        zone, confidence   = detect_attractor_zone(phase_space, phase_space[-1])
        direction_prob     = calculate_direction_probability(phase_space, prices)
        chaos              = calculate_chaos_score(prices)
        a_score            = attraos_score(zone, direction_prob, chaos)

        supabase.table("signals").upsert({
            "symbol": symbol, "detected_date": TODAY,
            "pattern_type": "ATTRAOS",
            "direction": "BULLISH" if a_score >= 15 else "NEUTRAL",
            "price": prices[-1], "attraos_score": a_score, "grade": zone
        }).execute()

        attraos_results.append({"symbol": symbol, "attraos_score": a_score, "zone": zone})

        if (i + 1) % 25 == 0:
            print(f"  Processed: {i+1}/{len(qualified_symbols)}")

    except Exception as e:
        errors.append(symbol)

print(f"✅ Attraos Engine: {len(attraos_results)} analyzed, {len(errors)} errors")

# =============================================================
# SECTION 9: MASTER SCORING ENGINE
# Combines all 5 engines into final score (0-80 pts)
# =============================================================

print("\n🏆 SECTION 9: MASTER SCORING ENGINE")
print("-" * 40)

master_results = []
errors         = []

for symbol in qualified_symbols:
    try:
        stock_info = supabase.table("stocks").select("name, sector").eq("symbol", symbol).execute()
        if not stock_info.data: continue

        name   = stock_info.data[0]["name"]
        sector = stock_info.data[0]["sector"]

        def get_score(pattern_type, field):
            r = supabase.table("signals").select(field) \
                .eq("symbol", symbol).eq("pattern_type", pattern_type) \
                .eq("detected_date", TODAY).execute()
            return r.data[0].get(field, 0) or 0 if r.data else 0

        ema_sig = supabase.table("signals").select("technical_score, price") \
            .eq("symbol", symbol).eq("pattern_type", "EMA_STRUCTURE") \
            .eq("detected_date", TODAY).execute()

        ema_score     = ema_sig.data[0].get("technical_score", 0) or 0 if ema_sig.data else 0
        current_price = ema_sig.data[0].get("price", 0) or 0 if ema_sig.data else 0

        pat_sig = supabase.table("signals").select("technical_score") \
            .eq("symbol", symbol).eq("detected_date", TODAY) \
            .neq("pattern_type", "EMA_STRUCTURE").neq("pattern_type", "VPA") \
            .neq("pattern_type", "CANDLE_SCAN").neq("pattern_type", "ATTRAOS") \
            .neq("pattern_type", "MASTER_SCORE").execute()
        pattern_score = pat_sig.data[0].get("technical_score", 0) or 0 if pat_sig.data else 0

        vpa_score    = get_score("VPA", "vpa_score")
        candle_score = get_score("CANDLE_SCAN", "candle_score")
        a_score      = get_score("ATTRAOS", "attraos_score")

        total = ema_score + pattern_score + vpa_score + candle_score + a_score
        grade = "A+" if total >= 70 else "A" if total >= 55 else "B" if total >= 40 else "C" if total >= 25 else "SKIP"

        master_results.append({
            "symbol": symbol, "name": name, "sector": sector,
            "price": current_price, "total_score": total, "grade": grade
        })

        existing = supabase.table("signals").select("id") \
            .eq("symbol", symbol).eq("detected_date", TODAY) \
            .eq("pattern_type", "MASTER_SCORE").execute()

        row_data = {
            "technical_score": ema_score, "vpa_score": vpa_score,
            "candle_score": candle_score, "attraos_score": a_score,
            "total_score": total, "grade": grade, "price": current_price,
            "direction": "BULLISH" if total >= 55 else "NEUTRAL"
        }

        if existing.data:
            supabase.table("signals").update(row_data) \
                .eq("symbol", symbol).eq("detected_date", TODAY) \
                .eq("pattern_type", "MASTER_SCORE").execute()
        else:
            supabase.table("signals").insert({
                **row_data, "symbol": symbol,
                "detected_date": TODAY, "pattern_type": "MASTER_SCORE"
            }).execute()

    except Exception as e:
        errors.append(f"{symbol}: {e}")

master_results.sort(key=lambda x: x["total_score"], reverse=True)
aplus = sum(1 for r in master_results if r["grade"] == "A+")
a     = sum(1 for r in master_results if r["grade"] == "A")

print(f"✅ Master Scoring: {len(master_results)} scored, {aplus} A+, {a} A grade")
print(f"\n   TOP 5 STOCKS:")
for idx, r in enumerate(master_results[:5], 1):
    print(f"   {idx}. {r['symbol']:<18} {r['total_score']}/80  {r['grade']}")

# =============================================================
# SECTION 10: NEWS INTELLIGENCE ENGINE (PHASE 4)
# Groq AI sentiment scoring for A+ and A stocks
# =============================================================

print("\n📰 SECTION 10: NEWS INTELLIGENCE ENGINE")
print("-" * 40)

# Get A+ and A stocks from today's master scores
resp = supabase.table("signals") \
    .select("symbol, total_score, grade") \
    .eq("detected_date", TODAY) \
    .in_("grade", ["A+", "A"]) \
    .order("total_score", desc=True) \
    .execute()

ordered_stocks = resp.data
aplus_news     = [s for s in ordered_stocks if s["grade"] == "A+"]
agrade_news    = [s for s in ordered_stocks if s["grade"] == "A"]
print(f"Stocks for news: {len(aplus_news)} A+ + {len(agrade_news)} A = {len(ordered_stocks)} total")

def clean_symbol(symbol):
    return symbol.replace(".NS", "").replace(".BO", "")

def fetch_newsapi(symbol):
    try:
        result = newsapi.get_everything(
            q=f'"{clean_symbol(symbol)}" stock India',
            language="en", sort_by="publishedAt", page_size=5,
            from_param=(date.today() - timedelta(days=7)).isoformat()
        )
        if result.get("code") == "rateLimited": return []
        return [
            {"headline": a["title"], "source": a["source"]["name"], "published": a["publishedAt"][:10]}
            for a in result.get("articles", []) if a.get("title")
        ]
    except Exception:
        return []

def fetch_rss(symbol):
    try:
        query = clean_symbol(symbol).replace(" ", "+")
        url   = f"https://news.google.com/rss/search?q={query}+stock+India&hl=en-IN&gl=IN&ceid=IN:en"
        feed  = feedparser.parse(url)
        return [
            {"headline": e.title, "source": "Google News", "published": TODAY}
            for e in feed.entries[:5]
        ]
    except Exception:
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
{{"sentiment": <float 0.0-10.0>, "sentiment_label": "<BULLISH|BEARISH|NEUTRAL>",
  "risk_events": [{{"event": "<desc>", "risk_date": "<YYYY-MM-DD or null>", "impact": "<HIGH|MEDIUM|LOW>"}}],
  "summary": "<one sentence>"}}
Scoring: 8-10=bullish, 4-6=neutral, 0-2=bearish. Return JSON only."""

def score_sentiment(symbol, articles):
    headlines_text = "\n".join(f"{i+1}. [{a['source']}] {a['headline']}" for i, a in enumerate(articles)) \
                     if articles else "No recent news found."
    try:
        response = groq.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": "You are a financial analyst. Return only valid JSON."},
                {"role": "user",   "content": SENTIMENT_PROMPT.format(symbol=clean_symbol(symbol), headlines=headlines_text)}
            ],
            temperature=0.1, max_tokens=500,
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"): raw = raw[4:]
        return json.loads(raw.strip())
    except Exception:
        return {"sentiment": 5.0, "sentiment_label": "NEUTRAL", "risk_events": [], "summary": "Could not score."}

# Clear today's news before inserting fresh
supabase.table("news_items").delete().eq("published_date", TODAY).execute()
time.sleep(1)

news_errors = 0
for i, stock in enumerate(ordered_stocks):
    symbol = stock["symbol"]
    grade  = stock["grade"]

    articles, source = fetch_news(symbol)
    scored           = score_sentiment(symbol, articles)
    sentiment_val    = float(scored.get("sentiment", 5.0))
    sentiment_label  = str(scored.get("sentiment_label", "NEUTRAL"))
    risk_events      = scored.get("risk_events", [])
    summary          = str(scored.get("summary", "")) or None
    risk_event_text  = str(risk_events[0]["event"]) if risk_events else None
    risk_date_val    = risk_events[0].get("risk_date") if risk_events else None
    if risk_date_val in ("null", "", None): risk_date_val = None

    for article in articles:
        try:
            supabase.table("news_items").insert({
                "symbol":          str(symbol),
                "headline":        str(article["headline"])[:500],
                "source":          str(article["source"]),
                "published_date":  str(article["published"]),
                "sentiment":       sentiment_label,
                "sentiment_score": int(round(sentiment_val)),
                "risk_event":      risk_event_text,
                "risk_date":       risk_date_val,
                "summary":         summary,
            }).execute()
        except Exception:
            news_errors += 1

    try:
        supabase.table("signals") \
            .update({"news_score": int(round(sentiment_val))}) \
            .eq("symbol", symbol).eq("detected_date", TODAY).execute()
    except Exception:
        pass

    if (i + 1) % 20 == 0:
        print(f"  News: {i+1}/{len(ordered_stocks)} stocks processed")

    time.sleep(1.5)

print(f"✅ News Engine: {len(ordered_stocks)} stocks scored, {news_errors} errors")

# =============================================================
# SECTION 11: MARKET CONTEXT ENGINE (PHASE 5)
# Nifty50 + VIX + FII/DII → market_context_score (0-10)
# =============================================================

print("\n🌍 SECTION 11: MARKET CONTEXT ENGINE")
print("-" * 40)

def fetch_nifty_data():
    try:
        nifty = yf.download("^NSEI", period="5d", interval="1d", progress=False, auto_adjust=True)
        if nifty.empty: raise Exception("Empty")
        latest_close  = float(nifty["Close"].iloc[-1].iloc[0])
        prev_close    = float(nifty["Close"].iloc[-2].iloc[0])
        week_ago      = float(nifty["Close"].iloc[0].iloc[0])
        daily_change  = ((latest_close - prev_close) / prev_close) * 100
        weekly_change = ((latest_close - week_ago)   / week_ago)   * 100
        trend = "BULLISH" if weekly_change > 1.5 else "BEARISH" if weekly_change < -1.5 else "SIDEWAYS"
        print(f"   Nifty 50 : {latest_close:,.0f} | Day: {daily_change:+.2f}% | Week: {weekly_change:+.2f}% | {trend}")
        return {"nifty_close": latest_close, "nifty_change": daily_change, "nifty_trend": trend, "weekly_change": weekly_change}
    except Exception as e:
        print(f"   ⚠️ Nifty error: {e}")
        return {"nifty_close": 0, "nifty_change": 0, "nifty_trend": "SIDEWAYS", "weekly_change": 0}

def fetch_vix_data():
    try:
        vix = yf.download("^INDIAVIX", period="3d", interval="1d", progress=False, auto_adjust=True)
        if vix.empty: raise Exception("Empty")
        vix_value = float(vix["Close"].iloc[-1].iloc[0])
        vix_level = "VERY_LOW" if vix_value < 13 else "LOW" if vix_value < 16 else \
                    "MODERATE" if vix_value < 20 else "HIGH" if vix_value < 25 else "VERY_HIGH"
        print(f"   India VIX: {vix_value:.2f} | {vix_level}")
        return {"vix_value": vix_value, "vix_level": vix_level}
    except Exception as e:
        print(f"   ⚠️ VIX error: {e}")
        return {"vix_value": 20, "vix_level": "MODERATE"}

def fetch_fii_dii():
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.nseindia.com/market-data/fii-dii-activity"
        }
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        time.sleep(2)
        session.get("https://www.nseindia.com/market-data/fii-dii-activity", headers=headers, timeout=10)
        time.sleep(1)
        data    = session.get("https://www.nseindia.com/api/fiidiiTradeReact", headers=headers, timeout=10).json()
        fii_row = next((r for r in data if "FII" in r.get("category", "").upper()), None)
        dii_row = next((r for r in data if "DII" in r.get("category", "").upper()), None)
        fii_net = float(fii_row["netValue"]) if fii_row else 0.0
        dii_net = float(dii_row["netValue"]) if dii_row else 0.0
        combined = fii_net + dii_net
        fii_sentiment = "STRONG_BUYING" if fii_net > 500 else "BUYING" if fii_net > 0 else \
                        "SELLING" if fii_net > -500 else "STRONG_SELLING"
        print(f"   FII Net  : ₹{fii_net:,.0f} Cr | {fii_sentiment}")
        print(f"   DII Net  : ₹{dii_net:,.0f} Cr | Combined: ₹{combined:,.0f} Cr")
        return {"fii_net": fii_net, "dii_net": dii_net, "combined_flow": combined, "fii_sentiment": fii_sentiment}
    except Exception as e:
        print(f"   ⚠️ FII/DII error: {e}")
        return {"fii_net": 0, "dii_net": 0, "combined_flow": 0, "fii_sentiment": "NEUTRAL"}

def calculate_market_score(nifty, vix, fii_dii):
    weekly   = nifty.get("weekly_change", 0)
    vix_val  = vix.get("vix_value", 20)
    combined = fii_dii.get("combined_flow", 0)

    nifty_score = 10 if weekly > 3 else 8 if weekly > 1.5 else 6 if weekly > 0 else \
                  4 if weekly > -1.5 else 2 if weekly > -3 else 0
    vix_score   = 10 if vix_val < 13 else 8 if vix_val < 16 else 6 if vix_val < 20 else \
                  3 if vix_val < 25 else 0
    flow_score  = 10 if combined > 2000 else 8 if combined > 500 else 6 if combined > 0 else \
                  4 if combined > -500 else 2 if combined > -2000 else 0

    final_score = int(round((nifty_score + vix_score + flow_score) / 3))
    verdict     = "STRONG_BULL" if final_score >= 8 else "BULL" if final_score >= 6 else \
                  "NEUTRAL" if final_score >= 4 else "BEAR" if final_score >= 2 else "STRONG_BEAR"

    print(f"   Nifty score: {nifty_score}/10 | VIX score: {vix_score}/10 | Flow score: {flow_score}/10")
    print(f"   FINAL: {final_score}/10 → {verdict}")
    return {"final_score": final_score, "verdict": verdict}

nifty_data   = fetch_nifty_data()
vix_data     = fetch_vix_data()
fii_dii_data = fetch_fii_dii()
market       = calculate_market_score(nifty_data, vix_data, fii_dii_data)

# Save to daily_summary
try:
    summary_row = {
        "summary_date":   TODAY,
        "portfolio_size": 0,
        "cash_pct":       0,
        "market_context": market["verdict"],
        "agent_thoughts": (
            f"Nifty: {nifty_data['nifty_trend']} ({nifty_data['nifty_change']:+.2f}%) | "
            f"VIX: {vix_data['vix_value']:.1f} ({vix_data['vix_level']}) | "
            f"FII: ₹{fii_dii_data['fii_net']:,.0f}Cr | "
            f"DII: ₹{fii_dii_data['dii_net']:,.0f}Cr | "
            f"Market Score: {market['final_score']}/10 ({market['verdict']})"
        )
    }
    supabase.table("daily_summary").upsert(summary_row, on_conflict="summary_date").execute()
    print("✅ daily_summary saved!")
except Exception as e:
    print(f"   ⚠️ daily_summary error: {e}")

# Update market_context_score in signals
ctx_success = 0
for stock in ordered_stocks:
    try:
        supabase.table("signals") \
            .update({"market_context_score": market["final_score"]}) \
            .eq("symbol", stock["symbol"]).eq("detected_date", TODAY).execute()
        ctx_success += 1
    except Exception:
        pass

print(f"✅ Market context: {ctx_success}/{len(ordered_stocks)} stocks updated")

# =============================================================
# SECTION 12: FINAL SUMMARY REPORT
# =============================================================

print("\n" + "=" * 60)
print("  NIFTYMIND AI — DAILY RUN COMPLETE")
print(f"  Date: {TODAY}")
print("=" * 60)
print(f"\n📊 MASTER SCORES (80pts base):")
print(f"   A+ Grade (70+): {sum(1 for r in master_results if r['grade'] == 'A+')} stocks")
print(f"   A  Grade (55+): {sum(1 for r in master_results if r['grade'] == 'A')} stocks")
print(f"   B  Grade (40+): {sum(1 for r in master_results if r['grade'] == 'B')} stocks")
print(f"\n🌍 MARKET CONTEXT: {market['verdict']} ({market['final_score']}/10)")
print(f"   Nifty: {nifty_data['nifty_trend']} | VIX: {vix_data['vix_value']:.1f} | FII: ₹{fii_dii_data['fii_net']:,.0f}Cr")
print(f"\n🏆 TOP 10 STOCKS TODAY:")
print(f"   {'#':<3} {'Symbol':<18} {'Score':>6} {'Grade':>5}")
print(f"   {'-'*35}")
for idx, r in enumerate(master_results[:10], 1):
    print(f"   {idx:<3} {r['symbol']:<18} {r['total_score']:>6} {r['grade']:>5}")
print(f"\n✅ All engines complete! Data saved to Supabase.")
print(f"   Next run: Tomorrow at 3:45 PM IST")
