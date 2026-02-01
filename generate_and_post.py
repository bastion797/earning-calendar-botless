import os
import io
import math
import json
import requests
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta, MO
from PIL import Image, ImageDraw, ImageFont

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
FMP_API_KEY = os.environ.get("FMP_API_KEY", "").strip()
TE_API_KEY = os.environ.get("TE_API_KEY", "").strip()

MIN_MCAP = 100_000_000  # $100M

# ---------- Date helpers ----------
def next_monday_to_friday(today_utc: date):
    # Compute next week's Monday–Friday from "run day"
    # If run is Sunday night ET, it's Monday UTC, so this will return the SAME Monday.
    monday = today_utc + relativedelta(weekday=MO(0))
    friday = monday + timedelta(days=4)
    return monday, friday

# ---------- Data fetching (replace these as you upgrade sources) ----------
def fetch_earnings_fmp(start: date, end: date):
    if not FMP_API_KEY:
        print("No FMP_API_KEY found; returning empty earnings.")
        return []

    url = "https://financialmodelingprep.com/stable/earnings-calendar"
    params = {
        "from": start.isoformat(),
        "to": end.isoformat(),
        "apikey": FMP_API_KEY,
    }

    try:
        r = requests.get(url, params=params, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        data = r.json()
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        text = (getattr(e.response, "text", "") or "")[:300]
        print(f"FMP earnings request failed: HTTP {status} | {text}")
        return []
    except Exception as e:
        print(f"FMP earnings request failed: {e}")
        return []

    out = []
    for row in data:
        sym = row.get("symbol")
        d = row.get("date")
        if not sym or not d:
            continue

        out.append({
            "symbol": sym,
            "date": d[:10],
            "time": "",  # stable earnings-calendar doesn't provide BMO/AMC
            "name": "",  # optional: can add later if you want company names
        })

    print(f"Fetched {len(out)} earnings rows from FMP stable endpoint")
    return out

def fetch_market_caps_fmp(symbols):
    """
    Pull market caps for only the symbols that have earnings this week.
    This keeps API calls manageable even on free tiers.
    """
    if not FMP_API_KEY or not symbols:
        return {}

    mcap = {}
    # Try batch quote endpoint first (cheaper); fallback to per-symbol profile if needed.
    # Batch quote: /api/v3/quote/AAPL,MSFT,...
    # Caveat: URL length limit. Chunk symbols.
    chunk_size = 50
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i+chunk_size]
        url = f"https://financialmodelingprep.com/api/v3/quote/{','.join(chunk)}"
        params = {"apikey": FMP_API_KEY}
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        rows = r.json()
        for row in rows:
            sym = row.get("symbol")
            cap = row.get("marketCap")

        cap_val = None
        if isinstance(cap, (int, float)):
            cap_val = int(cap)
        elif isinstance(cap, str):
            try:
                cap_val = int(float(cap.replace(",", "")))
            except ValueError:
                cap_val = None

        if sym and cap_val is not None:
            mcap[sym] = cap_val
    return mcap

def fetch_macro_events(start: date, end: date):
    """
    Macro events: use whatever source you like later.
    For now:
    - if TE_API_KEY exists, you can wire TradingEconomics calendar
    - otherwise returns empty
    """
    if not TE_API_KEY:
        return []

    # TradingEconomics example endpoint (you may need to adjust based on your plan/region filters)
    # Docs: https://tradingeconomics.com/api/calendar.aspx
    url = "https://api.tradingeconomics.com/calendar"
    params = {
        "c": TE_API_KEY,
        "d1": start.isoformat(),
        "d2": end.isoformat(),
        "format": "json",
        # You can add "country": "united states" but TE uses specific filters; adjust as needed.
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    events = []
    for row in data:
        # Fields vary; common ones: Date, Event, Country, Importance
        dt = row.get("Date") or row.get("date") or ""
        title = row.get("Event") or row.get("event") or ""
        country = row.get("Country") or row.get("country") or ""
        importance = row.get("Importance") or row.get("importance") or ""
        if not dt or not title:
            continue
        # dt might be full timestamp; normalize to YYYY-MM-DD
        day = dt[:10]
        events.append({"date": day, "title": title, "country": country, "importance": str(importance)})
    # Optional: filter to US-only + higher importance here
    return events

# ---------- Build the week structure ----------
def build_week(monday: date, friday: date, earnings, market_caps, macro_events):
    days = {}
    d = monday
    while d <= friday:
        days[d.isoformat()] = {"earnings": [], "macro": []}
        d += timedelta(days=1)

    # attach macro
    for ev in macro_events:
        day = ev.get("date")
        if day in days:
            days[day]["macro"].append(ev)

    # attach earnings (filtered by mcap)
    for e in earnings:
        sym = e["symbol"]
        day = e["date"]
        cap = market_caps.get(sym)
        if day not in days:
            continue
        if cap is None or cap < MIN_MCAP:
            continue
        days[day]["earnings"].append({
            "symbol": sym,
            "name": e.get("name", ""),
            "time": e.get("time", ""),
            "marketCap": cap,
        })

    # sort earnings by market cap desc
    for day in days:
        days[day]["earnings"].sort(key=lambda x: x["marketCap"], reverse=True)

    return days

# ---------- Render PNG ----------
def fmt_mcap(n: int):
    if n >= 1_000_000_000_000:
        return f"{n/1_000_000_000_000:.1f}T"
    if n >= 1_000_000_000:
        return f"{n/1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n/1_000_000:.0f}M"
    return str(n)

def render_calendar_png(week_days, monday: date, friday: date):
    W, H = 1800, 1100
    margin = 40
    header_h = 90
    cols = 5
    col_w = (W - 2*margin) // cols
    row_h = H - margin - header_h

    img = Image.new("RGB", (W, H), (18, 18, 18))
    draw = ImageDraw.Draw(img)

    # Fonts (DejaVu is present on ubuntu runners)
    title_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 42)
    day_font   = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 24)
    text_font  = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 20)
    small_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 18)

    title = f"Weekly Earnings + Macro | {monday.isoformat()} → {friday.isoformat()} | mcap ≥ $100M"
    draw.text((margin, 20), title, font=title_font, fill=(235, 235, 235))

    # Build ordered weekdays
    dates = [monday + timedelta(days=i) for i in range(5)]

    # Box + content
    for i, d in enumerate(dates):
        x0 = margin + i * col_w
        y0 = header_h
        x1 = x0 + col_w - 10
        y1 = H - margin

        # outline
        draw.rectangle([x0, y0, x1, y1], outline=(70, 70, 70), width=2)

        day_key = d.isoformat()
        pretty = d.strftime("%a %b %d")
        draw.text((x0 + 12, y0 + 10), pretty, font=day_font, fill=(220, 220, 220))

        y = y0 + 46

        # Macro
        macros = week_days[day_key]["macro"][:5]
        if macros:
            draw.text((x0 + 12, y), "MACRO", font=small_font, fill=(180, 180, 180))
            y += 24
            for ev in macros:
                t = ev["title"]
                # truncate
                if len(t) > 28:
                    t = t[:27] + "…"
                draw.text((x0 + 18, y), f"• {t}", font=small_font, fill=(200, 200, 200))
                y += 22
            y += 8

        # Earnings
        earns = week_days[day_key]["earnings"]
        draw.text((x0 + 12, y), f"EARNINGS ({len(earns)})", font=small_font, fill=(180, 180, 180))
        y += 24

        max_lines = 18
        shown = earns[:max_lines]
        for e in shown:
            sym = e["symbol"]
            cap = fmt_mcap(e["marketCap"])
            tm  = e.get("time", "").upper()
            tm = tm if tm else "—"
            line = f"{sym:<6} {tm:<4} {cap:>5}"
            draw.text((x0 + 18, y), line, font=text_font, fill=(235, 235, 235))
            y += 24

        extra = len(earns) - len(shown)
        if extra > 0:
            draw.text((x0 + 18, y), f"+{extra} more", font=text_font, fill=(160, 160, 160))

    return img

# ---------- Post to Discord ----------
def post_png_to_discord(img: Image.Image, content: str):
    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("Missing DISCORD_WEBHOOK_URL secret.")

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    buf.seek(0)

    files = {"file": ("earnings_calendar.png", buf, "image/png")}
    data = {"content": content}

    r = requests.post(DISCORD_WEBHOOK_URL, data=data, files=files, timeout=30)
    r.raise_for_status()

def main():
    today_utc = datetime.utcnow().date()
    monday, friday = next_monday_to_friday(today_utc)

    earnings = fetch_earnings_fmp(monday, friday)
    symbols = sorted(list({e["symbol"] for e in earnings}))
    market_caps = fetch_market_caps_fmp(symbols)
    macro = fetch_macro_events(monday, friday)

    # ===== DEBUG OUTPUT (SAFE) =====
    print(f"Date range: {monday} -> {friday}")
    print(f"Earnings rows fetched: {len(earnings)}")
    print(f"Unique symbols: {len(symbols)}")
    print(f"Market caps fetched: {len(market_caps)}")
    print(f"Macro events fetched: {len(macro)}")
    # ===============================

    week_days = build_week(monday, friday, earnings, market_caps, macro)
    img = render_calendar_png(week_days, monday, friday)

    msg = (
        f"**Weekly Calendar** ({monday} → {friday})\n"
        "Earnings: NYSE+NASDAQ, mcap ≥ $100M\n"
        "Macro events included where available"
    )

    post_png_to_discord(img, msg)

if __name__ == "__main__":
    main()