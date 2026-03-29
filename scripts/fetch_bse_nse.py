#!/usr/bin/env python3
"""
BSE/NSE data fetcher for GitHub Pages.

OUTPUT STRUCTURE:
  bse_items  — official BSE filings, actions, calendar (authoritative, never cut)
  news_items — targeted Google News queries about NSE/BSE (supplementary)

ET Markets general RSS and LiveMint general RSS are intentionally excluded:
they return market news (FII flows, analyst notes) not corporate filings.
"""

import json, re, xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path

OUT     = Path(__file__).parent.parent / "data" / "bse_nse.json"
OUT.parent.mkdir(exist_ok=True)
NOW_UTC = datetime.now(timezone.utc)
# Stale cutoff for NEWS items (7 days). BSE official items ignore this.
NEWS_CUTOFF_MS = int((NOW_UTC - timedelta(days=7)).timestamp() * 1000)

bse_items:  list[dict] = []   # authoritative BSE filings — never cut
news_items: list[dict] = []   # targeted news queries — supplementary
bse_seen:   set[str]   = set()
news_seen:  set[str]   = set()
log:        list[str]  = []


# ── helpers ──────────────────────────────────────────────────────────────────

def slug(s: str) -> str:
    return re.sub(r'[^a-z0-9]', '', (s or '').lower())[:60]


def parse_dt(s: str):
    if not s:
        return None
    s = re.sub(r'\s+', ' ', s.strip())
    s = re.sub(r'\s+[+-]\d{4}$', '', s).strip()
    s = re.sub(r'\s+GMT$', '', s).strip()
    fmts = [
        '%m/%d/%Y %I:%M:%S %p',   # "3/28/2026 2:05:11 PM"  ← BSE actual
        '%m/%d/%Y %I:%M %p',
        '%m/%d/%Y %H:%M:%S',
        '%m/%d/%Y',
        '%d %b %Y %I:%M:%S %p',   # "28 Mar 2026 2:05:11 PM"
        '%d %b %Y %I:%M %p',
        '%d %b %Y %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d %H:%M:%S',
        '%a, %d %b %Y %H:%M:%S',  # RSS pubDate
        '%d/%m/%Y %H:%M:%S',
        '%d/%m/%Y',
        '%d %b %Y',
        '%Y-%m-%d',
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def to_ms(dt) -> int:
    if dt is None:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def classify(title: str) -> str:
    t = (title or '').lower()
    if re.search(r'dividend|buyback|bonus|rights issue|stock split|face value', t):
        return 'dividend'
    if re.search(r'\bq[1-4]\b|quarter|result|profit|revenue|earnings|pat\b|ebitda|financial result', t):
        return 'results'
    if re.search(r'board meeting|board of directors|agm|egm|annual general|extraordinary general', t):
        return 'board'
    if re.search(r'insider|promoter|stake|pledge|bulk deal|block deal|trading window', t):
        return 'insider'
    return 'filing'


def strip_html(s: str) -> str:
    return re.sub(r'<[^>]+>', '', s or '').strip()


def add_bse(title: str, link: str, source: str, dt, ann_type=None):
    """Add to the authoritative BSE section. No stale filter, no size cap."""
    title = (title or '').strip()
    if not title or len(title) < 8:
        return
    k = slug(title)
    if k in bse_seen:
        return
    bse_seen.add(k)
    ts = to_ms(dt)
    bse_items.append({
        "title":  title,
        "link":   link or "#",
        "source": source,
        "ts":     ts,
        "type":   ann_type or classify(title),
    })


def add_news(title: str, link: str, source: str, dt, ann_type=None):
    """Add to supplementary news. Drops items older than 7 days."""
    title = (title or '').strip()
    if not title or len(title) < 8:
        return
    k = slug(title)
    if k in news_seen or k in bse_seen:  # deduplicate across both lists
        return
    ts = to_ms(dt)
    if ts > 0 and ts < NEWS_CUTOFF_MS:
        return  # stale
    news_seen.add(k)
    news_items.append({
        "title":  title,
        "link":   link or "#",
        "source": source,
        "ts":     ts,
        "type":   ann_type or classify(title),
    })


# ── SOURCE 1: BSE official Python package ────────────────────────────────────
def fetch_bse_official():
    try:
        from bse import BSE  # type: ignore
        print("BSE: initialising...")
        with BSE(download_folder='/tmp/bse_dl') as bse:

            # Pages 1 and 2 of today's announcements
            for page in [1, 2]:
                data = bse.announcements(page_no=page)
                rows = data.get('Table') or []
                print(f"  BSE page {page}: {len(rows)} rows")

                # Log first row field names + sample date for diagnostics
                if rows and page == 1:
                    first = rows[0]
                    keys  = list(first.keys())
                    dt_sample = (first.get('NEWS_DT') or first.get('DissemDT') or 'MISSING')
                    print(f"  Keys: {keys[:10]}")
                    print(f"  NEWS_DT sample: '{dt_sample}'")
                    log.append(f"BSE NEWS_DT='{dt_sample}' keys={keys[:8]}")

                ts_ok = 0
                for row in rows:
                    title  = (row.get('HEADLINE') or '').strip()
                    scrip  = (row.get('SCRIP_NAME') or row.get('ShortName') or '').strip()
                    dt_raw = (row.get('NEWS_DT') or row.get('DissemDT') or '').strip()
                    cat    = (row.get('CATEGORYNAME') or '').lower()
                    attch  = row.get('ATTACHMENTNAME') or ''
                    code   = str(row.get('SCRIP_CD') or '')
                    link   = (
                        f'https://www.bseindia.com/xml-data/corpfiling/AttachLive/{attch}'
                        if attch else
                        f'https://www.bseindia.com/corporates/ann.html?scripcode={code}'
                    )
                    tp = ('dividend' if re.search(r'corp.*action|dividend|buyback|bonus|rights|split', cat)
                          else 'results'  if re.search(r'result|financial', cat)
                          else 'board'    if re.search(r'board|agm|egm', cat)
                          else 'insider'  if 'insider' in cat
                          else classify(title))
                    full   = f"{scrip}: {title}" if scrip and scrip.lower() not in title.lower() else title
                    parsed = parse_dt(dt_raw)
                    if to_ms(parsed) > 0:
                        ts_ok += 1
                    add_bse(full, link, 'BSE', parsed, tp)

                log.append(f"BSE page {page}: {ts_ok}/{len(rows)} with timestamps")
                print(f"  → {ts_ok}/{len(rows)} items have valid timestamps")

            # Forthcoming corporate actions (next 14 days)
            try:
                actions = bse.actions(
                    from_date=NOW_UTC - timedelta(days=1),
                    to_date=NOW_UTC + timedelta(days=14)
                ) or []
                for a in actions:
                    scrip   = (a.get('scrip_name') or a.get('SCRIP_NAME') or '').strip()
                    purpose = (a.get('purpose') or a.get('PURPOSE') or '').strip()
                    ex_date = a.get('ex_date') or a.get('EX_DATE') or ''
                    code    = str(a.get('scrip_code') or a.get('SCRIP_CD') or '')
                    link    = f'https://www.bseindia.com/corporates/ann.html?scripcode={code}'
                    if scrip and purpose:
                        title = f"{scrip}: {purpose}"
                        if ex_date:
                            title += f" (Ex: {ex_date})"
                        add_bse(title, link, 'BSE Actions', parse_dt(ex_date), 'dividend')
                log.append(f"BSE actions: {len(actions)}")
                print(f"  BSE actions: {len(actions)}")
            except Exception as e:
                log.append(f"BSE actions error: {e}")

            # Result calendar (next 14 days)
            try:
                results = bse.resultCalendar(
                    from_date=NOW_UTC,
                    to_date=NOW_UTC + timedelta(days=14)
                ) or []
                for r in results:
                    scrip  = (r.get('scrip_name') or r.get('SCRIP_NAME') or '').strip()
                    res_dt = r.get('result_date') or r.get('RESULT_DATE') or ''
                    code   = str(r.get('scrip_code') or r.get('SCRIP_CD') or '')
                    link   = f'https://www.bseindia.com/corporates/ann.html?scripcode={code}'
                    if scrip:
                        add_bse(f"{scrip}: Results expected", link, 'BSE Calendar',
                                parse_dt(res_dt), 'results')
                log.append(f"BSE calendar: {len(results)}")
                print(f"  BSE calendar: {len(results)}")
            except Exception as e:
                log.append(f"BSE calendar error: {e}")

    except ImportError:
        log.append("BSE: package not installed")
        print("WARNING: bse package missing — pip install bse")
    except Exception as e:
        log.append(f"BSE ERROR: {e}")
        print(f"BSE ERROR: {e}")
        import traceback; traceback.print_exc()


# ── SOURCE 2: NSE via nsepython ───────────────────────────────────────────────
def fetch_nse_official():
    try:
        import nsepython as nse  # type: ignore
        data = None
        for fn in ['nse_get_corporate_announcements', 'nse_corporate_actions']:
            try:
                data = getattr(nse, fn)()
                if data:
                    break
            except Exception:
                pass

        if isinstance(data, list) and data:
            added = 0
            for item in data[:100]:
                desc   = (item.get('desc') or item.get('subject') or '').strip()
                symbol = (item.get('symbol') or '').strip()
                dt_s   = item.get('bDt') or item.get('an_dt') or item.get('date') or ''
                attch  = item.get('attchmntFile') or ''
                link   = (f'https://nsearchives.nseindia.com/corporate/{attch}'
                          if attch else
                          'https://www.nseindia.com/companies-listing/corporate-filings-announcements')
                title  = f"{symbol}: {desc}" if symbol else desc
                add_bse(title, link, 'NSE', parse_dt(dt_s))
                added += 1
            log.append(f"NSE: {added} items")
        else:
            log.append("NSE: no data")
    except ImportError:
        log.append("NSE: nsepython not installed")
    except Exception as e:
        log.append(f"NSE ERROR: {e}")


# ── SOURCE 3: Targeted Google News RSS for NSE/BSE filings ───────────────────
def fetch_gnews_targeted():
    """
    Only queries specifically about exchange filings and corporate actions.
    NOT general market news.
    """
    import requests

    queries = [
        ("NSE+BSE+board+meeting+dividend+results+India+corporate+filing+2026",
         "Exchange News", None),
        ("BSE+NSE+quarterly+results+earnings+India+Q4+FY26",
         "BSE/NSE Results", "results"),
        ("NSE+BSE+dividend+declared+record+date+bonus+buyback+India+2026",
         "Corp Actions", "dividend"),
        ("NSE+BSE+board+meeting+AGM+EGM+India+April+May+2026",
         "Board Meetings", "board"),
        ("NSE+bulk+deal+block+deal+promoter+buying+selling+India+2026",
         "Bulk/Block Deals", "insider"),
    ]

    for query, source, default_type in queries:
        url = (f"https://news.google.com/rss/search?q={query}"
               f"&hl=en-IN&gl=IN&ceid=IN:en&scoring=n")
        try:
            r = requests.get(url, timeout=14, headers={
                'User-Agent': 'Mozilla/5.0 (compatible; NewsBot/1.0)',
            })
            r.raise_for_status()
            root = ET.fromstring(r.content)
            count = 0
            for item in list(root.iter('item'))[:30]:
                title = strip_html((item.findtext('title') or '').strip())
                link  = (item.findtext('link') or '').strip()
                dt_s  = (item.findtext('pubDate') or '')
                if title:
                    add_news(title, link, source, parse_dt(dt_s), default_type)
                    count += 1
            log.append(f"{source}: {count} items")
            print(f"{source}: {count} items")
        except Exception as e:
            log.append(f"{source}: FAILED — {e}")
            print(f"{source} FAILED: {e}")


# ── RUN ───────────────────────────────────────────────────────────────────────
print("=" * 60)
print(f"BSE/NSE Fetcher  |  {NOW_UTC.strftime('%Y-%m-%d %H:%M')} UTC")
print("=" * 60)

fetch_bse_official()
fetch_nse_official()
fetch_gnews_targeted()

# Sort each section: items with real ts newest first, ts=0 items after (still included)
def sort_key(x):
    ts = x.get('ts', 0)
    return ts if ts > 0 else 1  # ts=0 → sort as 1 (after real timestamps but present)

bse_items.sort(key=sort_key, reverse=True)
news_items.sort(key=lambda x: x.get('ts', 0), reverse=True)

# News: cap at 80 items (supplementary only)
news_items_out = news_items[:80]

output = {
    "updated":    NOW_UTC.isoformat(),
    "bse_count":  len(bse_items),
    "news_count": len(news_items_out),
    "log":        log,
    "bse_items":  bse_items,        # ALL BSE official items — no cap
    "news_items": news_items_out,   # targeted news — capped at 80
}

OUT.write_text(json.dumps(output, ensure_ascii=False, indent=2))

print("=" * 60)
print(f"BSE items:  {len(bse_items)}")
print(f"News items: {len(news_items_out)}")
bse_ts = [i['ts'] for i in bse_items if i['ts'] > 0]
if bse_ts:
    newest = datetime.fromtimestamp(max(bse_ts)/1000, tz=timezone.utc)
    print(f"BSE newest with ts: {newest:%Y-%m-%d %H:%M} UTC")
bse_zero = sum(1 for i in bse_items if i['ts'] == 0)
print(f"BSE items with ts=0: {bse_zero} (shown as 'Today' in UI)")
src_counts = {}
for i in bse_items:
    src_counts[i['source']] = src_counts.get(i['source'], 0) + 1
for src, cnt in sorted(src_counts.items(), key=lambda x: -x[1]):
    print(f"  BSE/{src:20s} {cnt}")
print(f"Log: {log}")
print("=" * 60)
