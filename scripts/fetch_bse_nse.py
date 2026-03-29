#!/usr/bin/env python3
"""
Fetch BSE & NSE corporate announcements for GitHub Pages.
FIXES in this version:
  A. BSE date format: "28 Mar 2026  2:05:11 PM" (double-space, 12hr AM/PM)
  B. Stale filter: reject items older than 7 days from the JSON entirely
  C. Age strings NOT stored (HTML recomputes live from ts milliseconds)
  D. Google News sorted by date via &scoring=n param
  E. nsepython: try multiple function signatures
  F. BSE pages 1+2 + actions + result calendar all fetched
"""

import json, re, xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path

OUT       = Path(__file__).parent.parent / "data" / "bse_nse.json"
OUT.parent.mkdir(exist_ok=True)
NOW_UTC   = datetime.now(timezone.utc)
CUTOFF_MS = int((NOW_UTC - timedelta(days=7)).timestamp() * 1000)

items: list[dict] = []
seen:  set[str]   = set()
log:   list[str]  = []


def slug(s: str) -> str:
    return re.sub(r'[^a-z0-9]', '', (s or '').lower())[:60]


def parse_dt(s: str):
    if not s:
        return None
    s = re.sub(r'\s+', ' ', s.strip())           # collapse double-space (BSE quirk)
    s = re.sub(r'\s+[+-]\d{4}$', '', s).strip()  # strip trailing +0530 etc
    s = re.sub(r'\s+GMT$', '', s).strip()
    fmts = [
        # ── BSE ACTUAL FORMAT (M/D/YYYY h:mm:ss AM/PM) ─────────────────────
        '%m/%d/%Y %I:%M:%S %p',   # "3/28/2026 2:05:11 PM"  ← confirmed BSE format
        '%m/%d/%Y %I:%M %p',      # "3/28/2026 2:05 PM"
        '%m/%d/%Y %H:%M:%S',      # "3/28/2026 14:05:11"
        '%m/%d/%Y',               # "3/28/2026"
        # ── OTHER FORMATS ───────────────────────────────────────────────────
        '%d %b %Y %I:%M:%S %p',   # "28 Mar 2026 2:05:11 PM"
        '%d %b %Y %I:%M %p',
        '%d %b %Y %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S',      # ISO 8601
        '%Y-%m-%d %H:%M:%S',
        '%a, %d %b %Y %H:%M:%S',  # RFC 822 (RSS pubDate)
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
    if re.search(r'dividend|buyback|bonus|rights issue|stock split|face value', t): return 'dividend'
    if re.search(r'\bq[1-4]\b|quarter|result|profit|revenue|earnings|pat\b|ebitda|financial result', t): return 'results'
    if re.search(r'board meeting|agm|egm|annual general|extraordinary general', t): return 'board'
    if re.search(r'insider|promoter|stake|pledge|bulk deal|block deal', t): return 'insider'
    return 'filing'


def strip_html(s: str) -> str:
    return re.sub(r'<[^>]+>', '', s or '').strip()


def add(title: str, link: str, source: str, dt, ann_type=None, force=False):
    """
    Add item to output. Drops items older than 7 days unless force=True.
    Does NOT store age string — HTML recomputes from ts milliseconds at render time.
    """
    title = (title or '').strip()
    if not title or len(title) < 5:
        return
    k = slug(title)
    if k in seen:
        return
    ts = to_ms(dt)
    if ts > 0 and ts < CUTOFF_MS and not force:
        return   # stale — skip
    seen.add(k)
    items.append({
        "title":  title,
        "link":   link or "#",
        "source": source,
        "ts":     ts,
        # NOTE: no "age" field — HTML computes it live so it's always accurate
        "type":   ann_type or classify(title),
    })


# ── SOURCE 1: BSE official Python package ────────────────────────────────────
def fetch_bse_official():
    try:
        from bse import BSE  # type: ignore
        print("BSE: starting session...")
        with BSE(download_folder='/tmp/bse_dl') as bse:

            for page in [1, 2]:
                data = bse.announcements(page_no=page)
                rows = data.get('Table') or []
                print(f"  BSE page {page}: {len(rows)} rows")
                # Debug: log actual field values from first row to diagnose date format
                if rows and page == 1:
                    first = rows[0]
                    all_keys = list(first.keys())
                    news_dt = first.get('NEWS_DT') or first.get('DissemDT') or 'NOT FOUND'
                    print(f"  BSE field keys: {all_keys[:10]}")
                    print(f"  BSE NEWS_DT sample: '{news_dt}'")
                    log.append(f"BSE NEWS_DT sample: '{news_dt}' | keys: {all_keys[:8]}")
                added_count = 0
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
                    full = f"{scrip}: {title}" if scrip and scrip.lower() not in title.lower() else title
                    parsed = parse_dt(dt_raw)
                    ts_val = to_ms(parsed)
                    if ts_val > 0:
                        added_count += 1
                    add(full, link, 'BSE', parsed, tp)
                print(f"  BSE page {page}: {added_count}/{len(rows)} items with valid timestamps")
                log.append(f"BSE page {page}: {added_count}/{len(rows)} items with valid timestamps")

            # Forthcoming corporate actions ± 14 days
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
                        title = f"{scrip}: {purpose}" + (f" (Ex: {ex_date})" if ex_date else "")
                        add(title, link, 'BSE Actions', parse_dt(ex_date), 'dividend', force=True)
                log.append(f"BSE actions: {len(actions)} items")
            except Exception as e:
                log.append(f"BSE actions: {e}")

            # Result calendar — next 14 days
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
                        add(f"{scrip}: Results expected", link, 'BSE Calendar',
                            parse_dt(res_dt), 'results', force=True)
                log.append(f"BSE result calendar: {len(results)} items")
            except Exception as e:
                log.append(f"BSE result calendar: {e}")

    except ImportError:
        log.append("BSE: package not installed — run: pip install bse")
        print("WARNING: bse package missing")
    except Exception as e:
        log.append(f"BSE: ERROR — {e}")
        print(f"BSE ERROR: {e}")
        import traceback; traceback.print_exc()


# ── SOURCE 2: NSE via nsepython ───────────────────────────────────────────────
def fetch_nse_official():
    try:
        import nsepython as nse  # type: ignore
        data = None

        # nsepython API has changed between versions — try each known name
        for fn in ['nse_get_corporate_announcements',
                   'nse_corporate_actions',
                   'nsetools']:
            try:
                data = getattr(nse, fn)()
                if data:
                    print(f"NSE: {fn}() returned {len(data) if isinstance(data, list) else type(data)}")
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
                add(title, link, 'NSE', parse_dt(dt_s))
                added += 1
            log.append(f"NSE: {added} items added")
        else:
            log.append("NSE: no data (try updating nsepython)")
    except ImportError:
        log.append("NSE: nsepython not installed — run: pip install nsepython")
    except Exception as e:
        log.append(f"NSE: ERROR — {e}")
        print(f"NSE ERROR: {e}")


# ── SOURCE 3: RSS feeds (no CORS on GitHub Actions) ───────────────────────────
def fetch_rss(url: str, source: str, default_type=None):
    import requests
    try:
        r = requests.get(url, timeout=14, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; NewsAggregator/1.0)',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*',
        })
        r.raise_for_status()
        root = ET.fromstring(r.content)
        count = 0
        for item in list(root.iter('item'))[:50]:
            title = strip_html((item.findtext('title') or '').strip())
            link  = (item.findtext('link') or '').strip()
            dt_s  = (item.findtext('pubDate') or
                     item.findtext('{http://purl.org/dc/elements/1.1/}date') or '')
            if title:
                add(title, link, source, parse_dt(dt_s), default_type)
                count += 1
        log.append(f"{source}: {count} items (7d window)")
        print(f"{source}: {count} items")
    except Exception as e:
        log.append(f"{source}: FAILED — {e}")
        print(f"{source} FAILED: {e}")


# ── SOURCE 4: Google News RSS — &scoring=n sorts by date not relevance ─────────
def fetch_gnews(query: str, source: str, default_type=None):
    url = f"https://news.google.com/rss/search?q={query}&hl=en-IN&gl=IN&ceid=IN:en&scoring=n"
    fetch_rss(url, source, default_type)


# ── RUN ───────────────────────────────────────────────────────────────────────
print("=" * 60)
print(f"BSE/NSE Fetcher — cutoff: {(NOW_UTC-timedelta(days=7)).strftime('%Y-%m-%d')} UTC")
print("=" * 60)

fetch_bse_official()
fetch_nse_official()
fetch_rss("https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms", "ET Markets")
fetch_rss("https://www.livemint.com/rss/companies", "LiveMint")
fetch_rss("https://www.moneycontrol.com/rss/corporateannouncements.xml", "Moneycontrol")
fetch_rss("https://www.business-standard.com/rss/markets-106.rss", "Business Standard")
fetch_gnews("NSE+BSE+board+meeting+results+dividend+India+corporate", "Exchange News")
fetch_gnews("BSE+NSE+quarterly+results+earnings+India", "BSE/NSE Results", "results")
fetch_gnews("NSE+BSE+dividend+bonus+buyback+India", "Corp Actions", "dividend")
fetch_gnews("NSE+BSE+board+meeting+AGM+EGM+India", "Board Meetings", "board")
fetch_gnews("NSE+bulk+deal+block+deal+promoter+buying+India", "Bulk/Block Deals", "insider")

# Sort newest first, cap at 200
items.sort(key=lambda x: x.get('ts', 0), reverse=True)
items_out = items[:200]

output = {
    "updated": NOW_UTC.isoformat(),
    "count":   len(items_out),
    "log":     log,
    "items":   items_out,
}
OUT.write_text(json.dumps(output, ensure_ascii=False, indent=2))

print("=" * 60)
tss = [i['ts'] for i in items_out if i['ts'] > 0]
if tss:
    newest = datetime.fromtimestamp(max(tss)/1000, tz=timezone.utc)
    oldest = datetime.fromtimestamp(min(tss)/1000, tz=timezone.utc)
    print(f"Saved {len(items_out)} items  |  newest: {newest:%Y-%m-%d %H:%M}  oldest: {oldest:%Y-%m-%d %H:%M} UTC")
sources = {}
for i in items_out:
    sources[i['source']] = sources.get(i['source'], 0) + 1
for src, cnt in sorted(sources.items(), key=lambda x: -x[1]):
    print(f"  {src:30s} {cnt}")
print(f"Log: {log}")
print("=" * 60)
