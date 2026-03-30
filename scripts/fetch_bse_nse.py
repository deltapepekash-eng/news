#!/usr/bin/env python3
"""
BSE/NSE data fetcher for GitHub Pages.

OUTPUT STRUCTURE (two separate arrays — BSE never cut by news cap):
  bse_items  — official BSE filings, actions, calendar (authoritative, no cap)
  news_items — targeted Google News queries about NSE/BSE (supplementary, cap 150)

FIXES vs previous version:
  - bse + nsepython packages now properly expected (added to workflow pip install)
  - BSE announcements: pages 1–8 fetched (was 1–4), stops only on empty page
  - fetch_rss_corp: cap raised from [:50] to [:200] — was silently truncating
  - fetch_gnews_targeted: cap raised from [:30] to [:50] per query
  - All sources: robust fallback if bse/nsepython absent (graceful degradation)
  - Dedup key includes link so same title from two stocks isn't dropped
  - Zero-timestamp items kept but pushed to bottom (not silently dropped)
  - Detailed per-source counts in log for easier debugging
"""

import json, re, xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path

OUT     = Path(__file__).parent.parent / "data" / "bse_nse.json"
OUT.parent.mkdir(parents=True, exist_ok=True)
NOW_UTC = datetime.now(timezone.utc)
NEWS_CUTOFF_MS = int((NOW_UTC - timedelta(days=7)).timestamp() * 1000)

bse_items:  list[dict] = []
news_items: list[dict] = []
bse_seen:   set[str]   = set()
news_seen:  set[str]   = set()
log:        list[str]  = []


def slug(title: str, link: str = '') -> str:
    """Dedup key: title slug + last path segment of link to avoid cross-stock drops."""
    t_slug = re.sub(r'[^a-z0-9]', '', (title or '').lower())[:50]
    # Extract scrip code or last path component from link
    m = re.search(r'scripcode=(\d+)', link or '')
    suffix = m.group(1) if m else re.sub(r'[^a-z0-9]', '', (link or '').lower())[-10:]
    return t_slug + suffix


def parse_dt(s: str):
    if not s:
        return None

    s = s.strip()

    # Normalize ISO format
    s = s.replace('T', ' ')

    # Fix fractional seconds
    if '.' in s:
        try:
            main, frac = s.split('.', 1)
            frac = re.sub(r'[^0-9]', '', frac)
            frac = (frac + '000000')[:6]
            s = f"{main}.{frac}"
        except Exception:
            pass

    # Remove timezone noise
    s = re.sub(r'\s+[+-]\d{4}$', '', s)
    s = s.replace('GMT', '').strip()

    # Try ISO first
    try:
        dt = datetime.fromisoformat(s)
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass

    # Fallback formats
    fmts = [
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S',
        '%m/%d/%Y %I:%M:%S %p',
        '%m/%d/%Y %I:%M %p',
        '%d %b %Y %H:%M:%S',
        '%d %b %Y %I:%M %p',
        '%Y-%m-%d',
    ]

    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue

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
    """Add to BSE section. No stale filter, no cap. Min title 5 chars."""
    title = (title or '').strip()
    if not title or len(title) < 5:
        return
    # FIX: include link in dedup key so same headline for two different stocks both appear
    k = slug(title, link)
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
    """Add to news section. Drops items older than 7 days. Deduplicates vs BSE."""
    title = (title or '').strip()
    if not title or len(title) < 8:
        return
    k = slug(title, link)
    if k in news_seen or k in bse_seen:
        return
    ts = to_ms(dt)
    if ts > 0 and ts < NEWS_CUTOFF_MS:
        return
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
    except ImportError:
        log.append("BSE: package not installed — run: pip install bse")
        print("WARNING: bse package missing — pip install bse")
        return

    try:
        with BSE(download_folder='/tmp/bse_dl') as bse:

            # FIX: fetch pages 1–8 (was 1–4), stop only when page returns < 5 rows
            total_added = 0
            for page in range(1, 9):
                try:
                    data = bse.announcements(page_no=page)
                    rows = data.get('Table') or []
                    print(f"  BSE page {page}: {len(rows)} rows")

                    if not rows:
                        log.append(f"BSE p{page}: empty — stopping pagination")
                        break

                    if page == 1 and rows:
                        first   = rows[0]
                        keys    = list(first.keys())
                        dt_samp = (first.get('NEWS_DT') or first.get('DissemDT') or 'MISSING')
                        print(f"  NEWS_DT sample: '{dt_samp}'")
                        log.append(f"BSE NEWS_DT='{dt_samp}' keys={keys[:8]}")

                    ts_ok = 0
                    page_added = 0
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
                        before = len(bse_items)
                        add_bse(full, link, 'BSE', parsed, tp)
                        if len(bse_items) > before:
                            page_added += 1

                    total_added += page_added
                    log.append(f"BSE p{page}: {len(rows)} rows, {page_added} new, {ts_ok} with ts")

                    # Stop early if page is sparse (last page reached)
                    if len(rows) < 5:
                        break

                except Exception as e:
                    log.append(f"BSE page {page} ERROR: {e}")
                    print(f"  BSE page {page} error: {e}")
                    break

            log.append(f"BSE announcements total added: {total_added}")
            print(f"  BSE total: {total_added} announcements added")

            # Corporate actions ±14 days
            try:
                actions = bse.actions(
                    from_date=NOW_UTC - timedelta(days=1),
                    to_date=NOW_UTC + timedelta(days=14)
                ) or []
                act_added = 0
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
                        before = len(bse_items)
                        add_bse(title, link, 'BSE Actions', parse_dt(ex_date), 'dividend')
                        if len(bse_items) > before:
                            act_added += 1
                log.append(f"BSE actions: {len(actions)} fetched, {act_added} added")
                print(f"  BSE actions: {len(actions)} fetched, {act_added} added")
            except Exception as e:
                log.append(f"BSE actions error: {e}")

            # Result calendar next 14 days
            try:
                results = bse.resultCalendar(
                    from_date=NOW_UTC,
                    to_date=NOW_UTC + timedelta(days=14)
                ) or []
                cal_added = 0
                for r in results:
                    scrip  = (r.get('scrip_name') or r.get('SCRIP_NAME') or '').strip()
                    res_dt = r.get('result_date') or r.get('RESULT_DATE') or ''
                    code   = str(r.get('scrip_code') or r.get('SCRIP_CD') or '')
                    link   = f'https://www.bseindia.com/corporates/ann.html?scripcode={code}'
                    if scrip:
                        before = len(bse_items)
                        add_bse(f"{scrip}: Results expected", link, 'BSE Calendar',
                                parse_dt(res_dt), 'results')
                        if len(bse_items) > before:
                            cal_added += 1
                log.append(f"BSE calendar: {len(results)} fetched, {cal_added} added")
                print(f"  BSE calendar: {len(results)} fetched, {cal_added} added")
            except Exception as e:
                log.append(f"BSE calendar error: {e}")

    except Exception as e:
        log.append(f"BSE ERROR: {e}")
        print(f"BSE ERROR: {e}")
        import traceback; traceback.print_exc()


# ── SOURCE 2: NSE via nsepython ───────────────────────────────────────────────
def fetch_nse_official():
    try:
        import nsepython as nse  # type: ignore
    except ImportError:
        log.append("NSE: nsepython not installed — run: pip install nsepython")
        print("WARNING: nsepython missing — pip install nsepython")
        return

    try:
        data = None
        for fn in ['nse_get_corporate_announcements', 'nse_corporate_actions']:
            try:
                data = getattr(nse, fn)()
                if data:
                    log.append(f"NSE: used function {fn}")
                    break
            except Exception as e:
                log.append(f"NSE {fn}: {e}")

        if isinstance(data, list) and data:
            added = 0
            # FIX: was [:100], now no artificial cap — let dedup handle it
            for item in data:
                desc   = (item.get('desc') or item.get('subject') or '').strip()
                symbol = (item.get('symbol') or '').strip()
                dt_s   = item.get('bDt') or item.get('an_dt') or item.get('date') or ''
                attch  = item.get('attchmntFile') or ''
                link   = (f'https://nsearchives.nseindia.com/corporate/{attch}'
                          if attch else
                          'https://www.nseindia.com/companies-listing/corporate-filings-announcements')
                title  = f"{symbol}: {desc}" if symbol else desc
                before = len(bse_items)
                add_bse(title, link, 'NSE', parse_dt(dt_s))
                if len(bse_items) > before:
                    added += 1
            log.append(f"NSE: {len(data)} fetched, {added} added")
            print(f"  NSE: {len(data)} fetched, {added} added")
        else:
            log.append("NSE: no data returned")
            print("  NSE: no data returned")
    except Exception as e:
        log.append(f"NSE ERROR: {e}")
        print(f"NSE ERROR: {e}")


# ── SOURCE 3: Moneycontrol corporate announcements RSS ───────────────────────
def fetch_rss_corp(url: str, source: str, default_type=None):
    import requests
    try:
        r = requests.get(url, timeout=14, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; NewsBot/1.0)',
        })
        r.raise_for_status()
        root = ET.fromstring(r.content)
        # FIX: was [:50] — hard cap that silently truncated the feed
        all_items = list(root.iter('item'))
        count = 0
        for item in all_items:
            title = strip_html((item.findtext('title') or '').strip())
            link  = (item.findtext('link') or '').strip()
            dt_s  = item.findtext('pubDate') or ''
            if title:
                add_news(title, link, source, parse_dt(dt_s), default_type)
                count += 1
        log.append(f"{source}: {len(all_items)} in feed, {count} processed")
        print(f"{source}: {len(all_items)} in feed, {count} processed")
    except Exception as e:
        log.append(f"{source}: {e}")
        print(f"{source} FAILED: {e}")


# ── SOURCE 4: Targeted Google News RSS ───────────────────────────────────────
def fetch_gnews_targeted():
    import requests
    queries = [
        # Corporate filings & actions
        ("NSE+BSE+board+meeting+dividend+results+India+corporate+filing+2026", "Exchange News", None),
        ("BSE+NSE+quarterly+results+earnings+India+Q4+FY26+FY2026", "BSE/NSE Results", "results"),
        ("NSE+BSE+dividend+declared+record+date+bonus+buyback+India+2026", "Corp Actions", "dividend"),
        ("NSE+BSE+board+meeting+AGM+EGM+India+April+May+2026", "Board Meetings", "board"),
        ("NSE+bulk+deal+block+deal+promoter+buying+selling+India+2026", "Bulk/Block Deals", "insider"),
        ("BSE+NSE+trading+window+closure+India+2026", "Trading Window", "insider"),
        ("BSE+NSE+allotment+shares+warrants+India+2026", "Share Allotment", "filing"),
        ("NSE+BSE+merger+acquisition+amalgamation+India+2026", "M&A", "filing"),
        ("NSE+BSE+credit+rating+ICRA+CRISIL+India+2026", "Ratings", "filing"),
        ("BSE+NSE+order+win+contract+awarded+India+2026", "Orders Won", "filing"),
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
            # FIX: was [:30] per query — raised to [:50] for more coverage
            all_items = list(root.iter('item'))
            count = 0
            added = 0
            for item in all_items[:50]:
                title = strip_html((item.findtext('title') or '').strip())
                link  = (item.findtext('link') or '').strip()
                dt_s  = item.findtext('pubDate') or ''
                if title:
                    before = len(news_items)
                    add_news(title, link, source, parse_dt(dt_s), default_type)
                    if len(news_items) > before:
                        added += 1
                    count += 1
            log.append(f"{source}: {len(all_items)} in feed, {count} tried, {added} new")
            print(f"{source}: {len(all_items)} in feed, {count} tried, {added} new")
        except Exception as e:
            log.append(f"{source}: FAILED — {e}")
            print(f"{source} FAILED: {e}")


# ── RUN ───────────────────────────────────────────────────────────────────────
print("=" * 60)
print(f"BSE/NSE Fetcher  |  {NOW_UTC.strftime('%Y-%m-%d %H:%M')} UTC")
print("=" * 60)

fetch_bse_official()
fetch_nse_official()
fetch_rss_corp("https://www.moneycontrol.com/rss/corporateannouncements.xml",
               "Moneycontrol Corp")
fetch_gnews_targeted()

# Sort BSE: real timestamps newest first, ts=0 pushed to bottom
bse_items.sort(key=lambda x: x['ts'] if x['ts'] > 0 else 1, reverse=True)
# Sort news: newest first, cap at 150
news_items.sort(key=lambda x: x.get('ts', 0), reverse=True)
news_items_out = news_items[:150]

output = {
    "updated":    NOW_UTC.isoformat(),
    "bse_count":  len(bse_items),
    "news_count": len(news_items_out),
    "log":        log,
    "bse_items":  bse_items,
    "news_items": news_items_out,
}

OUT.write_text(json.dumps(output, ensure_ascii=False, indent=2))

print("=" * 60)
print(f"TOTAL: {len(bse_items)} BSE items + {len(news_items_out)} news items"
      f" = {len(bse_items) + len(news_items_out)}")
bse_ts   = [i['ts'] for i in bse_items if i['ts'] > 0]
bse_zero = sum(1 for i in bse_items if i['ts'] == 0)
if bse_ts:
    newest = datetime.fromtimestamp(max(bse_ts)/1000, tz=timezone.utc)
    print(f"BSE newest ts: {newest:%Y-%m-%d %H:%M} UTC | ts=0 count: {bse_zero}")
src_counts: dict = {}
for i in bse_items + news_items_out:
    src_counts[i['source']] = src_counts.get(i['source'], 0) + 1
for src, cnt in sorted(src_counts.items(), key=lambda x: -x[1]):
    print(f"  {src:30s} {cnt}")
print(f"Log: {log}")
print("=" * 60)
