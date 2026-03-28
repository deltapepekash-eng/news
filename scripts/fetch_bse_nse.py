#!/usr/bin/env python3
"""
Fetch BSE & NSE corporate announcements and save as JSON for GitHub Pages.
Runs server-side via GitHub Actions — no CORS restrictions.
"""

import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
import requests

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/xml, */*",
    "Accept-Language": "en-IN,en;q=0.9",
})
TIMEOUT = 15

items = []
seen  = set()

def slug(s):
    return re.sub(r'[^a-z0-9]', '', (s or '').lower())[:60]

def add(title, link, source, date_str, ann_type=None):
    title = (title or '').strip()
    if not title or len(title) < 5:
        return
    k = slug(title)
    if k in seen:
        return
    seen.add(k)
    # Parse timestamp
    ts = 0
    age = ""
    if date_str:
        for fmt in ('%Y-%m-%dT%H:%M:%S', '%d %b %Y %H:%M:%S', '%a, %d %b %Y %H:%M:%S %z',
                    '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%Y-%m-%d'):
            try:
                dt = datetime.strptime(date_str.strip().rstrip(' +0000').rstrip(' GMT'), fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                ts = int(dt.timestamp() * 1000)
                break
            except Exception:
                pass
        if ts:
            secs = (datetime.now(timezone.utc).timestamp() * 1000 - ts) / 1000
            if secs < 60:       age = "just now"
            elif secs < 3600:   age = f"{int(secs/60)}m ago"
            elif secs < 86400:  age = f"{int(secs/3600)}h ago"
            else:               age = f"{int(secs/86400)}d ago"

    if not ann_type:
        ann_type = classify(title)

    items.append({
        "title":  title,
        "link":   link or "#",
        "source": source,
        "ts":     ts,
        "age":    age,
        "type":   ann_type,
    })

def classify(title):
    t = title.lower()
    if re.search(r'dividend|buyback|bonus|rights issue|stock split', t): return 'dividend'
    if re.search(r'q[1-4]|quarter|result|profit|revenue|earnings|pat|ebitda', t): return 'results'
    if re.search(r'board meeting|agm|egm|annual general|extraordinary general', t): return 'board'
    if re.search(r'insider|promoter|stake|pledge|bulk|block deal', t): return 'insider'
    return 'filing'

def strip_html(s):
    return re.sub(r'<[^>]+>', '', s or '').strip()

# ─── SOURCE 1: BSE Direct API ───────────────────────────────────────────────
def fetch_bse_direct():
    url = ("https://api.bseindia.com/BseIndiaAPI/api/Announcement/w"
           "?strCat=-1&strSearch=P&strType=C&page=1")
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        d = r.json()
        rows = d.get('Table') or d.get('table') or d.get('data') or []
        for row in rows[:50]:
            title  = (row.get('HEADLINE') or row.get('Subject') or '').strip()
            scrip  = (row.get('SCRIP_NAME') or row.get('ShortName') or '').strip()
            dt_str = row.get('DissemDT') or row.get('Dt') or ''
            cat    = (row.get('CATEGORYNAME') or '').lower()
            attch  = row.get('ATTACHMENTNAME') or ''
            link   = ('https://www.bseindia.com/xml-data/corpfiling/AttachLive/' + attch
                      if attch else
                      'https://www.bseindia.com/corporates/ann.html?scrip=' +
                      str(row.get('SCRIP_CD', '')))
            tp = ('dividend' if re.search(r'dividend|buyback|bonus|rights', cat) else
                  'results'  if re.search(r'result|financial', cat) else
                  'board'    if 'board' in cat else
                  'insider'  if re.search(r'insider|bulk|block', cat) else 'filing')
            full = f"{scrip}: {title}" if scrip else title
            add(full, link, 'BSE Direct', dt_str, tp)
        print(f"BSE Direct: {len(rows)} rows fetched")
    except Exception as e:
        print(f"BSE Direct failed: {e}")

# ─── SOURCE 2: BSE Corporate Filing RSS (alternate endpoint) ────────────────
def fetch_bse_rss():
    urls = [
        "https://www.bseindia.com/xml-data/corpfiling/AttachLive/RSS.xml",
        "https://www.bseindia.com/markets/equity/EQReports/Ann_Equityresult.aspx?expandable=5",
    ]
    for url in urls:
        try:
            r = SESSION.get(url, timeout=TIMEOUT)
            if not r.ok:
                continue
            root = ET.fromstring(r.text)
            for item in root.iter('item'):
                title = strip_html((item.findtext('title') or '').strip())
                link  = (item.findtext('link') or '').strip()
                dt    = (item.findtext('pubDate') or '').strip()
                if title:
                    add(title, link, 'BSE RSS', dt)
            print(f"BSE RSS {url}: ok")
            break
        except Exception as e:
            print(f"BSE RSS {url}: {e}")

# ─── SOURCE 3: NSE via Screener / Tickertape public API ─────────────────────
def fetch_nse_via_screener():
    """Screener.in hosts a public corporate actions feed that mirrors NSE."""
    url = "https://www.screener.in/api/announcements/?format=json"
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        for item in (data if isinstance(data, list) else data.get('results', []))[:40]:
            title = item.get('title') or item.get('headline') or ''
            link  = item.get('url') or item.get('attachment') or '#'
            dt    = item.get('date') or item.get('created_at') or ''
            src   = item.get('company_name') or 'NSE/Screener'
            add(f"{src}: {title}" if src and src != title else title, link, 'NSE via Screener', dt)
        print(f"Screener: ok")
    except Exception as e:
        print(f"Screener failed: {e}")

# ─── SOURCE 4: Moneycontrol Corporate Announcements RSS ─────────────────────
def fetch_moneycontrol_rss():
    url = "https://www.moneycontrol.com/rss/corporateannouncements.xml"
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        for item in root.iter('item'):
            title = strip_html(item.findtext('title') or '')
            link  = (item.findtext('link') or '').strip()
            dt    = item.findtext('pubDate') or ''
            if title:
                add(title, link, 'Moneycontrol', dt)
        print("Moneycontrol RSS: ok")
    except Exception as e:
        print(f"Moneycontrol RSS failed: {e}")

# ─── SOURCE 5: ET Markets Corporate RSS ─────────────────────────────────────
def fetch_et_rss():
    url = "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms"
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        for item in root.iter('item'):
            title = strip_html(item.findtext('title') or '')
            link  = (item.findtext('link') or '').strip()
            dt    = item.findtext('pubDate') or ''
            if title:
                add(title, link, 'ET Markets', dt)
        print("ET Markets RSS: ok")
    except Exception as e:
        print(f"ET Markets RSS failed: {e}")

# ─── SOURCE 6: LiveMint Companies RSS ───────────────────────────────────────
def fetch_livemint_rss():
    url = "https://www.livemint.com/rss/companies"
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        for item in root.iter('item'):
            title = strip_html(item.findtext('title') or '')
            link  = (item.findtext('link') or '').strip()
            dt    = item.findtext('pubDate') or ''
            if title:
                add(title, link, 'LiveMint', dt)
        print("LiveMint RSS: ok")
    except Exception as e:
        print(f"LiveMint RSS failed: {e}")

# ─── SOURCE 7: Business Standard Markets RSS ────────────────────────────────
def fetch_bs_rss():
    url = "https://www.business-standard.com/rss/markets-106.rss"
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        for item in root.iter('item'):
            title = strip_html(item.findtext('title') or '')
            link  = (item.findtext('link') or '').strip()
            dt    = item.findtext('pubDate') or ''
            if title:
                add(title, link, 'Business Standard', dt)
        print("Business Standard RSS: ok")
    except Exception as e:
        print(f"Business Standard RSS failed: {e}")

# ─── SOURCE 8: NSE via Google News RSS (5-min lag) ──────────────────────────
def fetch_gnews_nse():
    queries = [
        "NSE+corporate+action+results+dividend+India",
        "BSE+NSE+results+dividend+board+meeting+India+corporate",
        "NSE+India+quarterly+results+bonus+rights+issue+2026",
        "NSE+BSE+dividend+declared+buyback+bonus+India",
        "NSE+BSE+board+meeting+AGM+EGM+India+2026",
        "NSE+insider+trading+promoter+buying+bulk+deal+India",
    ]
    for q in queries:
        url = f"https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en"
        try:
            r = SESSION.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            root = ET.fromstring(r.text)
            for item in root.iter('item'):
                title = strip_html(item.findtext('title') or '')
                link  = (item.findtext('link') or '').strip()
                dt    = item.findtext('pubDate') or ''
                src_el = item.find('source')
                src   = src_el.text.strip() if src_el is not None and src_el.text else 'Google News'
                if title:
                    add(title, link, src, dt)
            print(f"GNews {q[:30]}: ok")
        except Exception as e:
            print(f"GNews {q[:30]} failed: {e}")

# ─── RUN ALL SOURCES ─────────────────────────────────────────────────────────
print("=== Starting BSE/NSE data fetch ===")
fetch_bse_direct()
fetch_bse_rss()
fetch_nse_via_screener()
fetch_moneycontrol_rss()
fetch_et_rss()
fetch_livemint_rss()
fetch_bs_rss()
fetch_gnews_nse()

# Sort newest first
items.sort(key=lambda x: x.get('ts', 0), reverse=True)
items = items[:200]  # keep top 200

output = {
    "updated": datetime.now(timezone.utc).isoformat(),
    "count":   len(items),
    "items":   items,
}

out_path = Path(__file__).parent.parent / "data" / "bse_nse.json"
out_path.parent.mkdir(exist_ok=True)
out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2))
print(f"=== Saved {len(items)} items to {out_path} ===")
