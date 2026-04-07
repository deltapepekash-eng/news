#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BHARAT·INTEL — Market Intelligence Fetcher  v2.1
Fixed: Python 3.9 compat, robust RSS headers, working BSE/NSE fallbacks.
"""

from __future__ import annotations   # fixes list[str] on Python 3.9

import json
import hashlib
import time
import urllib.request
import urllib.error
import re
from datetime import datetime, timezone, timedelta
from xml.etree import ElementTree as ET
from collections import Counter
from pathlib import Path
from typing import Optional, List, Dict

# ── CONFIG ────────────────────────────────────────────────────────────────────
MAX_ITEMS     = 200
FETCH_TIMEOUT = 20
DATA_DIR      = Path("data")
IST           = timezone(timedelta(hours=5, minutes=30))

# ── HEADERS ───────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# ── NEWS SOURCES ──────────────────────────────────────────────────────────────
# Each entry: (display_name, [url1, url2, ...], category)
# Multiple URLs = fallback chain; first success wins.

INDIA_RSS: List[tuple] = [
    ("Economic Times Markets",
     ["https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
      "https://economictimes.indiatimes.com/rssfeedsdefault.cms"],
     "Markets"),
    ("Economic Times Economy",
     ["https://economictimes.indiatimes.com/economy/rssfeeds/1373380680.cms"],
     "Economy"),
    ("Mint Markets",
     ["https://www.livemint.com/rss/markets",
      "https://www.livemint.com/rss/news"],
     "Markets"),
    ("Business Standard",
     ["https://www.business-standard.com/rss/markets-106.rss",
      "https://www.business-standard.com/rss/home_page_top_stories.rss"],
     "Markets"),
    ("Financial Express",
     ["https://www.financialexpress.com/market/feed/"],
     "Markets"),
    ("MoneyControl",
     ["https://www.moneycontrol.com/rss/marketreports.xml",
      "https://www.moneycontrol.com/rss/latestnews.xml"],
     "Markets"),
    ("Hindu Business Line",
     ["https://www.thehindubusinessline.com/markets/stock-markets/feeder/default.rss",
      "https://www.thehindubusinessline.com/feeder/default.rss"],
     "Markets"),
    ("NDTV Profit",
     ["https://www.ndtvprofit.com/rss?type=4"],
     "Markets"),
    # Google News — always works, good backup
    ("India Markets (Google)",
     ["https://news.google.com/rss/search?q=india+stock+market+nifty+sensex&hl=en-IN&gl=IN&ceid=IN:en"],
     "Markets"),
    ("India Economy (Google)",
     ["https://news.google.com/rss/search?q=india+economy+rbi+sebi+budget&hl=en-IN&gl=IN&ceid=IN:en"],
     "Economy"),
]

WORLD_RSS: List[tuple] = [
    ("Reuters Business",
     ["https://feeds.reuters.com/reuters/businessNews"],
     "Global Economy"),
    ("Reuters Markets",
     ["https://feeds.reuters.com/reuters/markets"],
     "Global Markets"),
    ("CNBC World Economy",
     ["https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258"],
     "Global Economy"),
    ("Yahoo Finance",
     ["https://finance.yahoo.com/news/rssindex"],
     "Global Economy"),
    ("Investing.com Commodities",
     ["https://www.investing.com/rss/news_25.rss"],
     "Commodities"),
    ("Global Markets (Google)",
     ["https://news.google.com/rss/search?q=global+stock+market+fed+rates+wall+street&hl=en&gl=US&ceid=US:en"],
     "Global Markets"),
    ("Commodities (Google)",
     ["https://news.google.com/rss/search?q=crude+oil+gold+commodities+opec+brent&hl=en&gl=US&ceid=US:en"],
     "Commodities"),
]

# BSE — RSS feed (most stable), then JSON API, then Google News
BSE_RSS_URL = "https://www.bseindia.com/markets/marketinfo/RSSFeed.aspx?type=corp_ann"
BSE_API_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
    "?strCat=-1&strPrevDate={prev}&strScrip=&strSearch=P"
    "&strToDate={today}&strType=C&subcategory=-1"
)
BSE_GOOGLE_URL = (
    "https://news.google.com/rss/search"
    "?q=BSE+corporate+announcement+results+dividend+site:bseindia.com"
    "&hl=en-IN&gl=IN&ceid=IN:en"
)

# NSE — API (needs session trick), then Google News fallback
NSE_API_URL = (
    "https://www.nseindia.com/api/corporateAnnouncementData"
    "?index=equities&from_date={week_ago}&to_date={today}"
)
NSE_GOOGLE_URL = (
    "https://news.google.com/rss/search"
    "?q=NSE+India+corporate+announcement+quarterly+results+dividend"
    "&hl=en-IN&gl=IN&ceid=IN:en"
)

# ── TRENDING TAXONOMY ─────────────────────────────────────────────────────────
TRENDING_TERMS = {
    "nifty 50","nifty","sensex","nifty bank","nifty it","nifty midcap",
    "nifty smallcap","nifty fmcg","nifty pharma","nifty metal","nifty realty",
    "rbi","sebi","mpc","monetary policy","repo rate","fpi","fii","dii",
    "inflation","cpi","wpi","gst","budget","fiscal deficit",
    "gdp","current account","trade deficit","rupee","usd inr","forex reserve",
    "fed","federal reserve","fomc","ecb","us treasury","10y yield",
    "dollar index","crude oil","brent","wti","opec","natural gas",
    "gold","silver","copper","china pmi","us pmi",
    "it sector","pharma","banking sector","psu bank","fmcg","auto sector",
    "metal sector","realty","infrastructure","defence","power sector",
    "reliance","hdfc","icici","sbi","tcs","infosys","wipro","hcl tech",
    "adani","tata","bajaj","maruti","ultratech","asian paints","kotak",
    "axis bank","sun pharma","dr reddy","cipla","zomato","paytm",
    "ipo","qip","buyback","dividend","bonus","merger","acquisition",
    "block deal","promoter pledge","insider trading","quarterly results",
    "earnings","guidance","ebitda","roce",
    "ukraine","middle east","opec cut","recession","rate cut","rate hike",
    "stagflation","yield curve",
}

BLOCKLIST = {
    "cricket","ipl","bollywood","film","actor","actress",
    "election","vote","weather","rain","flood","earthquake",
    "accident","crime","murder","sports","football","tennis",
    "icc","world cup","premier league","nba","nfl",
}

# ── HELPERS ───────────────────────────────────────────────────────────────────
def make_id(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()[:12]

def now_iso() -> str:
    return (datetime.now(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z"))

def fetch_url(url: str, extra: Optional[Dict] = None) -> Optional[bytes]:
    req = urllib.request.Request(url)
    for k, v in HEADERS.items():
        req.add_header(k, v)
    if extra:
        for k, v in extra.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as r:
            return r.read()
    except urllib.error.HTTPError as e:
        print(f"    HTTP {e.code}: {url[:70]}")
    except urllib.error.URLError as e:
        print(f"    URLError: {url[:70]} — {e.reason}")
    except Exception as e:
        print(f"    Error: {url[:70]} — {e}")
    return None

def parse_date(s: str) -> str:
    if not s:
        return now_iso()
    s = (s.strip()
         .replace(" IST", " +0530")
         .replace(" EST", " -0500")
         .replace(" PST", " -0800")
         .replace(" EDT", " -0400"))
    for fmt in (
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S GMT",
        "%a, %d %b %Y %H:%M:%S +0000",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%d %b %Y %H:%M:%S %z",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return (dt.astimezone(timezone.utc)
                    .isoformat(timespec="seconds")
                    .replace("+00:00", "Z"))
        except ValueError:
            continue
    return now_iso()

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", s or "")).strip()

def load_existing(path: Path) -> Dict:
    if path.exists():
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"  Warning: could not read {path}: {e}")
    return {}

def merge(existing: List[Dict], fresh: List[Dict], cap: int = MAX_ITEMS) -> List[Dict]:
    seen = {x["id"] for x in existing}
    combined = [x for x in fresh if x["id"] not in seen] + existing
    try:
        combined.sort(key=lambda x: x.get("published_at", ""), reverse=True)
    except Exception:
        pass
    return combined[:cap]

# ── SEARCH TERMS ──────────────────────────────────────────────────────────────
def load_terms() -> List[str]:
    path = DATA_DIR / "search_terms.json"
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            terms = data.get("terms", [])
            if terms:
                print(f"  Loaded {len(terms)} search terms from search_terms.json")
                return [t.lower() for t in terms]
        except Exception as e:
            print(f"  Could not read search_terms.json: {e}")
    defaults = [
        "nifty 50","sensex","rbi","fii","it sector","hdfc bank",
        "infosys","reliance","adani","sebi","crude oil","fed rate",
        "china pmi","dollar index","gold","budget","inflation","gdp",
        "midcap","smallcap",
    ]
    print(f"  Using {len(defaults)} default terms (no search_terms.json found)")
    return defaults

def get_matched(title: str, summary: str, terms: List[str]) -> List[str]:
    text = (title + " " + summary).lower()
    return [t for t in terms if t in text]

# ── RSS PARSER ────────────────────────────────────────────────────────────────
_NS = {
    "atom":    "http://www.w3.org/2005/Atom",
    "content": "http://purl.org/rss/1.0/modules/content/",
    "dc":      "http://purl.org/dc/elements/1.1/",
}

def _gtxt(entry: ET.Element, *tags: str) -> str:
    for tag in tags:
        # Direct child
        el = entry.find(tag)
        if el is not None:
            val = el.text or el.get("href", "")
            if val:
                return val.strip()
        # Namespaced
        for uri in _NS.values():
            local = tag.split(":")[-1]
            el = entry.find(f"{{{uri}}}{local}")
            if el is not None:
                val = el.text or el.get("href", "")
                if val:
                    return val.strip()
    return ""

def fetch_rss(name: str, urls: List[str], cat: str, terms: List[str]) -> List[Dict]:
    raw = None
    for url in urls:
        raw = fetch_url(url)
        if raw:
            break
    if not raw:
        print(f"    ✗ all URLs failed for {name}")
        return []
    try:
        text = raw.decode("utf-8", errors="replace").lstrip("\ufeff").strip()
        root = ET.fromstring(text)
    except ET.ParseError as e:
        print(f"    ✗ XML parse error for {name}: {e}")
        return []

    entries = root.findall(".//item") or root.findall(".//atom:entry", _NS)
    items: List[Dict] = []
    for e in entries:
        title = clean(_gtxt(e, "title"))
        link  = _gtxt(e, "link", "url", "id", "guid")
        pub   = _gtxt(e, "pubDate", "published", "updated", "dc:date")
        desc  = clean(_gtxt(e, "description", "summary", "content:encoded", "content"))
        if not title or not link:
            continue
        items.append({
            "id":            make_id(link),
            "title":         title[:350],
            "source":        name,
            "url":           link,
            "published_at":  parse_date(pub),
            "category":      cat,
            "matched_terms": get_matched(title, desc, terms),
            "summary":       desc[:300],
        })
    print(f"    ✓ {name}: {len(items)} items")
    return items

# ── BSE ───────────────────────────────────────────────────────────────────────
def fetch_bse() -> List[Dict]:
    items: List[Dict] = []

    # Try 1: BSE RSS feed
    raw = fetch_url(BSE_RSS_URL, {"Referer": "https://www.bseindia.com/"})
    if raw:
        try:
            root = ET.fromstring(raw.decode("utf-8", errors="replace").lstrip("\ufeff"))
            for e in root.findall(".//item"):
                title = clean(_gtxt(e, "title"))
                link  = _gtxt(e, "link", "guid")
                pub   = _gtxt(e, "pubDate")
                desc  = clean(_gtxt(e, "description"))
                if not title:
                    continue
                parts    = title.split(" - ", 1)
                company  = parts[0][:100]
                ann_type = parts[1][:80] if len(parts) > 1 else "Announcement"
                items.append({
                    "id": make_id(link or title),
                    "company": company,
                    "announcement_type": ann_type,
                    "detail": desc[:250],
                    "url": link or "https://www.bseindia.com",
                    "published_at": parse_date(pub),
                    "exchange": "BSE",
                })
            if items:
                print(f"    ✓ BSE RSS: {len(items)} items")
                return items
        except Exception as e:
            print(f"    ✗ BSE RSS: {e}")

    # Try 2: BSE JSON API
    today    = datetime.now(IST).strftime("%Y%m%d")
    week_ago = (datetime.now(IST) - timedelta(days=7)).strftime("%Y%m%d")
    raw = fetch_url(
        BSE_API_URL.format(prev=week_ago, today=today),
        {"Referer": "https://www.bseindia.com/", "Accept": "application/json"},
    )
    if raw:
        try:
            data  = json.loads(raw)
            table = data.get("Table", data.get("data", []))
            for row in table[:MAX_ITEMS]:
                company  = (row.get("SLONGNAME") or row.get("scrip_name") or "Unknown")[:100]
                ann_type = (row.get("CATEGORYNAME") or "Announcement")[:80]
                dt_str   = row.get("NEWS_DT") or ""
                att      = row.get("ATTACHMENTNAME", "")
                link     = (f"https://www.bseindia.com/xml-data/corpfiling/AttachHis/{att}"
                            if att else "https://www.bseindia.com")
                ts = now_iso()
                for fmt in ("%m/%d/%Y %I:%M:%S %p", "%Y-%m-%dT%H:%M:%S", "%d-%m-%Y %H:%M:%S"):
                    try:
                        dt = datetime.strptime(dt_str.strip(), fmt).replace(tzinfo=IST)
                        ts = (dt.astimezone(timezone.utc)
                              .isoformat(timespec="seconds")
                              .replace("+00:00", "Z"))
                        break
                    except (ValueError, AttributeError):
                        continue
                items.append({
                    "id": make_id(company + dt_str),
                    "company": company,
                    "announcement_type": ann_type,
                    "detail": (row.get("HEADLINE") or "")[:250],
                    "url": link,
                    "published_at": ts,
                    "exchange": "BSE",
                })
            if items:
                print(f"    ✓ BSE API: {len(items)} items")
                return items
        except Exception as e:
            print(f"    ✗ BSE API: {e}")

    # Try 3: Google News fallback
    print("    → BSE Google News fallback")
    for item in fetch_rss("BSE (Google)", [BSE_GOOGLE_URL], "Corporate", []):
        items.append({
            "id":                item["id"],
            "company":           item["title"][:100],
            "announcement_type": "Announcement",
            "detail":            item.get("summary", "")[:250],
            "url":               item["url"],
            "published_at":      item["published_at"],
            "exchange":          "BSE",
        })
    print(f"    ✓ BSE fallback: {len(items)} items")
    return items

# ── NSE ───────────────────────────────────────────────────────────────────────
def fetch_nse() -> List[Dict]:
    items: List[Dict] = []
    today    = datetime.now(IST).strftime("%d-%m-%Y")
    week_ago = (datetime.now(IST) - timedelta(days=7)).strftime("%d-%m-%Y")

    # NSE requires a session — do homepage ping first
    fetch_url("https://www.nseindia.com/", {"Upgrade-Insecure-Requests": "1"})

    raw = fetch_url(
        NSE_API_URL.format(week_ago=week_ago, today=today),
        {
            "Referer": "https://www.nseindia.com/",
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    if raw:
        try:
            data    = json.loads(raw)
            records = data if isinstance(data, list) else data.get("data", [])
            for row in records[:MAX_ITEMS]:
                company  = (row.get("symbol") or row.get("sm_name") or "Unknown")[:100]
                ann_type = (row.get("desc") or row.get("subject") or "Announcement")[:80]
                dt_str   = row.get("sort_date") or row.get("bm_timestamp") or ""
                ts = now_iso()
                for fmt in ("%Y-%m-%dT%H:%M:%S", "%d-%b-%Y %H:%M", "%d-%m-%Y %H:%M:%S"):
                    try:
                        dt = datetime.strptime(dt_str[:19].strip(), fmt).replace(tzinfo=IST)
                        ts = (dt.astimezone(timezone.utc)
                              .isoformat(timespec="seconds")
                              .replace("+00:00", "Z"))
                        break
                    except (ValueError, AttributeError):
                        continue
                url_link = row.get("attchmntFile") or ""
                if url_link and not url_link.startswith("http"):
                    url_link = "https://www.nseindia.com" + url_link
                items.append({
                    "id": make_id(company + dt_str),
                    "company": company,
                    "announcement_type": ann_type,
                    "detail": (row.get("attchmntText") or row.get("exc_dissem") or "")[:250],
                    "url": url_link or "https://www.nseindia.com",
                    "published_at": ts,
                    "exchange": "NSE",
                })
            if items:
                print(f"    ✓ NSE API: {len(items)} items")
                return items
        except Exception as e:
            print(f"    ✗ NSE API: {e}")

    # Fallback: Google News
    print("    → NSE Google News fallback")
    for item in fetch_rss("NSE (Google)", [NSE_GOOGLE_URL], "Corporate", []):
        items.append({
            "id":                item["id"],
            "company":           item["title"][:100],
            "announcement_type": "Announcement",
            "detail":            item.get("summary", "")[:250],
            "url":               item["url"],
            "published_at":      item["published_at"],
            "exchange":          "NSE",
        })
    print(f"    ✓ NSE fallback: {len(items)} items")
    return items

# ── TRENDING ──────────────────────────────────────────────────────────────────
def extract_trending(all_items: List[Dict]) -> List[Dict]:
    counter: Counter = Counter()
    for item in all_items:
        text = (item.get("title", "") + " " + item.get("summary", "")).lower()
        if any(b in text for b in BLOCKLIST):
            continue
        for term in TRENDING_TERMS:
            if term in text:
                counter[term] += 1
        for mt in item.get("matched_terms", []):
            if mt.lower() not in BLOCKLIST:
                counter[mt.lower()] += 1
    return [{"term": t.title(), "count": c} for t, c in counter.most_common(20)]

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main() -> None:
    sep = "=" * 60
    print(f"\n{sep}")
    print(f"BHARAT·INTEL Fetcher v2.1")
    print(f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M IST')}")
    print(f"{sep}\n")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    terms    = load_terms()
    existing = load_existing(DATA_DIR / "market_intel.json")

    print("Fetching India news…")
    new_india: List[Dict] = []
    for name, urls, cat in INDIA_RSS:
        print(f"  → {name}")
        new_india.extend(fetch_rss(name, urls, cat, terms))
        time.sleep(0.4)
    india_items = merge(existing.get("india_news", []), new_india)
    print(f"  India: {len(new_india)} new → {len(india_items)} stored\n")

    print("Fetching World news…")
    new_world: List[Dict] = []
    for name, urls, cat in WORLD_RSS:
        print(f"  → {name}")
        new_world.extend(fetch_rss(name, urls, cat, terms))
        time.sleep(0.4)
    world_items = merge(existing.get("world_news", []), new_world)
    print(f"  World: {len(new_world)} new → {len(world_items)} stored\n")

    print("Fetching BSE announcements…")
    bse_items = merge(existing.get("bse", []), fetch_bse())
    print(f"  BSE total stored: {len(bse_items)}\n")

    print("Fetching NSE announcements…")
    nse_items = merge(existing.get("nse", []), fetch_nse())
    print(f"  NSE total stored: {len(nse_items)}\n")

    print("Extracting trending…")
    trending = extract_trending(india_items + world_items)
    if trending:
        print(f"  Top: {', '.join(t['term'] for t in trending[:5])}\n")

    # Write market_intel.json
    intel_path = DATA_DIR / "market_intel.json"
    with open(intel_path, "w", encoding="utf-8") as f:
        json.dump({
            "meta": {
                "fetched_at": now_iso(),
                "version": 2,
                "item_counts": {
                    "india": len(india_items),
                    "world": len(world_items),
                    "bse":   len(bse_items),
                    "nse":   len(nse_items),
                },
                "search_terms_count": len(terms),
            },
            "india_news": india_items,
            "world_news":  world_items,
            "bse":          bse_items,
            "nse":          nse_items,
        }, f, ensure_ascii=False, separators=(",", ":"))
    print(f"✓ Wrote {intel_path}  ({intel_path.stat().st_size // 1024} KB)")

    # Write trending.json
    trend_path = DATA_DIR / "trending.json"
    with open(trend_path, "w", encoding="utf-8") as f:
        json.dump(
            {"meta": {"generated_at": now_iso()}, "trends": trending},
            f, ensure_ascii=False, separators=(",", ":"),
        )
    print(f"✓ Wrote {trend_path}  ({len(trending)} trends)")

    # Create default search_terms.json if absent
    terms_path = DATA_DIR / "search_terms.json"
    if not terms_path.exists():
        with open(terms_path, "w", encoding="utf-8") as f:
            json.dump({
                "version": 1,
                "updated_at": now_iso(),
                "terms": [t.title() for t in terms],
            }, f, indent=2, ensure_ascii=False)
        print(f"✓ Created {terms_path}")

    total = len(india_items) + len(world_items) + len(bse_items) + len(nse_items)
    print(f"\n{sep}")
    print(f"Done — {total} total items across all feeds")
    print(f"{sep}\n")


if __name__ == "__main__":
    main()
