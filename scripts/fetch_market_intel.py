#!/usr/bin/env python3
"""
BHARAT·INTEL — Market Intelligence Fetcher
Fetches BSE/NSE filings, India & World news based on search terms.
Appends to rolling 200-item feeds. Writes market_intel.json and trending.json.
Run via GitHub Actions on a cron schedule.
"""

import json
import os
import re
import hashlib
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from xml.etree import ElementTree as ET
from collections import Counter
from pathlib import Path

# ── CONFIG ────────────────────────────────────────────────────────────────────
MAX_ITEMS = 200          # rolling cap per feed
FETCH_TIMEOUT = 15       # seconds per request
DATA_DIR = Path("data")

# IST timezone
IST = timezone(timedelta(hours=5, minutes=30))

# ── NEWS SOURCES ──────────────────────────────────────────────────────────────
INDIA_RSS = [
    ("Economic Times Markets", "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms", "Markets"),
    ("Economic Times Economy",  "https://economictimes.indiatimes.com/economy/rssfeeds/1373380680.cms", "Economy"),
    ("Mint Markets",            "https://www.livemint.com/rss/markets",                                  "Markets"),
    ("Mint Economy",            "https://www.livemint.com/rss/economy",                                  "Economy"),
    ("Business Standard Mkts",  "https://www.business-standard.com/rss/markets-106.rss",                 "Markets"),
    ("Business Standard Corp",  "https://www.business-standard.com/rss/companies-101.rss",               "Corporate"),
    ("Financial Express",       "https://www.financialexpress.com/market/feed/",                         "Markets"),
    ("NDTV Profit",             "https://www.ndtvprofit.com/rss?type=4",                                 "Markets"),
    ("MoneyControl News",       "https://www.moneycontrol.com/rss/marketreports.xml",                    "Markets"),
    ("Hindu Business Line",     "https://www.thehindubusinessline.com/markets/stock-markets/feeder/default.rss", "Markets"),
]

WORLD_RSS = [
    ("Reuters Business",        "https://feeds.reuters.com/reuters/businessNews",                         "Global Economy"),
    ("Reuters Markets",         "https://feeds.reuters.com/reuters/markets",                              "Global Markets"),
    ("CNBC World Economy",      "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258", "Global Economy"),
    ("Bloomberg Asia",          "https://feeds.bloomberg.com/markets/news.rss",                           "Global Markets"),
    ("FT Markets",              "https://www.ft.com/markets?format=rss",                                  "Global Markets"),
    ("Investing.com Commodities","https://www.investing.com/rss/news_25.rss",                             "Commodities"),
    ("Yahoo Finance",           "https://finance.yahoo.com/news/rssindex",                                "Global Economy"),
]

# BSE Announcements (public XML feed)
BSE_ANNOUNCEMENTS_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?strCat=-1&strPrevDate={}&strScrip=&strSearch=P&strToDate={}&strType=C&subcategory=-1"

# NSE Announcements
NSE_ANNOUNCEMENTS_URL = "https://www.nseindia.com/api/corporateAnnouncementData?index=equities&from_date={}&to_date={}"

# ── TRENDING TAXONOMY ─────────────────────────────────────────────────────────
# Only extract these categories for trending — suppresses political/sports noise
TRENDING_TERMS = {
    # Indices
    "nifty 50","nifty","sensex","nifty bank","nifty it","nifty midcap","nifty smallcap",
    "nifty fmcg","nifty pharma","nifty metal","nifty realty","nifty auto","nifty psu",
    # Regulators / Policy
    "rbi","sebi","mpc","monetary policy","repo rate","crr","slr","fpi","fii","dii",
    "omo","open market operation","inflation","cpi","wpi","iip","gst","budget","fiscal deficit",
    # Macro
    "gdp","current account","trade deficit","rupee","usd inr","inr","forex reserve",
    # Global drivers
    "fed","federal reserve","fomc","ecb","boj","pboc","us treasury","10y yield",
    "dollar index","dxy","crude oil","brent","wti","opec","natural gas",
    "gold","silver","copper","us cpi","us ppi","us gdp","china pmi","us pmi",
    # Sectors
    "it sector","tech sector","pharma","banking sector","psu bank","private bank",
    "fmcg","auto sector","metal sector","realty","infrastructure","defence","power sector",
    "renewables","ev","electric vehicle","semiconductor","chemicals",
    # Top companies (partial — extend as needed)
    "reliance","hdfc","icici","sbi","tcs","infosys","wipro","hcl tech","tech mahindra",
    "adani","tata","bajaj","maruti","l&t","ultratech","asian paints","hindustan unilever",
    "kotak","axis bank","sun pharma","dr reddy","cipla","divis","muthoot",
    "zomato","paytm","nykaa","delhivery","ola","ola electric",
    # Events
    "ipo","qip","buyback","dividend","bonus","rights issue","merger","acquisition","delisting",
    "block deal","bulk deal","promoter pledge","insider trading","quarterly results",
    "earnings","guidance","revenue","profit","ebitda","roce","roe",
    # Global events
    "ukraine","middle east","opec cut","oil cut","recession","rate cut","rate hike",
    "soft landing","hard landing","stagflation","yield curve",
}

BLOCKLIST = {
    "cricket","ipl","bollywood","film","actor","actress","politician","bjp","congress",
    "election","vote","weather","rain","flood","earthquake","accident","crime","murder",
    "sports","football","tennis","icc","world cup","premier league","nba","nfl",
}

# ── HELPERS ───────────────────────────────────────────────────────────────────
def make_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:12]

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def fetch_url(url: str, headers: dict = None) -> bytes | None:
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "Mozilla/5.0 BharatIntel/2.0 (+https://github.com)")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as resp:
            return resp.read()
    except Exception as e:
        print(f"  ✗ fetch {url[:60]}: {e}")
        return None

def parse_rss_date(s: str) -> str:
    """Parse RSS pubDate to ISO 8601 UTC string."""
    if not s:
        return now_iso()
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S GMT",
        "%a, %d %b %Y %H:%M:%S +0000",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
    ]
    s = s.strip()
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        except ValueError:
            continue
    return now_iso()

def load_existing(path: Path) -> dict:
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def merge_items(existing: list, new_items: list, max_items: int = MAX_ITEMS) -> list:
    """Prepend new items, dedup by id, cap at max_items."""
    seen = {x["id"] for x in existing}
    combined = []
    for item in new_items:
        if item["id"] not in seen:
            combined.append(item)
            seen.add(item["id"])
    combined.extend(existing)
    # Sort by published_at descending, cap
    try:
        combined.sort(key=lambda x: x.get("published_at", ""), reverse=True)
    except Exception:
        pass
    return combined[:max_items]

# ── SEARCH TERMS ──────────────────────────────────────────────────────────────
def load_search_terms() -> list[str]:
    path = DATA_DIR / "search_terms.json"
    if path.exists():
        try:
            with open(path) as f:
                data = json.load(f)
                terms = data.get("terms", [])
                if terms:
                    print(f"  Loaded {len(terms)} search terms from search_terms.json")
                    return [t.lower() for t in terms]
        except Exception as e:
            print(f"  Could not load search_terms.json: {e}")
    # Defaults if file not found
    defaults = [
        "nifty 50", "sensex", "rbi", "fii", "it sector", "hdfc bank",
        "infosys", "reliance", "adani", "sebi", "crude oil", "fed rate",
        "china pmi", "dollar index", "gold", "budget", "inflation", "gdp",
        "midcap", "smallcap",
    ]
    print(f"  Using {len(defaults)} default search terms")
    return defaults

def item_matches_terms(text: str, terms: list[str]) -> list[str]:
    """Return list of matched terms."""
    text_l = text.lower()
    return [t for t in terms if t in text_l]

# ── RSS FETCHER ───────────────────────────────────────────────────────────────
def fetch_rss_feed(source_name: str, url: str, category: str, terms: list[str]) -> list[dict]:
    raw = fetch_url(url)
    if not raw:
        return []
    items = []
    try:
        root = ET.fromstring(raw)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        # Handle both RSS and Atom
        entries = root.findall(".//item") or root.findall(".//atom:entry", ns)
        for entry in entries:
            def t(tag):
                el = entry.find(tag) or entry.find(f"atom:{tag}", ns)
                return (el.text or "").strip() if el is not None and el.text else ""
            title = t("title")
            link  = t("link") or t("url") or t("id")
            pub   = t("pubDate") or t("published") or t("updated")
            desc  = t("description") or t("summary") or t("content")
            if not title or not link:
                continue
            combined = f"{title} {desc}"
            matched = item_matches_terms(combined, terms)
            items.append({
                "id": make_id(link),
                "title": title[:300],
                "source": source_name,
                "url": link,
                "published_at": parse_rss_date(pub),
                "category": category,
                "matched_terms": matched,
                "summary": re.sub(r"<[^>]+>", "", desc)[:200] if desc else "",
            })
    except ET.ParseError as e:
        print(f"  ✗ XML parse {source_name}: {e}")
    return items

# ── BSE FETCHER ───────────────────────────────────────────────────────────────
def fetch_bse(terms: list[str]) -> list[dict]:
    today = datetime.now(IST).strftime("%Y%m%d")
    week_ago = (datetime.now(IST) - timedelta(days=7)).strftime("%Y%m%d")
    url = BSE_ANNOUNCEMENTS_URL.format(week_ago, today)
    raw = fetch_url(url, headers={"Accept": "application/json", "Referer": "https://www.bseindia.com"})
    items = []
    if raw:
        try:
            data = json.loads(raw)
            table = data.get("Table", data.get("data", []))
            for row in table[:MAX_ITEMS]:
                company = row.get("SLONGNAME", row.get("scrip_name", "Unknown"))
                ann_type = row.get("CATEGORYNAME", row.get("category", "Announcement"))
                dt_str   = row.get("NEWS_DT", row.get("dt", ""))
                sub_dt   = row.get("SLONGNAME", "")
                att_name = row.get("ATTACHMENTNAME", "")
                url_link = f"https://www.bseindia.com/xml-data/corpfiling/AttachHis/{att_name}" if att_name else "https://www.bseindia.com"
                # Parse BSE datetime format: M/D/YYYY H:MM:SS AM/PM
                ts = now_iso()
                if dt_str:
                    try:
                        for fmt in ["%m/%d/%Y %I:%M:%S %p", "%Y-%m-%dT%H:%M:%S", "%d-%m-%Y %H:%M:%S"]:
                            try:
                                dt = datetime.strptime(dt_str.strip(), fmt)
                                dt = dt.replace(tzinfo=IST)
                                ts = dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")
                                break
                            except ValueError:
                                continue
                    except Exception:
                        pass
                items.append({
                    "id": make_id(company + dt_str),
                    "company": company[:100],
                    "announcement_type": ann_type[:80],
                    "detail": row.get("HEADLINE", row.get("headline", ""))[:200],
                    "url": url_link,
                    "published_at": ts,
                    "exchange": "BSE",
                })
        except Exception as e:
            print(f"  ✗ BSE parse: {e}")
    return items

# ── NSE FETCHER ───────────────────────────────────────────────────────────────
def fetch_nse(terms: list[str]) -> list[dict]:
    today = datetime.now(IST).strftime("%d-%m-%Y")
    week_ago = (datetime.now(IST) - timedelta(days=7)).strftime("%d-%m-%Y")
    url = NSE_ANNOUNCEMENTS_URL.format(week_ago, today)
    raw = fetch_url(url, headers={
        "Accept": "application/json",
        "Referer": "https://www.nseindia.com",
        "Cookie": "AKA_A2=A",
    })
    items = []
    if raw:
        try:
            data = json.loads(raw)
            records = data if isinstance(data, list) else data.get("data", [])
            for row in records[:MAX_ITEMS]:
                company  = row.get("symbol", row.get("sm_name", "Unknown"))
                ann_type = row.get("desc", row.get("subject", "Announcement"))
                dt_str   = row.get("sort_date", row.get("bm_timestamp", ""))
                ts = now_iso()
                if dt_str:
                    try:
                        for fmt in ["%Y-%m-%dT%H:%M:%S", "%d-%b-%Y %H:%M", "%d-%m-%Y %H:%M:%S"]:
                            try:
                                dt = datetime.strptime(dt_str[:19].strip(), fmt)
                                dt = dt.replace(tzinfo=IST)
                                ts = dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")
                                break
                            except ValueError:
                                continue
                    except Exception:
                        pass
                url_link = row.get("attchmntFile","")
                if url_link and not url_link.startswith("http"):
                    url_link = "https://www.nseindia.com" + url_link
                items.append({
                    "id": make_id(company + dt_str),
                    "company": company[:100],
                    "announcement_type": ann_type[:80],
                    "detail": row.get("attchmntText", row.get("exc_dissem", ""))[:200],
                    "url": url_link or "https://www.nseindia.com",
                    "published_at": ts,
                    "exchange": "NSE",
                })
        except Exception as e:
            print(f"  ✗ NSE parse: {e}")
    return items

# ── TRENDING ──────────────────────────────────────────────────────────────────
def extract_trending(all_items: list[dict]) -> list[dict]:
    """Extract top 20 market-relevant trending terms from all feeds."""
    counter = Counter()
    for item in all_items:
        text = (item.get("title","") + " " + item.get("summary","")).lower()
        # Check curated terms
        for term in TRENDING_TERMS:
            if term in text:
                # Blocklist check
                if not any(b in text for b in BLOCKLIST):
                    counter[term] += 1
        # Also count matched_terms from search
        for mt in item.get("matched_terms", []):
            if mt.lower() not in BLOCKLIST:
                counter[mt.lower()] += 1
    # Title-case display
    top = [{"term": t.title(), "count": c} for t, c in counter.most_common(20)]
    return top

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*60}")
    print(f"BHARAT·INTEL Fetcher — {datetime.now(IST).strftime('%Y-%m-%d %H:%M IST')}")
    print(f"{'='*60}\n")

    DATA_DIR.mkdir(exist_ok=True)
    terms = load_search_terms()

    # Load existing data
    existing = load_existing(DATA_DIR / "market_intel.json")
    old_india = existing.get("india_news", [])
    old_world = existing.get("world_news", [])
    old_bse   = existing.get("bse", [])
    old_nse   = existing.get("nse", [])

    # ── India News ────────────────────────────────────────────────────────────
    print("Fetching India news RSS feeds…")
    new_india = []
    for name, url, cat in INDIA_RSS:
        print(f"  → {name}")
        new_india.extend(fetch_rss_feed(name, url, cat, terms))
        time.sleep(0.3)
    india_items = merge_items(old_india, new_india)
    print(f"  India news: {len(new_india)} new → {len(india_items)} total\n")

    # ── World News ────────────────────────────────────────────────────────────
    print("Fetching World news RSS feeds…")
    new_world = []
    for name, url, cat in WORLD_RSS:
        print(f"  → {name}")
        new_world.extend(fetch_rss_feed(name, url, cat, terms))
        time.sleep(0.3)
    world_items = merge_items(old_world, new_world)
    print(f"  World news: {len(new_world)} new → {len(world_items)} total\n")

    # ── BSE Filings ───────────────────────────────────────────────────────────
    print("Fetching BSE announcements…")
    new_bse = fetch_bse(terms)
    bse_items = merge_items(old_bse, new_bse)
    print(f"  BSE: {len(new_bse)} new → {len(bse_items)} total\n")

    # ── NSE Filings ───────────────────────────────────────────────────────────
    print("Fetching NSE announcements…")
    new_nse = fetch_nse(terms)
    nse_items = merge_items(old_nse, new_nse)
    print(f"  NSE: {len(new_nse)} new → {len(nse_items)} total\n")

    # ── Trending ──────────────────────────────────────────────────────────────
    print("Extracting trending topics…")
    all_items = india_items + world_items
    trending = extract_trending(all_items)
    print(f"  Top trends: {', '.join(t['term'] for t in trending[:5])} …\n")

    # ── Write market_intel.json ───────────────────────────────────────────────
    output = {
        "meta": {
            "fetched_at": now_iso(),
            "version": 2,
            "item_counts": {
                "india": len(india_items),
                "world": len(world_items),
                "bse": len(bse_items),
                "nse": len(nse_items),
            },
            "search_terms_count": len(terms),
        },
        "india_news": india_items,
        "world_news": world_items,
        "bse": bse_items,
        "nse": nse_items,
    }
    out_path = DATA_DIR / "market_intel.json"
    with open(out_path, "w") as f:
        json.dump(output, f, separators=(",", ":"))
    size_kb = out_path.stat().st_size // 1024
    print(f"✓ Wrote {out_path} ({size_kb} KB)")

    # ── Write trending.json ───────────────────────────────────────────────────
    trend_out = {
        "meta": {"generated_at": now_iso()},
        "trends": trending,
    }
    trend_path = DATA_DIR / "trending.json"
    with open(trend_path, "w") as f:
        json.dump(trend_out, f, separators=(",", ":"))
    print(f"✓ Wrote {trend_path} ({len(trending)} trends)")

    # ── Write default search_terms.json if missing ────────────────────────────
    terms_path = DATA_DIR / "search_terms.json"
    if not terms_path.exists():
        defaults = {
            "version": 1,
            "updated_at": now_iso(),
            "terms": [t.title() for t in terms[:20]],
        }
        with open(terms_path, "w") as f:
            json.dump(defaults, f, indent=2)
        print(f"✓ Created default {terms_path}")

    print(f"\n{'='*60}")
    print("Done ✓")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
