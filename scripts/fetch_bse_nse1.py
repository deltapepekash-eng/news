#!/usr/bin/env python3
"""
BSE/NSE data fetcher for GitHub Pages.
"""

import json, re, xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path

OUT     = Path(__file__).parent.parent / "data" / "bse_nse.json"
OUT.parent.mkdir(exist_ok=True)
NOW_UTC = datetime.now(timezone.utc)
NEWS_CUTOFF_MS = int((NOW_UTC - timedelta(days=7)).timestamp() * 1000)

bse_items:  list[dict] = []
news_items: list[dict] = []
bse_seen:   set[str]   = set()
news_seen:  set[str]   = set()
log:        list[str]  = []


def slug(s: str) -> str:
    return re.sub(r'[^a-z0-9]', '', (s or '').lower())[:60]


# 🔥 NEW: company extractor (ONLY for BSE)
def extract_company(title: str) -> str:
    if not title:
        return "Unknown"

    # BSE format: "SCRIP: Title"
    if ":" in title:
        first = title.split(":")[0].strip()
        if len(first) <= 40:
            return first

    return "Unknown"


def parse_dt(s: str):
    if not s:
        return None
    s = re.sub(r'\s+', ' ', s.strip())
    s = re.sub(r'\s+[+-]\d{4}$', '', s).strip()
    s = re.sub(r'\s+GMT$', '', s).strip()
    fmts = [
        '%m/%d/%Y %I:%M:%S %p',
        '%m/%d/%Y %I:%M %p',
        '%m/%d/%Y %H:%M:%S',
        '%m/%d/%Y',
        '%d %b %Y %I:%M:%S %p',
        '%d %b %Y %I:%M %p',
        '%d %b %Y %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d %H:%M:%S',
        '%a, %d %b %Y %H:%M:%S',
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


# ✅ MODIFIED: add company ONLY here
def add_bse(title: str, link: str, source: str, dt, ann_type=None):
    title = (title or '').strip()
    if not title or len(title) < 5:
        return
    k = slug(title)
    if k in bse_seen:
        return
    bse_seen.add(k)
    ts = to_ms(dt)

    company = extract_company(title)

    bse_items.append({
        "company": company,   # 🔥 NEW FIELD
        "title":  title,
        "link":   link or "#",
        "source": source,
        "ts":     ts,
        "type":   ann_type or classify(title),
    })


# ❌ UNCHANGED
def add_news(title: str, link: str, source: str, dt, ann_type=None):
    title = (title or '').strip()
    if not title or len(title) < 8:
        return
    k = slug(title)
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
