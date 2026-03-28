# News Tracker — GitHub Actions Backend

This backend fetches BSE & NSE corporate announcements every 15 minutes
and saves them as a static JSON file served via GitHub Pages.
Your HTML app reads this JSON directly — bypassing all CORS restrictions.

## Setup (5 minutes)

### Step 1 — Add these files to your repo

Copy the following into your existing GitHub repo:

```
YOUR_REPO/
├── .github/
│   └── workflows/
│       └── fetch-bse-nse.yml   ← GitHub Actions workflow
├── scripts/
│   └── fetch_bse_nse.py        ← Python fetcher
├── data/
│   └── bse_nse.json            ← Auto-updated data file (seed provided)
└── news_tracker_mobile_v7.html ← Updated HTML (replace your old file)
```

### Step 2 — Enable GitHub Pages

1. Go to your repo → **Settings** → **Pages**
2. Under "Build and deployment":
   - Source: **Deploy from a branch**
   - Branch: `main` (or `master`), folder: `/ (root)`
3. Click **Save**
4. Your site will be at: `https://YOUR_USERNAME.github.io/YOUR_REPO/`

### Step 3 — Enable Actions permissions

1. Go to **Settings** → **Actions** → **General**
2. Under "Workflow permissions": select **Read and write permissions**
3. Click **Save**

### Step 4 — Update the URL in your HTML (if auto-detection fails)

Open `news_tracker_mobile_v7.html` and find this line near the top of the JS:

```js
return 'https://YOUR_USERNAME.github.io/YOUR_REPO/data/bse_nse.json';
```

Replace with your actual GitHub Pages URL, e.g.:

```js
return 'https://petejsmith.github.io/stock-tracker/data/bse_nse.json';
```

If you host the HTML at the root of your GitHub Pages site (same domain),
**auto-detection will work automatically** — no manual change needed.

### Step 5 — Trigger first run

Go to **Actions** → **Fetch BSE/NSE Data** → **Run workflow** → **Run workflow**

This runs the Python script immediately. You'll see `data/bse_nse.json` update
within ~60 seconds. After that, it runs automatically every 15 minutes.

---

## How it works

```
GitHub Actions (every 15 min)
       │
       ├── fetch_bse_nse.py runs on GitHub's servers
       │      ├── BSE Direct API (no CORS on server!)
       │      ├── Moneycontrol RSS
       │      ├── ET Markets RSS
       │      ├── LiveMint RSS
       │      ├── Business Standard RSS
       │      └── Google News RSS (NSE queries)
       │
       └── Writes data/bse_nse.json → commits → pushes
                    │
                    ▼
        GitHub Pages serves the JSON
                    │
                    ▼
        Your HTML fetches it directly
        (same domain = zero CORS issues)
```

## Freshness

- Market hours (09:15–16:30 IST, Mon–Fri): every **15 minutes**
- Off-hours / pre-market / post-market: every **30 minutes**
- Weekends: every **60 minutes**
- Data age is shown in the Exchange tab header: `⚡ refreshed 4m ago`

## Free tier limits

GitHub Actions free tier: **2,000 minutes/month**
This workflow uses ~30 seconds per run × ~2,880 runs/month ≈ **1,440 minutes**
— comfortably within the free limit.
