# Property Bot 🏠

Automated London rental property bot. Scrapes Rightmove, Zoopla, OpenRent, and OnTheMarket daily across 12 central London areas, filters by walk time to tube (≤12 min), and checks asking price against FMV (let-agreed comparables). Sends passing listings to Telegram every day at 1pm BST.

---

## What it does

1. **Scrapes** ~1,800 active furnished 2-bed+ listings (≤£15,000/mo) across 12 areas within 0.5 miles of each tube station
2. **Filters** by walk time to nearest tube (Google Maps Distance Matrix)
3. **Skips duplicates** — properties already sent in the last 30 days are ignored
4. **FMV check** — compares asking price against recent let-agreed comparables from Rightmove, Zoopla, and OnTheMarket. Only passes listings asking ≤ FMV + £500
5. **Sends** passing listings to Telegram with price, FMV verdict, walk time, and listing link
6. **Runs daily** at 1pm BST via GitHub Actions (no server needed)

---

## Areas covered

Covent Garden, Soho, Knightsbridge, West Kensington, London Bridge, Tower Hill, Baker Street, Bond Street, Marble Arch, Oxford Circus, Marylebone, Regent's Park

---

## Project structure

```
property-bot/
├── .github/
│   └── workflows/
│       └── daily_run.yml       # GitHub Actions — runs at 1pm BST daily
├── scrapers/
│   ├── rightmove.py            # Rightmove scraper
│   ├── zoopla.py               # Zoopla scraper
│   ├── openrent.py             # OpenRent scraper
│   └── onthemarket.py          # OnTheMarket scraper
├── utils/
│   ├── valuation.py            # FMV calculation using let-agreed comparables
│   ├── seen_listings.py        # Tracks sent listings (30-day dedup window)
│   ├── walk_time.py            # Google Maps walk-time lookup
│   └── voa_data.py             # VOA council tax data helper
├── filter_bot.py               # Telegram bot — applies filters & sends results
├── research_bot.py             # Telegram bot — triggers scraping
├── scheduler.py                # Orchestrates scrape → filter pipeline
├── requirements.txt            # Python dependencies
├── .env.example                # Template for required environment variables
└── seen_listings.json          # Auto-updated by GitHub Actions (tracks sent listings)
```

---

## Setup from scratch

### 1. Clone the repo

```bash
git clone https://github.com/hypebearsnfts-claude/property-bot.git
cd property-bot
```

### 2. Create a virtual environment and install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

### 3. Set up environment variables

```bash
cp .env.example .env
```

Edit `.env` and fill in:

| Variable | How to get it |
|---|---|
| `TELEGRAM_BOT_TOKEN` | Message @BotFather on Telegram → `/newbot` |
| `TELEGRAM_RESEARCH_CHAT_ID` / `TELEGRAM_FILTER_CHAT_ID` | Message @userinfobot on Telegram |
| `GOOGLE_MAPS_API_KEY` | [Google Cloud Console](https://console.cloud.google.com) → enable Distance Matrix API |
| `ANTHROPIC_API_KEY` | [Anthropic Console](https://console.anthropic.com) |

Set all three `TELEGRAM_*_TOKEN` variables to the same bot token.

### 4. Add secrets to GitHub Actions

Go to your repo → **Settings → Secrets and variables → Actions → New repository secret** and add:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_RESEARCH_BOT_TOKEN`
- `TELEGRAM_FILTER_BOT_TOKEN`
- `TELEGRAM_RESEARCH_CHAT_ID`
- `TELEGRAM_FILTER_CHAT_ID`
- `GOOGLE_MAPS_API_KEY`
- `ANTHROPIC_API_KEY`

### 5. Enable GitHub Actions write permissions

Go to repo → **Settings → Actions → General → Workflow permissions** → select **Read and write permissions**. This lets the workflow update `seen_listings.json` after each run.

### 6. Test locally (optional)

```bash
source .venv/bin/activate
python scheduler.py
```

---

## Automated schedule

The bot runs daily via `.github/workflows/daily_run.yml`:

- **Cron**: `0 12 * * *` = 12:00 UTC = **1:00pm BST** = **8:00pm SGT**
- No server needed — GitHub Actions handles execution for free

---

## Filters applied

| Filter | Value |
|---|---|
| Bedrooms | 2+ |
| Furnished | Yes |
| Max rent | £15,000/mo |
| Search radius | 0.5 miles per area |
| Walk to tube | ≤ 12 minutes |
| Asking vs FMV | ≤ FMV + £500 |
| Dedup window | 30 days |

---

## Telegram summary message

Each daily run sends a breakdown:

```
✅ Done.
• Scraped today: 1,791
• Already sent (skipped): 1,263
• New listings checked for FMV: 493
• Passed FMV & sent: 102
```

---

## Re-creating the bot on a new account

1. Clone the repo (all code is here)
2. Create a new Telegram bot via @BotFather
3. Add secrets to GitHub (step 4 above)
4. Clear `seen_listings.json` if you want a fresh start: `echo '{}' > seen_listings.json && git add seen_listings.json && git commit -m "reset seen listings" && git push`
