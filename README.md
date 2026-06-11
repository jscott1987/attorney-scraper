# Personal Injury Attorney Lead Scraper (US)

A two-stage pipeline that builds a HubSpot-import-ready CSV of personal injury
law firms across major US metros.

| Stage | Source | What it pulls |
|-------|--------|---------------|
| 1 | Google Places API (New) | firm name, address, phone, website, rating, review count |
| 2 | requests + BeautifulSoup | email scraped directly from each firm's own website |

Stage 2 fetches each firm's homepage (and any "contact" pages) and extracts
emails from `mailto:` links and common email patterns. No external API key.
Note: `contact_name` is not populated by this method — it needs an LLM/NER pass.

Output: `pi_attorney_leads.csv`

## Setup

```bash
pip install -r requirements.txt
```

Provide your API keys one of two ways:

**Option A — `.env` file (recommended).** Copy the template and fill it in:

```bash
cp .env.example .env
# then edit .env with your real keys
```

The script auto-loads `.env` via `python-dotenv`. The file is gitignored, so it
will not be committed.

**Option B — shell environment variables:**

```bash
export GOOGLE_MAPS_API_KEY=...   # Stage 1: enable "Places API (New)" in Google Cloud
```

> Stage 2 needs outbound access to arbitrary firm websites. It will not work in
> a restricted-egress environment whose network policy only allowlists specific
> hosts — run it where general internet egress is allowed.

## Run

```bash
python pi_attorney_scraper.py
```

## Configuration

Edit the constants at the top of `pi_attorney_scraper.py`:

- `METROS` — metros to search. More metros = more coverage = more API cost.
- `SEARCH_TERM` — the Places query (default: `personal injury attorney`).
- `ENRICH_WITH_LLM` — set `False` to skip Stage 2 (no email scraping, lower cost).
- `ENRICH_WORKERS` — concurrent website fetches during Stage 2 (default 8).
- `OUTPUT` — output CSV filename.

## Output columns

`firm, contact_name, phone, email, website, address, metro, rating, reviews`

## Notes

- Places API (New) returns at most 60 results per text query (3 pages of 20), so
  each metro is capped at the top ~60 firms by Google's relevance ranking.
- Stage 2 cost scales with the number of firms that have a website. Disable it
  with `ENRICH_WITH_LLM = False` for a phone/website-only pull.
- Stage 2 runs requests concurrently and flushes the CSV every 25 completions,
  so a long run is resilient to interruption.
