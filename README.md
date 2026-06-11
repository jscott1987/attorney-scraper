# Personal Injury Attorney Lead Scraper (US)

A two-stage pipeline that builds a HubSpot-import-ready CSV of personal injury
law firms across major US metros.

| Stage | Source | What it pulls |
|-------|--------|---------------|
| 1 | Google Places API | firm name, address, phone, website, rating, review count |
| 2 | [ScrapeGraphAI](https://github.com/ScrapeGraphAI/Scrapegraph-ai) | primary email + contact name from each firm's own website |

Output: `pi_attorney_leads.csv`

## Setup

```bash
pip install -r requirements.txt
playwright install            # only needed if you run ScrapeGraphAI locally

export GOOGLE_MAPS_API_KEY=...   # enable "Places API (New)" in Google Cloud
export OPENAI_API_KEY=...        # or swap the model in graph_config
```

## Run

```bash
python pi_attorney_scraper.py
```

## Configuration

Edit the constants at the top of `pi_attorney_scraper.py`:

- `METROS` — metros to search. More metros = more coverage = more API cost.
- `SEARCH_TERM` — the Places query (default: `personal injury attorney`).
- `ENRICH_WITH_LLM` — set `False` to skip Stage 2 (no email scraping, lower cost).
- `OUTPUT` — output CSV filename.

## Output columns

`firm, contact_name, phone, email, website, address, metro, rating, reviews`

## Notes

- Google requires a short delay before a `next_page_token` becomes valid; the
  script sleeps 2s between pages.
- Stage 2 cost scales with the number of firms that have a website. Disable it
  with `ENRICH_WITH_LLM = False` for a phone/website-only pull.
