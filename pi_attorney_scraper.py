"""
Personal Injury Attorney Lead Scraper (US)

Two-stage pipeline:
  Stage 1  Google Places API (New)  -> name, address, phone, website, rating (legal, structured)
  Stage 2  ScrapeGraphAI            -> email + contact name from each firm's OWN website

Output: pi_attorney_leads.csv  (HubSpot-import ready)

Setup:
  pip install -r requirements.txt
  playwright install            # only needed if you run ScrapeGraphAI locally (Stage 2)

Provide keys via a local .env file (see .env.example) or environment variables:
  GOOGLE_MAPS_API_KEY=...   # enable "Places API (New)" in Google Cloud
  OPENAI_API_KEY=...        # or swap the model in graph_config (Stage 2 only)
"""

import os
import csv
import time

import requests
from dotenv import load_dotenv

# Load a local .env file (if present) so API keys can live outside the shell.
load_dotenv()

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
GMAPS_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
LLM_KEY = os.environ.get("OPENAI_API_KEY")

# Add/remove metros. More metros = more coverage = more API cost.
METROS = [
    "Miami FL", "Fort Lauderdale FL", "Tampa FL", "Orlando FL", "Jacksonville FL",
    "Atlanta GA", "Houston TX", "Dallas TX", "Los Angeles CA", "New York NY",
    "Chicago IL", "Phoenix AZ", "Philadelphia PA", "Las Vegas NV", "Charlotte NC",
]

SEARCH_TERM = "personal injury attorney"
ENRICH_WITH_LLM = True   # set False to skip Stage 2 (no email scraping, lower cost)
OUTPUT = "pi_attorney_leads.csv"

# Places API (New) endpoint + the fields we want back (field mask is required).
PLACES_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
PLACES_FIELD_MASK = ",".join([
    "places.id",
    "places.displayName",
    "places.formattedAddress",
    "places.nationalPhoneNumber",
    "places.websiteUri",
    "places.rating",
    "places.userRatingCount",
    "nextPageToken",
])

graph_config = {
    "llm": {"api_key": LLM_KEY, "model": "openai/gpt-4o-mini"},
    "verbose": False,
    "headless": True,
}


def _require_key(name, val):
    if not val:
        raise SystemExit(
            f"Missing required env var: {name}. "
            "Set it in your shell or in a local .env file (see .env.example)."
        )


# ---------------------------------------------------------------------------
# STAGE 1  -- Google Places API (New): get firms + phone + website
# ---------------------------------------------------------------------------
def fetch_firms(metros, term):
    _require_key("GOOGLE_MAPS_API_KEY", GMAPS_KEY)
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GMAPS_KEY,
        "X-Goog-FieldMask": PLACES_FIELD_MASK,
    }
    seen = set()
    firms = []
    for metro in metros:
        print(f"[places] {term} in {metro}")
        body = {"textQuery": f"{term} in {metro}"}
        while True:
            resp = requests.post(PLACES_SEARCH_URL, headers=headers, json=body, timeout=30)
            if resp.status_code != 200:
                print(f"[places] error {resp.status_code} for {metro}: {resp.text[:300]}")
                break
            data = resp.json()
            for r in data.get("places", []):
                pid = r.get("id")
                if not pid or pid in seen:
                    continue
                seen.add(pid)
                firms.append({
                    "firm": (r.get("displayName") or {}).get("text", ""),
                    "contact_name": "",
                    "phone": r.get("nationalPhoneNumber", ""),
                    "email": "",
                    "website": r.get("websiteUri", ""),
                    "address": r.get("formattedAddress", ""),
                    "metro": metro,
                    "rating": r.get("rating", ""),
                    "reviews": r.get("userRatingCount", ""),
                })
            token = data.get("nextPageToken")
            if not token:
                break
            time.sleep(2)  # brief delay before nextPageToken becomes valid
            body = {"textQuery": f"{term} in {metro}", "pageToken": token}
    return firms


# ---------------------------------------------------------------------------
# STAGE 2  -- ScrapeGraphAI: pull email + contact from firm's own site
# ---------------------------------------------------------------------------
def enrich(firms):
    _require_key("OPENAI_API_KEY", LLM_KEY)
    # Imported lazily so Stage 1 doesn't depend on Stage 2's (heavier) deps.
    from pydantic import BaseModel, Field
    from scrapegraphai.graphs import SmartScraperGraph

    class Contact(BaseModel):
        email: str = Field(default="", description="primary contact email address")
        contact_name: str = Field(default="", description="a named attorney or intake contact")

    for f in firms:
        site = f["website"]
        if not site:
            continue
        try:
            scraper = SmartScraperGraph(
                prompt="Extract the primary contact email and a named attorney or "
                       "intake contact person. Return empty strings if not found.",
                source=site,
                config=graph_config,
                schema=Contact,
            )
            out = scraper.run()
            f["email"] = out.get("email", "") if isinstance(out, dict) else ""
            f["contact_name"] = out.get("contact_name", "") if isinstance(out, dict) else ""
            print(f"[enrich] {f['firm']:40.40}  {f['email']}")
        except Exception as e:
            print(f"[enrich] failed {f['firm']}: {e}")
    return firms


# ---------------------------------------------------------------------------
def write_csv(firms, path):
    cols = ["firm", "contact_name", "phone", "email", "website",
            "address", "metro", "rating", "reviews"]
    with open(path, "w", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=cols)
        w.writeheader()
        w.writerows(firms)


def main():
    firms = fetch_firms(METROS, SEARCH_TERM)
    print(f"\n{len(firms)} unique firms found")

    if ENRICH_WITH_LLM:
        firms = enrich(firms)

    write_csv(firms, OUTPUT)
    print(f"\nWrote {OUTPUT}")


if __name__ == "__main__":
    main()
