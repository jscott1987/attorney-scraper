"""
Personal Injury Attorney Lead Scraper (US)

Two-stage pipeline:
  Stage 1  Google Places API  -> name, address, phone, website, rating (legal, structured)
  Stage 2  ScrapeGraphAI      -> email + contact name from each firm's OWN website

Output: pi_attorney_leads.csv  (HubSpot-import ready)

Setup:
  pip install -r requirements.txt
  playwright install            # only needed if you run ScrapeGraphAI locally
  export GOOGLE_MAPS_API_KEY=...   # enable "Places API (New)" in Google Cloud
  export OPENAI_API_KEY=...        # or swap model in graph_config below
"""

import os
import csv
import time

import googlemaps
from pydantic import BaseModel, Field
from scrapegraphai.graphs import SmartScraperGraph

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
GMAPS_KEY = os.environ["GOOGLE_MAPS_API_KEY"]
LLM_KEY = os.environ["OPENAI_API_KEY"]

# Add/remove metros. More metros = more coverage = more API cost.
METROS = [
    "Miami FL", "Fort Lauderdale FL", "Tampa FL", "Orlando FL", "Jacksonville FL",
    "Atlanta GA", "Houston TX", "Dallas TX", "Los Angeles CA", "New York NY",
    "Chicago IL", "Phoenix AZ", "Philadelphia PA", "Las Vegas NV", "Charlotte NC",
]

SEARCH_TERM = "personal injury attorney"
ENRICH_WITH_LLM = True   # set False to skip Stage 2 (no email scraping, lower cost)
OUTPUT = "pi_attorney_leads.csv"

graph_config = {
    "llm": {"api_key": LLM_KEY, "model": "openai/gpt-4o-mini"},
    "verbose": False,
    "headless": True,
}


# ---------------------------------------------------------------------------
# STAGE 1  -- Google Places: get firms + phone + website
# ---------------------------------------------------------------------------
def fetch_firms(gmaps, metros, term):
    seen = set()
    firms = []
    for metro in metros:
        print(f"[places] {term} in {metro}")
        resp = gmaps.places(query=f"{term} in {metro}")
        page = resp
        while True:
            for r in page.get("results", []):
                pid = r["place_id"]
                if pid in seen:
                    continue
                seen.add(pid)
                d = gmaps.place(
                    place_id=pid,
                    fields=["name", "formatted_address", "formatted_phone_number",
                            "website", "rating", "user_ratings_total"],
                ).get("result", {})
                firms.append({
                    "firm": d.get("name", ""),
                    "address": d.get("formatted_address", ""),
                    "phone": d.get("formatted_phone_number", ""),
                    "website": d.get("website", ""),
                    "rating": d.get("rating", ""),
                    "reviews": d.get("user_ratings_total", ""),
                    "metro": metro,
                    "email": "",
                    "contact_name": "",
                })
            token = page.get("next_page_token")
            if not token:
                break
            time.sleep(2)  # Google requires a short delay before next_page_token is valid
            page = gmaps.places(query=f"{term} in {metro}", page_token=token)
    return firms


# ---------------------------------------------------------------------------
# STAGE 2  -- ScrapeGraphAI: pull email + contact from firm's own site
# ---------------------------------------------------------------------------
class Contact(BaseModel):
    email: str = Field(default="", description="primary contact email address")
    contact_name: str = Field(default="", description="a named attorney or intake contact")


def enrich(firms):
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
def main():
    gmaps = googlemaps.Client(key=GMAPS_KEY)
    firms = fetch_firms(gmaps, METROS, SEARCH_TERM)
    print(f"\n{len(firms)} unique firms found")

    if ENRICH_WITH_LLM:
        firms = enrich(firms)

    cols = ["firm", "contact_name", "phone", "email", "website",
            "address", "metro", "rating", "reviews"]
    with open(OUTPUT, "w", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=cols)
        w.writeheader()
        w.writerows(firms)
    print(f"\nWrote {OUTPUT}")


if __name__ == "__main__":
    main()
