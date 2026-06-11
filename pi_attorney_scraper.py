"""
Personal Injury Attorney Lead Scraper (US)

Two-stage pipeline:
  Stage 1  Google Places API (New)  -> name, address, phone, website, rating (legal, structured)
  Stage 2  ScrapeGraphAI (hosted)   -> email + contact name from each firm's OWN website

Stage 2 uses ScrapeGraphAI's hosted API (scrapegraph-py), so it runs anywhere
with no local browser and no OpenAI key — the service handles fetching + the LLM.

Output: pi_attorney_leads.csv  (HubSpot-import ready)

Setup:
  pip install -r requirements.txt

Provide keys via a local .env file (see .env.example) or environment variables:
  GOOGLE_MAPS_API_KEY=...   # enable "Places API (New)" in Google Cloud  (Stage 1)
  SGAI_API_KEY=...          # ScrapeGraphAI dashboard key                 (Stage 2)
"""

import os
import csv
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from dotenv import load_dotenv

# Load a local .env file (if present) so API keys can live outside the shell.
load_dotenv()

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
GMAPS_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
SGAI_KEY = os.environ.get("SGAI_API_KEY")

# Add/remove metros. More metros = more coverage = more API cost.
METROS = [
    "Miami FL", "Fort Lauderdale FL", "Tampa FL", "Orlando FL", "Jacksonville FL",
    "Atlanta GA", "Houston TX", "Dallas TX", "Los Angeles CA", "New York NY",
    "Chicago IL", "Phoenix AZ", "Philadelphia PA", "Las Vegas NV", "Charlotte NC",
]

SEARCH_TERM = "personal injury attorney"
ENRICH_WITH_LLM = True   # set False to skip Stage 2 (no email scraping, lower cost)
OUTPUT = "pi_attorney_leads.csv"
ENRICH_WORKERS = 8       # concurrent hosted-API requests during Stage 2

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
# STAGE 2  -- ScrapeGraphAI (hosted): pull email + contact from firm's own site
# ---------------------------------------------------------------------------
ENRICH_PROMPT = ("Extract the primary contact email and a named attorney or intake "
                 "contact person. Return empty strings if not found.")


def enrich(firms, output=None, save_every=25):
    """Fill in email + contact_name for every firm that has a website, using
    ScrapeGraphAI's hosted API. Runs requests concurrently and (if `output` is
    given) flushes the CSV every `save_every` completions so a long run is
    resilient to interruption."""
    _require_key("SGAI_API_KEY", SGAI_KEY)
    # Imported lazily so Stage 1 doesn't depend on Stage 2's deps.
    from pydantic import BaseModel, Field
    from scrapegraph_py import Client

    class Contact(BaseModel):
        email: str = Field(default="", description="primary contact email address")
        contact_name: str = Field(default="", description="a named attorney or intake contact")

    targets = [f for f in firms if f["website"]]
    print(f"[enrich] {len(targets)} firms with a website (of {len(firms)} total)")

    client = Client(api_key=SGAI_KEY)
    lock = threading.Lock()
    done = 0

    def _scrape(f):
        try:
            resp = client.smartscraper(
                user_prompt=ENRICH_PROMPT,
                website_url=f["website"],
                output_schema=Contact,
            )
            result = resp.get("result", {}) if isinstance(resp, dict) else {}
            if isinstance(result, dict):
                f["email"] = result.get("email") or ""
                f["contact_name"] = result.get("contact_name") or ""
        except Exception as e:
            print(f"[enrich] failed {f['firm']}: {e}")
        return f

    try:
        with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as ex:
            futures = [ex.submit(_scrape, f) for f in targets]
            for fut in as_completed(futures):
                f = fut.result()
                with lock:
                    done += 1
                    print(f"[enrich] {done:4d}/{len(targets)}  "
                          f"{f['firm']:40.40}  {f['email']}")
                    if output and done % save_every == 0:
                        write_csv(firms, output)
    finally:
        client.close()

    if output:
        write_csv(firms, output)
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
        firms = enrich(firms, output=OUTPUT)

    write_csv(firms, OUTPUT)
    print(f"\nWrote {OUTPUT}")


if __name__ == "__main__":
    main()
