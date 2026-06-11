"""
Personal Injury / Car Accident Attorney Lead Scraper (US, state-level)

Two-stage pipeline:
  Stage 1  Google Places API (New)  -> firm, phone, website, address, rating (per state)
  Stage 2  Hunter.io domain-search  -> email + contact name from each firm's domain

Output: pi_attorney_leads_49states.csv  (HubSpot-import ready)
Columns: firm, contact_name, phone, email, website, address, state, metro, rating, reviews

Setup:
  pip install -r requirements.txt

Keys via a local .env file (see .env.example) or environment variables:
  GOOGLE_MAPS_API_KEY=...   # enable "Places API (New)" in Google Cloud   (Stage 1)
  HUNTER_API_KEY=...        # hunter.io dashboard key                      (Stage 2)
"""

import os
import csv
import time
import threading
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from dotenv import load_dotenv

# Load a local .env file (if present) so API keys can live outside the shell.
load_dotenv()

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
GMAPS_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
HUNTER_KEY = os.environ.get("HUNTER_API_KEY")

SEARCH_TERM = "personal injury car accident attorney"
RESULTS_PER_STATE = 50   # cap firms kept per state (Places returns up to 60)
OUTPUT = "pi_attorney_leads_49states.csv"
ENRICH_WORKERS = 8       # concurrent Hunter.io requests during Stage 2

# Remaining states (the 10 already scraped are excluded) + DC, each mapped to
# its largest city, used as the Places text-search center. 41 targets total.
STATE_TARGETS = {
    "AL": "Birmingham", "AK": "Anchorage", "AR": "Little Rock", "CO": "Denver",
    "CT": "Hartford", "DE": "Wilmington", "DC": "Washington", "HI": "Honolulu",
    "ID": "Boise", "IN": "Indianapolis", "IA": "Des Moines", "KS": "Wichita",
    "KY": "Louisville", "LA": "New Orleans", "ME": "Portland", "MD": "Baltimore",
    "MA": "Boston", "MI": "Detroit", "MN": "Minneapolis", "MS": "Jackson",
    "MO": "Kansas City", "MT": "Billings", "NE": "Omaha", "NH": "Manchester",
    "NJ": "Newark", "NM": "Albuquerque", "ND": "Fargo", "OH": "Columbus",
    "OK": "Oklahoma City", "OR": "Portland", "RI": "Providence", "SC": "Columbia",
    "SD": "Sioux Falls", "TN": "Nashville", "UT": "Salt Lake City", "VT": "Burlington",
    "VA": "Richmond", "WA": "Seattle", "WV": "Charleston", "WI": "Milwaukee",
    "WY": "Cheyenne",
}

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

HUNTER_URL = "https://api.hunter.io/v2/domain-search"

COLUMNS = ["firm", "contact_name", "phone", "email", "website",
           "address", "state", "metro", "rating", "reviews"]


def _require_key(name, val):
    if not val:
        raise SystemExit(
            f"Missing required env var: {name}. "
            "Set it in your shell or in a local .env file (see .env.example)."
        )


# ---------------------------------------------------------------------------
# STAGE 1  -- Google Places API (New): get firms per state
# ---------------------------------------------------------------------------
def fetch_firms(targets, term, per_state=RESULTS_PER_STATE):
    _require_key("GOOGLE_MAPS_API_KEY", GMAPS_KEY)
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GMAPS_KEY,
        "X-Goog-FieldMask": PLACES_FIELD_MASK,
    }
    firms = []
    for state, city in targets.items():
        metro = f"{city}, {state}"
        print(f"[places] {term} in {metro}")
        query = f"{term} in {city} {state}"
        body = {"textQuery": query}
        seen = set()
        kept = 0
        while kept < per_state:
            resp = requests.post(PLACES_SEARCH_URL, headers=headers, json=body, timeout=30)
            if resp.status_code != 200:
                print(f"[places] error {resp.status_code} for {metro}: {resp.text[:300]}")
                break
            data = resp.json()
            for r in data.get("places", []):
                if kept >= per_state:
                    break
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
                    "state": state,
                    "metro": metro,
                    "rating": r.get("rating", ""),
                    "reviews": r.get("userRatingCount", ""),
                })
                kept += 1
            token = data.get("nextPageToken")
            if not token or kept >= per_state:
                break
            time.sleep(2)  # brief delay before nextPageToken becomes valid
            body = {"textQuery": query, "pageToken": token}
    return firms


# ---------------------------------------------------------------------------
# STAGE 2  -- Hunter.io domain-search: email + contact name from firm's domain
# ---------------------------------------------------------------------------
def _domain(website):
    netloc = urlparse(website).netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return netloc


def enrich(firms, output=None, save_every=25):
    """Fill in email + contact_name for every firm that has a website, via the
    Hunter.io domain-search API. Runs concurrently and (if `output` is given)
    flushes the CSV every `save_every` completions so a long run is resilient."""
    _require_key("HUNTER_API_KEY", HUNTER_KEY)
    targets = [f for f in firms if f["website"]]
    print(f"[enrich] {len(targets)} firms with a website (of {len(firms)} total)")

    lock = threading.Lock()
    done = 0

    def _scrape(f):
        domain = _domain(f["website"])
        if not domain:
            return f
        try:
            r = requests.get(HUNTER_URL,
                             params={"domain": domain, "api_key": HUNTER_KEY},
                             timeout=30)
            if r.status_code != 200:
                print(f"[enrich] {r.status_code} for {domain}: {r.text[:150]}")
                return f
            data = r.json().get("data", {})
            emails = data.get("emails", [])
            if emails:
                e0 = emails[0]
                f["email"] = e0.get("value", "") or ""
                name = f"{e0.get('first_name') or ''} {e0.get('last_name') or ''}".strip()
                f["contact_name"] = name
        except Exception as e:
            print(f"[enrich] failed {f['firm']}: {e}")
        return f

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

    if output:
        write_csv(firms, output)
    return firms


# ---------------------------------------------------------------------------
def write_csv(firms, path):
    with open(path, "w", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(firms)


def main():
    firms = fetch_firms(STATE_TARGETS, SEARCH_TERM)
    print(f"\n{len(firms)} firms found across {len(STATE_TARGETS)} targets")

    if HUNTER_KEY:
        firms = enrich(firms, output=OUTPUT)

    write_csv(firms, OUTPUT)
    print(f"\nWrote {OUTPUT}")


if __name__ == "__main__":
    main()
