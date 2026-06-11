"""
Personal Injury Attorney Lead Scraper (US)

Two-stage pipeline:
  Stage 1  Google Places API (New)  -> name, address, phone, website, rating (legal, structured)
  Stage 2  requests + BeautifulSoup -> email scraped directly from each firm's OWN website

Stage 2 fetches each firm's site (homepage + any "contact" pages) and pulls
emails from mailto: links and common email patterns. No external API key.

Output: pi_attorney_leads.csv  (HubSpot-import ready)

Setup:
  pip install -r requirements.txt

Provide keys via a local .env file (see .env.example) or environment variables:
  GOOGLE_MAPS_API_KEY=...   # enable "Places API (New)" in Google Cloud  (Stage 1)
"""

import os
import re
import csv
import time
import threading
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from dotenv import load_dotenv

# Load a local .env file (if present) so API keys can live outside the shell.
load_dotenv()

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
GMAPS_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")

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
# STAGE 2  -- requests + BeautifulSoup: scrape email from firm's own website
# ---------------------------------------------------------------------------
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
REQUEST_TIMEOUT = 12          # seconds per HTTP request
MAX_CONTACT_PAGES = 2         # extra "contact" pages to try if homepage has no email

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
# Junk we never want to treat as a real contact address.
BAD_EXTENSIONS = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".bmp",
                  ".css", ".js", ".ico")
BAD_DOMAINS = ("example.com", "example.org", "sentry.io", "sentry-next.wixpress.com",
               "wixpress.com", "wix.com", "godaddy.com", "squarespace.com",
               "domain.com", "yourdomain.com", "email.com", "schema.org",
               "sentry.wixpress.com", "googleusercontent.com")
# Role-based addresses a firm actually uses for intake, in order of preference.
PREFERRED_LOCALPARTS = ("intake", "newclients", "contact", "info", "office", "mail")


def _fetch(url):
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT},
                         timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if r.status_code == 200 and "text/html" in r.headers.get("Content-Type", ""):
            return r.text
    except requests.RequestException:
        return None
    return None


def _emails_from_html(html, soup):
    found = []
    # 1) mailto: links are the most reliable signal.
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().startswith("mailto:"):
            addr = href[7:].split("?")[0].strip()
            if addr:
                found.append(addr)
    # 2) plain-text + raw-HTML regex (catches addresses in script/attributes too).
    found += EMAIL_RE.findall(soup.get_text(" "))
    found += EMAIL_RE.findall(html)

    clean, seen = [], set()
    for e in found:
        e = e.strip().strip(".").lower()
        if not e or e in seen or e.endswith(BAD_EXTENSIONS):
            continue
        domain = e.rsplit("@", 1)[-1]
        if any(bad in domain for bad in BAD_DOMAINS):
            continue
        seen.add(e)
        clean.append(e)
    return clean


def _contact_links(soup, base):
    out, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = (a.get_text() or "").lower()
        if "contact" in href.lower() or "contact" in text:
            url = urljoin(base, href)
            if url not in seen:
                seen.add(url)
                out.append(url)
    return out


def _best_email(emails, site):
    if not emails:
        return ""
    site_domain = urlparse(site).netloc.lower().replace("www.", "")
    # Prefer an address on the firm's own domain.
    on_domain = [e for e in emails if site_domain and site_domain in e.rsplit("@", 1)[-1]]
    pool = on_domain or emails
    for part in PREFERRED_LOCALPARTS:
        for e in pool:
            if e.split("@", 1)[0] == part:
                return e
    return pool[0]


def _scrape_site(site):
    """Return the best email found on a firm's site (homepage, then contact pages)."""
    html = _fetch(site)
    if not html:
        return ""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    emails = _emails_from_html(html, soup)
    if not emails:
        for link in _contact_links(soup, site)[:MAX_CONTACT_PAGES]:
            chtml = _fetch(link)
            if not chtml:
                continue
            emails = _emails_from_html(chtml, BeautifulSoup(chtml, "html.parser"))
            if emails:
                break
    return _best_email(emails, site)


def enrich(firms, output=None, save_every=25):
    """Scrape an email for every firm that has a website, using requests +
    BeautifulSoup. Runs concurrently and (if `output` is given) flushes the CSV
    every `save_every` completions so a long run is resilient to interruption."""
    targets = [f for f in firms if f["website"]]
    print(f"[enrich] {len(targets)} firms with a website (of {len(firms)} total)")

    lock = threading.Lock()
    done = 0

    def _scrape(f):
        try:
            f["email"] = _scrape_site(f["website"])
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
