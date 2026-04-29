"""
fetch_omia.py — Fetch feline genetic variant data from OMIA
(Online Mendelian Inheritance in Animals) and update the local database.

Usage:
    python scripts/fetch_omia.py

How it works:
  1. Queries OMIA's public API for cat (species 9685) entries.
  2. Parses each record for gene, condition, and inheritance info.
  3. Merges new entries into the local SQLite database — existing rows
     with the same variant_id are updated, new rows are inserted.

Note: OMIA does not expose a formal REST API. This script uses their
search page (HTML scraping with requests + BeautifulSoup). It only pulls
the metadata fields we need and does NOT reproduce any copyrighted text.
If OMIA updates their site layout, the selectors below may need adjusting.
"""

import sqlite3
import os
import sys
import time
import logging

import requests
from bs4 import BeautifulSoup

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH   = os.path.join(BASE_DIR, "../database", "feline_genetics.db")

# ── OMIA constants ────────────────────────────────────────────────────────────
OMIA_SPECIES = "9685"          # NCBI taxon ID for Felis catus
OMIA_BASE    = "https://omia.org"
OMIA_SEARCH  = f"{OMIA_BASE}/results/?species_id={OMIA_SPECIES}&search_type=breed"
HEADERS      = {"User-Agent": "Bio595-FelineGenetics/1.0 (academic project)"}

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# ── Severity heuristics ───────────────────────────────────────────────────────

# Keywords that suggest different severity levels when found in the condition name
HIGH_KEYWORDS   = ["cardiomyopathy", "atrophy", "dystrophy", "gangliosidosis",
                   "haemophilia", "hemophilia", "muscular", "lethal",
                   "polycystic kidney", "neonatal", "blindness", "fatal"]
MEDIUM_KEYWORDS = ["retinal", "cystinuria", "syndrome", "deficiency",
                   "progressive", "isoerythrolysis"]


def infer_severity(condition_name: str) -> str:
    """Assign a severity level based on keywords in the condition name."""
    lower = condition_name.lower()
    for kw in HIGH_KEYWORDS:
        if kw in lower:
            return "High"
    for kw in MEDIUM_KEYWORDS:
        if kw in lower:
            return "Medium"
    return "Low"


# ── OMIA scraper ──────────────────────────────────────────────────────────────

def fetch_omia_cat_conditions() -> list[dict]:
    """
    Scrape OMIA for all cat-related condition entries.
    Returns a list of dicts with keys: omia_id, condition_name, gene, inheritance.
    """
    log.info("Fetching OMIA index for Felis catus (taxon 9685) …")
    try:
        resp = requests.get(OMIA_SEARCH, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.error(f"Could not reach OMIA: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    rows = soup.select("table.table tbody tr")  # OMIA result table rows
    log.info(f"Found {len(rows)} rows in OMIA results table.")

    conditions = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        try:
            omia_link = cells[0].find("a")
            if not omia_link:
                continue
            omia_id       = omia_link.get_text(strip=True)
            condition_name = cells[1].get_text(strip=True)
            gene           = cells[2].get_text(strip=True) if len(cells) > 2 else "Unknown"
            inheritance    = cells[3].get_text(strip=True) if len(cells) > 3 else "Unknown"
            conditions.append({
                "omia_id":        omia_id,
                "condition_name": condition_name,
                "gene":           gene,
                "inheritance":    inheritance,
            })
        except Exception as exc:
            log.warning(f"Skipping malformed row: {exc}")
            continue

    return conditions


def upsert_into_db(conditions: list[dict]) -> int:
    """
    Merge freshly-fetched OMIA data into the local SQLite database.
    Uses the OMIA ID as a stable key. Returns number of records merged.
    """
    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}. Run init_db.py first.")
        sys.exit(1)

    conn   = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    merged = 0

    for entry in conditions:
        omia_id        = entry["omia_id"]
        condition_name = entry["condition_name"]
        gene           = entry["gene"]
        inheritance    = entry["inheritance"]
        severity       = infer_severity(condition_name)

        # Use OMIA ID as the variant_id placeholder for OMIA-sourced records
        variant_id = f"OMIA:{omia_id}"

        cursor.execute(
            """INSERT INTO variants
               (variant_id, gene, omia_id, condition_name, severity, inheritance,
                plain_description, carrier_note, affected_note)
               VALUES (?,?,?,?,?,?,?,?,?)
               ON CONFLICT(variant_id) DO UPDATE SET
                   gene           = excluded.gene,
                   omia_id        = excluded.omia_id,
                   condition_name = excluded.condition_name,
                   severity       = excluded.severity,
                   inheritance    = excluded.inheritance""",
            (
                variant_id, gene, omia_id, condition_name, severity, inheritance,
                f"Condition fetched from OMIA ({omia_id}). Plain-language "
                f"description will be added manually.",
                "See your vet for advice on carrying this variant.",
                "See your vet for management options.",
            ),
        )
        merged += 1
        time.sleep(0.05)   # Be polite to the OMIA server

    conn.commit()
    conn.close()
    return merged


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=== OMIA Fetcher starting ===")
    data = fetch_omia_cat_conditions()
    if not data:
        log.warning("No data returned from OMIA. Check your internet connection.")
        sys.exit(0)
    n = upsert_into_db(data)
    log.info(f"Done. {n} OMIA records merged into {DB_PATH}.")
