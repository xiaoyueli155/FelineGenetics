"""
init_db.py — Build and populate the feline genetics SQLite database.

Step 1 (Data Preparation) in the project plan:
    The source data is cat_genes.csv, exported from OMIA's Table S1
    (https://omia.org) and filtered to domestic cat entries only.

Step 2 (Database Creation):
    This script reads cat_genes.csv and loads all rows into the
    local SQLite database defined by schema.sql.

Run once before launching the Flask app:
    python database/init_db.py

CSV columns (from OMIA Table S1):
    Trait.disorder  → condition_name
    OMIA.number     → omia_id (formatted as OMIA-XXXXXX-9685)
    Symbol          → gene
    Name            → gene_full_name (stored in conditions table)
    Other.symbols   → additional gene aliases (informational)
    Other.descs     → additional descriptions (informational)

Notes:
  - variant_id is set to "OMIA:{number}" since OMIA Table S1 does not
    include individual HGVS variant notations. The variant_checker
    matches on this ID using the OMIA ID field in the VCF or by gene.
  - severity is inferred by keyword matching on the condition name,
    since OMIA does not provide a severity rating.
  - inheritance is left as "Unknown" — it is not included in Table S1.
    It can be manually added later by editing the CSV.
"""

import sqlite3
import csv
import os

DB_PATH     = os.path.join(os.path.dirname(__file__), "feline_genetics.db")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")
CSV_PATH    = os.path.join(os.path.dirname(__file__), "cat_genes.csv")


# ── Severity inference ────────────────────────────────────────────────────────
# Keywords in the condition name that suggest High or Medium severity.
# Anything not matching defaults to Low.

HIGH_KEYWORDS = [
    "cardiomyopathy", "dystrophy", "gangliosidosis", "haemophilia",
    "hemophilia", "muscular", "polycystic kidney", "atrophy",
    "lipofuscinosis", "leber", "retinal degeneration", "spinal muscular",
    "encephalopathy", "dysgenesis", "commissural", "epileptic",
    "mucolipidosis", "mucopolysaccharidosis", "niemann-pick",
    "glycogen storage", "mannosidosis", "leukocyte adhesion",
    "fibrodysplasia", "hyperoxaluria", "myotubular", "osteogenesis",
    "porphyria", "wilson disease", "chediak-higashi",
    "neuronal ceroid", "thrombasthenia", "factor xi", "factor xii",
    "glaucoma", "forebrain", "cerebral", "chondrodysplasia",
    "acyl-coa dehydrogenase", "pyknodysostosis", "epidermolysis bullosa",
    "frontonasal dysplasia", "skeletal dysplasia",
]

MEDIUM_KEYWORDS = [
    "retinal", "cystinuria", "syndrome", "deficiency", "progressive",
    "isoerythrolysis", "hypothyroidism", "hypercholesterolaemia",
    "hyperlipoproteinaemia", "myotonia", "hypokalaemic",
    "acrodermatitis", "dihydropyrimidinase", "xanthinuria",
    "methaemoglobinaemia", "vitamin d", "pyruvate kinase",
    "multidrug resistance", "autoimmune", "verrucous", "ehlers-danlos",
    "hypogonadotropic", "resistance/susceptibility",
    "ichthyosis", "hypotrichosis", "enteropathy",
]


def infer_severity(condition_name: str) -> str:
    """
    Assign High / Medium / Low severity based on keywords in the
    condition name. This is a best-effort heuristic — severity should
    ideally be reviewed by a veterinary professional.
    """
    lower = condition_name.lower()
    for kw in HIGH_KEYWORDS:
        if kw in lower:
            return "High"
    for kw in MEDIUM_KEYWORDS:
        if kw in lower:
            return "Medium"
    return "Low"


def build_plain_description(condition_name: str, gene: str, gene_full_name: str) -> str:
    """
    Build a simple plain-language description using the condition name
    and gene information. This is a template — for a production tool,
    each condition would have a hand-written description.
    """
    return (
        f"{condition_name} is a genetic condition in cats associated with "
        f"a variant in the {gene} gene ({gene_full_name}). "
        f"If this variant is detected, please consult your veterinarian "
        f"for further evaluation and monitoring advice."
    )


# ── CSV loader ────────────────────────────────────────────────────────────────

def load_csv() -> list[dict]:
    """
    Read cat_genes.csv and return a list of row dicts.
    Skips rows where Symbol is empty (no known gene).
    """
    if not os.path.isfile(CSV_PATH):
        raise FileNotFoundError(
            f"CSV file not found: {CSV_PATH}\n"
            "Please place cat_genes.csv in the database/ folder."
        )

    rows = []
    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip rows with no gene symbol — we cannot match these in VCFs
            if not row.get("Symbol", "").strip():
                continue
            rows.append(row)

    print(f"[init_db] CSV loaded: {len(rows)} rows with a known gene symbol.")
    return rows


# ── Database builder ──────────────────────────────────────────────────────────

def init_database():
    """
    Create database tables (from schema.sql) and populate them from
    cat_genes.csv. Safe to re-run — existing rows are not duplicated.
    """
    rows = load_csv()

    conn   = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Apply schema (CREATE TABLE IF NOT EXISTS — safe to re-run)
    with open(SCHEMA_PATH, "r") as f:
        cursor.executescript(f.read())

    variant_count   = 0
    condition_count = 0

    for row in rows:
        condition_name = row["Trait.disorder"].strip()
        omia_number    = row["OMIA.number"].strip()
        gene           = row["Symbol"].strip()
        gene_full_name = row["Name"].strip()

        # Build the OMIA ID in standard format
        omia_id = f"OMIA-{omia_number}-9685"

        # variant_id uses the OMIA ID as a stable unique key
        # (we do not have per-variant HGVS notations from Table S1)
        variant_id = f"OMIA:{omia_number}"

        severity          = infer_severity(condition_name)
        plain_description = build_plain_description(
            condition_name, gene, gene_full_name
        )
        omia_url = f"https://omia.org/OMIA{omia_number}/9685/"

        # ── Insert into variants ──────────────────────────────────────────
        cursor.execute(
            """INSERT OR IGNORE INTO variants
               (variant_id, gene, omia_id, condition_name, severity,
                inheritance, plain_description, carrier_note, affected_note)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                variant_id,
                gene,
                omia_id,
                condition_name,
                severity,
                "Unknown",   # Not provided in Table S1
                plain_description,
                f"Your cat carries one copy of a variant in {gene} associated "
                f"with {condition_name}. Please discuss with your vet.",
                f"Your cat may be affected by {condition_name} (variant in {gene}). "
                f"Veterinary evaluation is recommended.",
            ),
        )
        if cursor.rowcount:
            variant_count += 1

        # ── Insert into conditions ────────────────────────────────────────
        cursor.execute(
            """INSERT OR IGNORE INTO conditions
               (condition_name, full_description, symptoms, management, omia_url)
               VALUES (?,?,?,?,?)""",
            (
                condition_name,
                f"Gene: {gene} ({gene_full_name}). "
                f"OMIA reference: {omia_id}.",
                "See OMIA and your veterinarian for symptom details.",
                "Consult a veterinarian for management recommendations.",
                omia_url,
            ),
        )
        if cursor.rowcount:
            condition_count += 1

    conn.commit()
    conn.close()

    print(f"[init_db] Database ready at: {DB_PATH}")
    print(f"[init_db] New variants inserted:   {variant_count}")
    print(f"[init_db] New conditions inserted:  {condition_count}")
    print(f"[init_db] Total rows processed:     {len(rows)}")


if __name__ == "__main__":
    init_database()
