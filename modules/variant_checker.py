"""
variant_checker.py — Match parsed VCF variants against the feline genetics DB.

Matching strategy (tried in order for each variant):
  1. Direct variant_id match  — works if the VCF ID column contains an
     HGVS notation (e.g. "c.10063C>A") or an OMIA key (e.g. "OMIA:807").
  2. Gene symbol match        — uses the INFO or ID field parsed from the
     VCF to look up the gene name in the database.
  3. Positional key match     — CHROM:POS_REF>ALT fallback for unannotated VCFs.

Because our database (built from OMIA Table S1) stores variant_ids as
"OMIA:{number}", strategy #2 (gene symbol) is the most commonly used path
for real VCF files that annotate the gene name in the INFO field.
"""

import sqlite3
import logging
import os
import re
from dataclasses import dataclass, field

from modules.vcf_parser import VcfVariant, generate_lookup_keys

log = logging.getLogger(__name__)

DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "database", "feline_genetics.db"
)


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class Finding:
    """A single matched variant with its health interpretation."""
    gene:              str
    condition_name:    str
    severity:          str
    inheritance:       str
    status:            str
    plain_description: str
    message:           str
    at_risk_breeds:    list[str] = field(default_factory=list)
    condition_details: dict      = field(default_factory=dict)


@dataclass
class HealthReport:
    """Full report for one cat, grouped by severity."""
    cat_name:               str
    cat_gender:             str
    cat_diet:               str
    total_variants_checked: int
    high:            list[Finding] = field(default_factory=list)
    medium:          list[Finding] = field(default_factory=list)
    low:             list[Finding] = field(default_factory=list)
    unmatched_count: int = 0

    @property
    def all_findings(self) -> list[Finding]:
        return self.high + self.medium + self.low

    @property
    def has_findings(self) -> bool:
        return bool(self.all_findings)


# ── Status logic ──────────────────────────────────────────────────────────────

def determine_status(zygosity: str, inheritance: str) -> str:
    """
    Combine zygosity + inheritance mode → Carrier / Affected / Uncertain.

    Rules:
      Autosomal Dominant  : one copy (het) → Affected
      Autosomal Recessive : two copies (hom_alt) → Affected; one copy → Carrier
      X-linked Recessive  : hom_alt → Affected; het → Carrier
      Unknown inheritance : hom_alt → Affected; het → Carrier (conservative)
    """
    z = zygosity
    inh = (inheritance or "").lower()

    if z == "homozygous_alt":
        return "Affected"

    if "dominant" in inh:
        if z in ("heterozygous", "homozygous_alt"):
            return "Affected"

    if "recessive" in inh or inh == "unknown":
        if z == "heterozygous":
            return "Carrier"

    return "Uncertain"


# ── Gene symbol extraction from VCF INFO field ───────────────────────────────

def extract_gene_from_info(info: str) -> str | None:
    """
    Try to pull a gene symbol from the VCF INFO field.
    Common annotations include:
      GENEINFO=PKD1:5310   (ClinVar-style)
      ANN=...|PKD1|...     (SnpEff-style)
      GENE=PKD1            (generic)
    Returns the gene symbol string, or None if not found.
    """
    patterns = [
        r"GENEINFO=([A-Z0-9_]+)",   # ClinVar
        r"GENE=([A-Z0-9_]+)",       # Generic
        r"\|([A-Z][A-Z0-9_]{1,})\|", # SnpEff ANN field (heuristic)
    ]
    for pat in patterns:
        m = re.search(pat, info or "")
        if m:
            return m.group(1)
    return None


# ── Database queries ──────────────────────────────────────────────────────────

def _query_by_variant_id(cursor: sqlite3.Cursor, key: str) -> dict | None:
    cursor.execute(
        """SELECT variant_id, gene, condition_name, severity, inheritance,
                  plain_description, carrier_note, affected_note
           FROM variants WHERE variant_id = ?""",
        (key,)
    )
    row = cursor.fetchone()
    if row:
        cols = ["variant_id","gene","condition_name","severity","inheritance",
                "plain_description","carrier_note","affected_note"]
        return dict(zip(cols, row))
    return None


def _query_by_gene(cursor: sqlite3.Cursor, gene: str) -> list[dict]:
    """
    Return ALL conditions associated with a gene symbol.
    One gene can cause multiple conditions (e.g. KIT → white spotting,
    dominant white, white feet).
    """
    cursor.execute(
        """SELECT variant_id, gene, condition_name, severity, inheritance,
                  plain_description, carrier_note, affected_note
           FROM variants WHERE UPPER(gene) = UPPER(?)""",
        (gene,)
    )
    rows = cursor.fetchall()
    cols = ["variant_id","gene","condition_name","severity","inheritance",
            "plain_description","carrier_note","affected_note"]
    return [dict(zip(cols, r)) for r in rows]


def _query_breeds(cursor: sqlite3.Cursor, variant_id: str) -> list[str]:
    cursor.execute(
        "SELECT breed_name FROM breeds WHERE variant_id = ?", (variant_id,)
    )
    return [r[0] for r in cursor.fetchall()]


def _query_condition_details(cursor: sqlite3.Cursor, condition_name: str) -> dict:
    cursor.execute(
        """SELECT full_description, symptoms, management, omia_url
           FROM conditions WHERE condition_name = ?""",
        (condition_name,)
    )
    row = cursor.fetchone()
    if row:
        return {
            "full_description": row[0],
            "symptoms":         row[1],
            "management":       row[2],
            "omia_url":         row[3],
        }
    return {}


# ── Main checker ──────────────────────────────────────────────────────────────

def check_variants(
    variants: list[VcfVariant],
    cat_name:   str = "Unknown",
    cat_gender: str = "Unknown",
    cat_diet:   str = "Unknown",
) -> HealthReport:
    """
    Match each VcfVariant against the database and produce a HealthReport.
    """
    report = HealthReport(
        cat_name=cat_name,
        cat_gender=cat_gender,
        cat_diet=cat_diet,
        total_variants_checked=len(variants),
    )

    if not os.path.isfile(DB_PATH):
        log.error(f"Database not found: {DB_PATH}. Run database/init_db.py first.")
        return report

    conn   = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    seen_conditions: set[str] = set()

    for var in variants:
        if var.zygosity == "homozygous_ref":
            continue

        matched_rows: list[dict] = []

        # ── Strategy 1: direct variant_id lookup ──────────────────────────
        for key in generate_lookup_keys(var):
            row = _query_by_variant_id(cursor, key)
            if row:
                matched_rows = [row]
                break

        # ── Strategy 2: gene symbol from INFO field ───────────────────────
        if not matched_rows:
            gene_hint = extract_gene_from_info(var.info)
            if gene_hint:
                matched_rows = _query_by_gene(cursor, gene_hint)

        # ── Strategy 3: gene symbol from the variant ID itself ────────────
        # Some VCFs put the gene name in the ID column
        if not matched_rows and var.variant_id and var.variant_id != ".":
            matched_rows = _query_by_gene(cursor, var.variant_id)

        if not matched_rows:
            report.unmatched_count += 1
            continue

        # ── Build a Finding for each matched condition ────────────────────
        for db_row in matched_rows:
            condition_name = db_row["condition_name"]
            if condition_name in seen_conditions:
                continue
            seen_conditions.add(condition_name)

            status  = determine_status(var.zygosity, db_row["inheritance"])
            message = (
                db_row["affected_note"] if status == "Affected"
                else db_row["carrier_note"]
            )

            breeds  = _query_breeds(cursor, db_row["variant_id"])
            details = _query_condition_details(cursor, condition_name)

            finding = Finding(
                gene=db_row["gene"],
                condition_name=condition_name,
                severity=db_row["severity"],
                inheritance=db_row["inheritance"],
                status=status,
                plain_description=db_row["plain_description"],
                message=message,
                at_risk_breeds=breeds,
                condition_details=details,
            )

            if finding.severity == "High":
                report.high.append(finding)
            elif finding.severity == "Medium":
                report.medium.append(finding)
            else:
                report.low.append(finding)

            log.info(
                f"  MATCH: {condition_name} [{finding.severity}] "
                f"gene={db_row['gene']} status={status}"
            )

    conn.close()
    log.info(
        f"Check complete — High: {len(report.high)}, "
        f"Medium: {len(report.medium)}, Low: {len(report.low)}, "
        f"Unmatched: {report.unmatched_count}"
    )
    return report
