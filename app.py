"""
app.py — Flask web application for the feline genetics module.

Routes:
  GET  /          → Upload page (index.html)
  POST /analyze   → Process uploaded VCF + cat info → redirect to report
  GET  /report    → Display the HealthReport (report.html)
  GET  /about     → Brief explanation of the tool

Run with:
    python app.py
Then open http://127.0.0.1:5000 in your browser.
"""

import os
import uuid
import logging

from flask import (
    Flask, request, render_template,
    redirect, url_for, flash, session
)

from modules.vcf_parser import parse_vcf
from modules.variant_checker import check_variants

# ── App setup ─────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "bio595-feline-genetics-dev-key")

UPLOAD_FOLDER   = os.path.join(os.path.dirname(__file__), "uploads")
ALLOWED_EXTENSIONS = {"vcf"}
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(name)s]: %(message)s")
log = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def allowed_file(filename: str) -> bool:
    """Check the file extension is .vcf"""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def safe_filename(filename: str) -> str:
    """Return a safe, unique filename to avoid collisions and path traversal."""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "vcf"
    return f"{uuid.uuid4().hex}.{ext}"


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    """Upload page."""
    return render_template("index.html")


@app.route("/analyze", methods=["POST"])
def analyze():
    """
    Handle the form submission:
      1. Validate uploaded file.
      2. Save it temporarily.
      3. Parse the VCF.
      4. Check variants against the database.
      5. Store the report in the session and redirect to /report.
    """
    # ── Validate file ─────────────────────────────────────────────────────────
    if "vcf_file" not in request.files:
        flash("No file was uploaded. Please choose a VCF file.", "error")
        return redirect(url_for("index"))

    vcf_file = request.files["vcf_file"]
    if vcf_file.filename == "":
        flash("No file selected.", "error")
        return redirect(url_for("index"))

    if not allowed_file(vcf_file.filename):
        flash("Only .vcf files are accepted.", "error")
        return redirect(url_for("index"))

    # ── Save the file ─────────────────────────────────────────────────────────
    safe_name = safe_filename(vcf_file.filename)
    save_path = os.path.join(UPLOAD_FOLDER, safe_name)
    vcf_file.save(save_path)
    log.info(f"VCF saved to {save_path}")

    # ── Read cat info from form ───────────────────────────────────────────────
    cat_name   = request.form.get("cat_name",   "").strip() or "Unknown"
    cat_gender = request.form.get("cat_gender", "Unknown")
    cat_diet   = request.form.get("cat_diet",   "").strip() or "Not specified"

    # ── Parse + check ─────────────────────────────────────────────────────────
    try:
        variants = parse_vcf(save_path)
    except (FileNotFoundError, ValueError) as exc:
        flash(f"Could not read VCF file: {exc}", "error")
        _cleanup(save_path)
        return redirect(url_for("index"))
    except Exception as exc:
        log.exception("Unexpected error during VCF parsing")
        flash(f"An unexpected error occurred while reading your file: {exc}", "error")
        _cleanup(save_path)
        return redirect(url_for("index"))

    report = check_variants(variants, cat_name, cat_gender, cat_diet)

    # ── Serialise report into session (small enough for a cookie) ─────────────
    # We store only what the template needs, not the full Python objects.
    session["report"] = _serialise_report(report)

    # ── Clean up uploaded file ────────────────────────────────────────────────
    _cleanup(save_path)

    return redirect(url_for("show_report"))


@app.route("/report")
def show_report():
    """Display the results report page."""
    report_data = session.get("report")
    if not report_data:
        flash("No report found. Please upload a VCF file first.", "error")
        return redirect(url_for("index"))
    return render_template("report.html", report=report_data)


@app.route("/about")
def about():
    return render_template("about.html")


# ── Utility helpers ───────────────────────────────────────────────────────────

def _cleanup(path: str):
    """Delete a temporary file silently."""
    try:
        os.remove(path)
    except OSError:
        pass


def _serialise_finding(f) -> dict:
    """Convert a Finding dataclass to a plain dict for session storage."""
    return {
        "gene":            f.gene,
        "condition_name":  f.condition_name,
        "severity":        f.severity,
        "inheritance":     f.inheritance,
        "status":          f.status,
        "plain_description": f.plain_description,
        "message":         f.message,
        "at_risk_breeds":  f.at_risk_breeds,
        "condition_details": f.condition_details,
    }


def _serialise_report(report) -> dict:
    """Convert a HealthReport dataclass to a plain dict for session storage."""
    return {
        "cat_name":               report.cat_name,
        "cat_gender":             report.cat_gender,
        "cat_diet":               report.cat_diet,
        "total_variants_checked": report.total_variants_checked,
        "unmatched_count":        report.unmatched_count,
        "has_findings":           report.has_findings,
        "high":   [_serialise_finding(f) for f in report.high],
        "medium": [_serialise_finding(f) for f in report.medium],
        "low":    [_serialise_finding(f) for f in report.low],
    }


# ── Run ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Initialize the database on first run if it doesn't exist yet
    db_path = os.path.join(os.path.dirname(__file__), "database", "feline_genetics.db")
    if not os.path.exists(db_path):
        log.info("Database not found — running init_db.py …")
        import sys
        sys.path.insert(0, os.path.dirname(__file__))
        from database.init_db import init_database
        init_database()

    app.run(debug=True, host="127.0.0.1", port=5000)
