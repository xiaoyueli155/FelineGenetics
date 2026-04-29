"""
vcf_parser.py — Parse a cat's VCF (Variant Call Format) file.

A VCF file is the standard output from DNA sequencing labs. It lists every
position in the genome where the sample differs from the reference, one line
per variant.

This module is intentionally kept simple:
  - It skips header lines (lines starting with '#').
  - It extracts the five core mandatory VCF columns.
  - It normalises the variant ID into a form that can be looked up in our DB.
  - It handles malformed lines gracefully (log & skip, never crash).
"""

import logging
import os
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

# Maximum file size we accept (50 MB) — prevents memory issues on a laptop
MAX_FILE_BYTES = 50 * 1024 * 1024


@dataclass
class VcfVariant:
    """One row from a VCF file, parsed into labelled fields."""
    chrom:      str          # Chromosome, e.g. "A1" or "chrA1"
    pos:        int          # 1-based genomic position
    variant_id: str          # rs-ID or "." if not annotated
    ref:        str          # Reference allele
    alt:        str          # Alternate allele(s), comma-separated if multi-allelic
    qual:       str          # Quality score (string because it can be ".")
    filter_:    str          # FILTER field — "PASS" or a reason for filtering
    info:       str          # INFO field (unparsed raw string)
    gt:         str          # Genotype from the first sample column, e.g. "0/1"
    zygosity:   str = field(init=False)  # "homozygous_alt", "heterozygous", "homozygous_ref"

    def __post_init__(self):
        self.zygosity = self._infer_zygosity(self.gt)

    @staticmethod
    def _infer_zygosity(gt: str) -> str:
        """
        GT field examples:
          "0/0" → homozygous reference (not a variant of interest)
          "0/1" or "1/0" → heterozygous  (one copy — carrier for recessive,
                                           or one dominant allele)
          "1/1" → homozygous alternate   (two copies)
          "."   → missing / unknown
        """
        if not gt or gt in (".", "./."):
            return "unknown"
        alleles = gt.replace("|", "/").split("/")
        unique = set(alleles) - {"."}
        if unique == {"0"}:
            return "homozygous_ref"
        elif "0" in unique and len(unique) > 1:
            return "heterozygous"
        elif len(unique) == 1:
            return "homozygous_alt"
        else:
            return "heterozygous"   # e.g. "1/2" multi-allelic


def parse_vcf(filepath: str) -> list[VcfVariant]:
    """
    Read a VCF file and return a list of VcfVariant objects.

    Args:
        filepath: Absolute or relative path to the .vcf file.

    Returns:
        List of parsed VcfVariant objects (one per non-header, non-empty line).

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is too large or has no data lines.
    """
    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"VCF file not found: {filepath}")

    file_size = os.path.getsize(filepath)
    if file_size > MAX_FILE_BYTES:
        raise ValueError(
            f"File is {file_size / 1e6:.1f} MB — max allowed is "
            f"{MAX_FILE_BYTES / 1e6:.0f} MB. Please use a filtered VCF."
        )

    variants   = []
    line_count = 0
    skip_count = 0

    with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
        for raw_line in fh:
            line = raw_line.rstrip("\n")

            # ── Skip meta-information and header lines ────────────────────
            if line.startswith("##"):
                continue
            if line.startswith("#CHROM"):
                # Column header line — useful for validation but we skip it
                continue
            if not line.strip():
                continue

            line_count += 1
            cols = line.split("\t")

            # A valid VCF data line must have at least 8 columns
            if len(cols) < 8:
                log.warning(f"Line {line_count}: only {len(cols)} columns — skipping.")
                skip_count += 1
                continue

            try:
                chrom      = cols[0]
                pos        = int(cols[1])
                variant_id = cols[2]         # Often "." in raw VCFs
                ref        = cols[3].upper()
                alt        = cols[4].upper()
                qual       = cols[5]
                filter_    = cols[6]
                info       = cols[7]

                # Genotype is in the first sample column (col 9 onward)
                # The FORMAT column (col 8) tells us the field order
                gt = "."
                if len(cols) >= 10:
                    fmt_fields = cols[8].split(":")
                    smp_fields = cols[9].split(":")
                    if "GT" in fmt_fields:
                        gt_index = fmt_fields.index("GT")
                        if gt_index < len(smp_fields):
                            gt = smp_fields[gt_index]

                v = VcfVariant(
                    chrom=chrom, pos=pos, variant_id=variant_id,
                    ref=ref, alt=alt, qual=qual, filter_=filter_,
                    info=info, gt=gt,
                )
                variants.append(v)

            except (ValueError, IndexError) as exc:
                log.warning(f"Line {line_count}: parse error ({exc}) — skipping.")
                skip_count += 1

    log.info(
        f"VCF parsed: {len(variants)} variants extracted, "
        f"{skip_count} lines skipped, from '{os.path.basename(filepath)}'."
    )

    if not variants:
        raise ValueError(
            "No valid variant lines found in the VCF file. "
            "Please check the file format."
        )

    return variants


def generate_lookup_keys(variant: VcfVariant) -> list[str]:
    """
    Build all the identifier strings we will use to query the database
    for a given VcfVariant.

    We try several forms because:
      - Some VCFs annotate the ID column with an rs-number or HGVS notation
        that matches our database directly.
      - Others leave the ID as ".", so we also try a positional key.

    Returns a list of strings to try, in priority order.
    """
    keys = []

    # 1. Use the ID column as-is (may be an HGVS notation or rs-ID)
    if variant.variant_id and variant.variant_id != ".":
        keys.append(variant.variant_id)

    # 2. Try a positional key: CHROM:POS_REF>ALT  (for unannotated VCFs)
    chrom_clean = variant.chrom.replace("chr", "").replace("Chr", "")
    for alt_allele in variant.alt.split(","):
        keys.append(f"{chrom_clean}:{variant.pos}_{variant.ref}>{alt_allele}")

    return keys
