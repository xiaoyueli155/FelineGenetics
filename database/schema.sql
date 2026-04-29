-- =============================================================
--  Feline Genetics Database Schema
--  Bio595 Project — Xiaoyue
-- =============================================================

-- Every known feline genetic variant and its health implications
CREATE TABLE IF NOT EXISTS variants (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    variant_id      TEXT UNIQUE NOT NULL,   -- e.g. "c.1176_1177insC"
    gene            TEXT NOT NULL,           -- e.g. "PKD1"
    omia_id         TEXT,                    -- OMIA reference ID
    condition_name  TEXT NOT NULL,           -- Human-readable disease name
    severity        TEXT NOT NULL            -- 'High', 'Medium', or 'Low'
                    CHECK(severity IN ('High','Medium','Low')),
    inheritance     TEXT,                    -- 'Autosomal Dominant', 'Autosomal Recessive', 'X-linked', etc.
    plain_description TEXT,                  -- Written for non-scientists
    carrier_note    TEXT,                    -- What it means to be a carrier
    affected_note   TEXT                     -- What it means to be affected
);

-- Which cat breeds are most at risk for each condition
CREATE TABLE IF NOT EXISTS breeds (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    variant_id  TEXT NOT NULL REFERENCES variants(variant_id),
    breed_name  TEXT NOT NULL
);

-- Extra details about each health condition (one row per unique condition)
CREATE TABLE IF NOT EXISTS conditions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_name  TEXT UNIQUE NOT NULL,
    full_description TEXT,
    symptoms        TEXT,       -- Comma-separated symptom list
    management      TEXT,       -- Vet management advice
    omia_url        TEXT        -- Link to OMIA page for reference
);
