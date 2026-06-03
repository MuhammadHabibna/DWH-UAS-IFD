"""
load.py — Fase Load: denormalisasi dimensional + async bulk insert ke Supabase
Mata Kuliah: Data Warehouse | UAS SADA 2026
Anggota 2 — Database Architect

Menerima DataFrame flat hasil transform.py (Anggota 1), memecahnya ke tabel
dimensi & fakta, lalu insert ke Supabase secara asinkron menggunakan asyncpg.
"""

import logging
from datetime import datetime

import asyncpg
import pandas as pd

from etl.config import ENV_CATEGORY_LABELS

logger = logging.getLogger(__name__)

# Nama bulan untuk dim_date
_MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def _parse_date_or_none(val):
    """Konversi string tanggal ISO ke date object, atau None."""
    if val and str(val).strip() != "" and not (isinstance(val, float)):
        try:
            return datetime.strptime(str(val), "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None
    return None


async def load_batch_async(
    conn: asyncpg.Connection,
    df: pd.DataFrame,
    batch_label: str,
) -> dict[str, int]:
    """
    Load satu batch DataFrame bersih ke Supabase.
    Langkah: insert dimensi → resolve surrogate keys → insert fakta.
    """
    logger.info(f"  [LOAD] Memulai batch '{batch_label}' ({len(df):,} baris)")
    counts: dict[str, int] = {}

    if df.empty:
        return counts

    # ── INSERT DIMENSI (ON CONFLICT DO NOTHING) ──────────────────────────────

    # dim_project
    projects = df[["project_number", "project_name", "document_type", "project_url"]].drop_duplicates(subset=["project_number"])
    recs = [(r["project_number"], r["project_name"], r.get("document_type", ""), r.get("project_url", "")) for _, r in projects.iterrows()]
    if recs:
        await conn.executemany(
            "INSERT INTO dim_project (project_number, project_name, document_type, project_url) VALUES ($1,$2,$3,$4) ON CONFLICT (project_number) DO NOTHING", recs)
    counts["dim_project"] = len(recs)

    # dim_company
    companies = df[["company_name"]].drop_duplicates()
    recs = [(r["company_name"],) for _, r in companies.iterrows() if r["company_name"]]
    if recs:
        await conn.executemany(
            "INSERT INTO dim_company (company_name) VALUES ($1) ON CONFLICT (company_name) DO NOTHING", recs)
    counts["dim_company"] = len(recs)

    # dim_location
    locations = df[["country", "ifc_country_code", "wb_country_code"]].drop_duplicates(subset=["country"])
    recs = [(r["country"], r.get("ifc_country_code", ""), r.get("wb_country_code", "")) for _, r in locations.iterrows() if r["country"]]
    if recs:
        await conn.executemany(
            "INSERT INTO dim_location (country, ifc_country_code, wb_country_code) VALUES ($1,$2,$3) ON CONFLICT (country) DO NOTHING", recs)
    counts["dim_location"] = len(recs)

    # dim_industry
    industries = df[["industry", "department"]].drop_duplicates()
    recs = [(r["industry"], r["department"]) for _, r in industries.iterrows() if r["industry"] and r["department"]]
    if recs:
        await conn.executemany(
            "INSERT INTO dim_industry (industry, department) VALUES ($1,$2) ON CONFLICT (industry, department) DO NOTHING", recs)
    counts["dim_industry"] = len(recs)

    # dim_product_line
    products = df[["product_line"]].drop_duplicates()
    recs = [(r["product_line"],) for _, r in products.iterrows() if r["product_line"]]
    if recs:
        await conn.executemany(
            "INSERT INTO dim_product_line (product_line) VALUES ($1) ON CONFLICT (product_line) DO NOTHING", recs)
    counts["dim_product_line"] = len(recs)

    # dim_env_category
    if "env_category_code" in df.columns and "env_category_label" in df.columns:
        env_cats = df[["env_category_code", "env_category_label"]].drop_duplicates(subset=["env_category_code"])
        recs = [(r["env_category_code"], r["env_category_label"]) for _, r in env_cats.iterrows() if r["env_category_code"]]
        if recs:
            await conn.executemany(
                "INSERT INTO dim_env_category (category_code, category_label) VALUES ($1,$2) ON CONFLICT (category_code) DO NOTHING", recs)
        counts["dim_env_category"] = len(recs)

    # dim_date
    valid_dates = df[df["date_disclosed"].notna() & df["date_disclosed"].ne("")]["date_disclosed"].unique()
    date_recs = []
    for d_str in valid_dates:
        try:
            dt = datetime.strptime(str(d_str), "%Y-%m-%d")
            date_recs.append((dt.date(), dt.year, (dt.month - 1) // 3 + 1, dt.month, _MONTH_NAMES[dt.month - 1], dt.day, (dt.year // 10) * 10))
        except (ValueError, TypeError):
            continue
    if date_recs:
        await conn.executemany(
            "INSERT INTO dim_date (date_id, year, quarter, month, month_name, day_of_month, decade) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (date_id) DO NOTHING", date_recs)
    counts["dim_date"] = len(date_recs)

    logger.info(f"  [LOAD] Dimensi dimuat — Proyek:{counts.get('dim_project',0)} Perusahaan:{counts.get('dim_company',0)} Lokasi:{counts.get('dim_location',0)} Industri:{counts.get('dim_industry',0)}")

    # ── RESOLVE SURROGATE KEYS ───────────────────────────────────────────────

    company_lookup = {r["company_name"]: r["company_id"] for r in await conn.fetch("SELECT company_id, company_name FROM dim_company")}
    location_lookup = {r["country"]: r["location_id"] for r in await conn.fetch("SELECT location_id, country FROM dim_location")}
    industry_lookup = {(r["industry"], r["department"]): r["industry_id"] for r in await conn.fetch("SELECT industry_id, industry, department FROM dim_industry")}
    product_lookup = {r["product_line"]: r["product_line_id"] for r in await conn.fetch("SELECT product_line_id, product_line FROM dim_product_line")}
    env_lookup = {r["category_code"]: r["env_category_id"] for r in await conn.fetch("SELECT env_category_id, category_code FROM dim_env_category")}

    # ── INSERT FAKTA ─────────────────────────────────────────────────────────

    facts = []
    skipped = 0

    for _, row in df.iterrows():
        if not row.get("date_disclosed") or pd.isna(row["date_disclosed"]) or row["date_disclosed"] == "":
            skipped += 1
            continue

        cid = company_lookup.get(row.get("company_name"))
        lid = location_lookup.get(row.get("country"))
        iid = industry_lookup.get((row.get("industry"), row.get("department")))
        pid = product_lookup.get(row.get("product_line"))
        eid = env_lookup.get(row.get("env_category_code"))

        if not all([cid, lid, iid, pid, eid]):
            skipped += 1
            continue

        facts.append((
            row["project_number"], cid, lid, iid,
            datetime.strptime(row["date_disclosed"], "%Y-%m-%d").date(),
            pid, eid, row.get("status", "Unspecified"),
            _parse_date_or_none(row.get("projected_board_date")),
            _parse_date_or_none(row.get("approval_date")),
            _parse_date_or_none(row.get("signed_date")),
            _parse_date_or_none(row.get("invested_date")),
            float(row.get("risk_mgmt_usd_million", 0) or 0),
            float(row.get("guarantee_usd_million", 0) or 0),
            float(row.get("loan_usd_million", 0) or 0),
            float(row.get("equity_usd_million", 0) or 0),
            float(row.get("total_investment_usd_million", 0) or 0),
        ))

    if facts:
        await conn.executemany("""
            INSERT INTO fact_investment (
                project_number, company_id, location_id, industry_id, date_disclosed,
                product_line_id, env_category_id, status,
                projected_board_date, approval_date, signed_date, invested_date,
                risk_mgmt_usd_million, guarantee_usd_million,
                loan_usd_million, equity_usd_million, total_investment_usd_million
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
        """, facts)

    counts["fact_investment"] = len(facts)
    counts["skipped"] = skipped
    logger.info(f"  [LOAD] Fakta dimuat: {len(facts):,} baris (dilewati: {skipped})")
    return counts


async def refresh_views_async(conn: asyncpg.Connection) -> None:
    """Segarkan Materialized Views setelah seluruh data dimuat."""
    try:
        await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_industry_yearly_summary")
        await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_country_investment_summary")
    except Exception:
        await conn.execute("REFRESH MATERIALIZED VIEW mv_industry_yearly_summary")
        await conn.execute("REFRESH MATERIALIZED VIEW mv_country_investment_summary")
    logger.info("  [LOAD] Materialized Views berhasil di-refresh")
