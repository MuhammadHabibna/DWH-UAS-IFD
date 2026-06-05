"""
datamart.py — Atoti OLAP DataMart: IFC Investment DWH
Mata Kuliah : Data Warehouse | UAS SADA 2026
Anggota 3   : OLAP Analyst
"""

import asyncio
import logging
import sys

import atoti as tt
import asyncpg
import pandas as pd

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("atoti_datamart")

DB_URI = "postgresql://postgres.bbbszbykqcxrxnfszvmc:DWH-UAS-IFC@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres"


# ─────────────────────────────────────────────
# PHASE 1 — FETCH DATA DARI SUPABASE
# ─────────────────────────────────────────────
async def fetch_dwh_data() -> dict[str, pd.DataFrame]:
    logger.info("Menghubungkan ke Supabase PostgreSQL...")
    conn = await asyncpg.connect(DB_URI, statement_cache_size=0)

    logger.info("Mengunduh data dimensi dan fakta...")
    tables = [
        "dim_project", "dim_company", "dim_location",
        "dim_industry", "dim_product_line", "dim_env_category",
        "dim_date", "fact_investment",
    ]

    dfs: dict[str, pd.DataFrame] = {}
    for table in tables:
        rows = await conn.fetch(f"SELECT * FROM {table}")
        if rows:
            dfs[table] = pd.DataFrame([dict(r) for r in rows])
            logger.info(f"  {table:<22}: {len(dfs[table]):,} baris")
        else:
            dfs[table] = pd.DataFrame()
            logger.warning(f"  {table:<22}: KOSONG")

    await conn.close()

    # ── Bersihkan fact_investment ──────────────────────────────────────────
    if not dfs["fact_investment"].empty:
        num_cols = [
            "loan_usd_million", "equity_usd_million",
            "guarantee_usd_million", "risk_mgmt_usd_million",
            "total_investment_usd_million",
        ]
        for col in num_cols:
            if col in dfs["fact_investment"].columns:
                dfs["fact_investment"][col] = (
                    pd.to_numeric(dfs["fact_investment"][col], errors="coerce").fillna(0.0)
                )

    # ── Bersihkan dim_date ────────────────────────────────────────────────
    if not dfs["dim_date"].empty:
        dfs["dim_date"] = dfs["dim_date"].fillna(0)
        for col in dfs["dim_date"].columns:
            if col != "date_id":
                dfs["dim_date"][col] = dfs["dim_date"][col].astype(str)

    return dfs


# ─────────────────────────────────────────────
# PHASE 2 — BANGUN ATOTI CUBE
# ─────────────────────────────────────────────
def setup_atoti_cube(session: tt.Session, dfs: dict[str, pd.DataFrame]):
    logger.info("\n=== INISIALISASI ATOTI SESSION ===")

    # 2. Buat tabel in-memory
    logger.info("Membuat tabel Atoti dari DataFrame...")
    project_tbl  = session.read_pandas(dfs["dim_project"],      table_name="dim_project",      keys=["project_number"])
    company_tbl  = session.read_pandas(dfs["dim_company"],      table_name="dim_company",      keys=["company_id"])
    location_tbl = session.read_pandas(dfs["dim_location"],     table_name="dim_location",     keys=["location_id"])
    industry_tbl = session.read_pandas(dfs["dim_industry"],     table_name="dim_industry",     keys=["industry_id"])
    product_tbl  = session.read_pandas(dfs["dim_product_line"], table_name="dim_product_line", keys=["product_line_id"])
    env_tbl      = session.read_pandas(dfs["dim_env_category"], table_name="dim_env_category", keys=["env_category_id"])
    date_tbl     = session.read_pandas(dfs["dim_date"],         table_name="dim_date",         keys=["date_id"])
    fact_tbl     = session.read_pandas(dfs["fact_investment"],  table_name="fact_investment")

    # 3. Relasi Star Schema
    logger.info("Membangun relasi Star Schema...")
    fact_tbl.join(project_tbl)
    fact_tbl.join(company_tbl)
    fact_tbl.join(location_tbl)
    fact_tbl.join(industry_tbl)
    fact_tbl.join(product_tbl)
    fact_tbl.join(env_tbl)
    fact_tbl.join(date_tbl, fact_tbl["date_disclosed"] == date_tbl["date_id"])

    # 4. Buat Cube
    logger.info("Membangun OLAP Cube...")
    cube = session.create_cube(fact_tbl, name="IFC Investment Cube")
    h = cube.hierarchies
    m = cube.measures
    l = cube.levels

    # ── HIERARKI ─────────────────────────────────────────────────────────
    logger.info("Mengkonfigurasi hierarki dimensi...")

    h["Time"] = [
        date_tbl["decade"],
        date_tbl["year"],
        date_tbl["quarter"],
        date_tbl["month_name"],
    ]

    h["Location"] = [
        location_tbl["country"],
    ]

    h["Industry"] = [
        industry_tbl["industry"],
        industry_tbl["department"],
    ]

    h["Product Line"] = [
        product_tbl["product_line"],
    ]

    h["Env Category"] = [
        env_tbl["category_label"],
    ]

    # ── MEASURES KUSTOM ──────────────────────────────────────────────────
    logger.info("Mendefinisikan measures kustom...")

    m["Project Count"] = tt.agg.count_distinct(fact_tbl["project_number"])

    m["Total Investment (USD M)"] = m["total_investment_usd_million.SUM"]
    m["Loan (USD M)"]             = m["loan_usd_million.SUM"]
    m["Equity (USD M)"]           = m["equity_usd_million.SUM"]
    m["Guarantee (USD M)"]        = m["guarantee_usd_million.SUM"]
    m["Risk Mgmt (USD M)"]        = m["risk_mgmt_usd_million.SUM"]

    m["Avg Investment per Project (USD M)"] = (
        m["Total Investment (USD M)"] / m["Project Count"]
    )

    m["Loan Ratio (%)"]   = (m["Loan (USD M)"]   / m["Total Investment (USD M)"]) * 100
    m["Equity Ratio (%)"] = (m["Equity (USD M)"] / m["Total Investment (USD M)"]) * 100

    logger.info("\n[SUCCESS] OLAP Cube berhasil dibangun!")
    logger.info(f"[URL] Dashboard Atoti: {session.url}")

    return cube


# ─────────────────────────────────────────────
# PHASE 3 — INSIGHT PROGRAMATIK
# ─────────────────────────────────────────────
def run_insight_queries(cube: tt.Cube):
    m = cube.measures
    h = cube.hierarchies

    logger.info("\n=== INSIGHT BISNIS (OLAP QUERY) ===")

    # Insight 1 — Top 10 negara berdasarkan total investasi
    logger.info("\n[Insight 1] Top 10 Negara — Total Investment")
    df1 = cube.query(
        m["Total Investment (USD M)"],
        m["Project Count"],
        levels=[h["Location"]["country"]],
    ).sort_values("Total Investment (USD M)", ascending=False).head(10)
    print(df1.to_string())

    # Insight 2 — Tren investasi per dekade
    logger.info("\n[Insight 2] Tren Investasi per Dekade")
    df2 = cube.query(
        m["Total Investment (USD M)"],
        m["Project Count"],
        m["Avg Investment per Project (USD M)"],
        levels=[h["Time"]["decade"]],
    ).sort_values("decade")
    print(df2.to_string())

    # Insight 3 — Distribusi per Product Line
    logger.info("\n[Insight 3] Distribusi Investment per Product Line")
    df3 = cube.query(
        m["Total Investment (USD M)"],
        m["Loan Ratio (%)"],
        m["Equity Ratio (%)"],
        levels=[h["Product Line"]["product_line"]],
    ).sort_values("Total Investment (USD M)", ascending=False)
    print(df3.to_string())

    # Insight 4 — Top 5 industri berdasarkan jumlah proyek
    logger.info("\n[Insight 4] Top 5 Industri — Jumlah Proyek")
    df4 = cube.query(
        m["Project Count"],
        m["Total Investment (USD M)"],
        levels=[h["Industry"]["industry"]],
    ).sort_values("Project Count", ascending=False).head(5)
    print(df4.to_string())

    # Insight 5 — Rata-rata investasi per proyek per tahun
    logger.info("\n[Insight 5] Rata-rata Investasi per Proyek per Tahun")
    df5 = cube.query(
        m["Avg Investment per Project (USD M)"],
        m["Project Count"],
        levels=[h["Time"]["year"]],
    )
    print(df5.to_string())


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
async def main():
    dfs = await fetch_dwh_data()

    from pathlib import Path
    config = tt.SessionConfig(
        port=9090,
        user_content_storage=Path(__file__).parent / "atoti_storage"
    )
    with tt.Session.start(config) as session:
        cube = setup_atoti_cube(session, dfs)
        run_insight_queries(cube)

        logger.info(f"\n[READY] Server Atoti berjalan di: {session.url}")
        logger.info("Buka browser ke http://localhost:9090 untuk dashboard.")
        logger.info("Tekan Ctrl+C untuk menghentikan server.\n")

        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Menghentikan server Atoti...")


if __name__ == "__main__":
    asyncio.run(main())