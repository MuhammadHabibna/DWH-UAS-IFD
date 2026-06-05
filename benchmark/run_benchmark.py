import asyncio
import json
import logging
import sys
import time
from pathlib import Path
import asyncpg

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("benchmark")

DB_URI = "postgresql://postgres.bbbszbykqcxrxnfszvmc:DWH-UAS-IFC@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres"
BENCHMARK_DIR = Path(__file__).resolve().parent
RESULTS_FILE = BENCHMARK_DIR / "benchmark_results.md"

# Query definitions
QUERIES = {
    "Query 1: Analisis Sektor Industri per Tahun": {
        "flat": """
            SELECT 
                EXTRACT(YEAR FROM date_disclosed) AS year,
                industry,
                department,
                COUNT(*) AS total_projects,
                SUM(loan_usd_million) AS total_loan,
                SUM(equity_usd_million) AS total_equity,
                SUM(total_investment_usd_million) AS total_investment
            FROM flat_investment
            WHERE date_disclosed IS NOT NULL
            GROUP BY year, industry, department
            ORDER BY year DESC, total_investment DESC;
        """,
        "star": """
            SELECT 
                d.year,
                ind.industry,
                ind.department,
                COUNT(f.project_number) AS total_projects,
                SUM(f.loan_usd_million) AS total_loan,
                SUM(f.equity_usd_million) AS total_equity,
                SUM(f.total_investment_usd_million) AS total_investment
            FROM fact_investment f
            JOIN dim_date d ON f.date_disclosed = d.date_id
            JOIN dim_industry ind ON f.industry_id = ind.industry_id
            GROUP BY d.year, ind.industry, ind.department
            ORDER BY d.year DESC, total_investment DESC;
        """,
        "mv": """
            SELECT 
                year,
                industry,
                department,
                total_projects,
                total_loan_usd_million AS total_loan,
                total_equity_usd_million AS total_equity,
                total_investment_usd_million AS total_investment
            FROM mv_industry_yearly_summary
            ORDER BY year DESC, total_investment DESC;
        """
    },
    "Query 2: Analisis Investasi Negara per Dekade": {
        "flat": """
            SELECT 
                (EXTRACT(YEAR FROM date_disclosed)::int / 10) * 10 AS decade,
                country,
                COUNT(*) AS total_projects,
                SUM(total_investment_usd_million) AS total_investment,
                AVG(total_investment_usd_million) AS avg_investment
            FROM flat_investment
            WHERE date_disclosed IS NOT NULL
            GROUP BY decade, country
            ORDER BY decade DESC, total_investment DESC;
        """,
        "star": """
            SELECT 
                d.decade,
                loc.country,
                COUNT(f.project_number) AS total_projects,
                SUM(f.total_investment_usd_million) AS total_investment,
                AVG(f.total_investment_usd_million) AS avg_investment
            FROM fact_investment f
            JOIN dim_date d ON f.date_disclosed = d.date_id
            JOIN dim_location loc ON f.location_id = loc.location_id
            GROUP BY d.decade, loc.country
            ORDER BY d.decade DESC, total_investment DESC;
        """,
        "mv": """
            SELECT 
                decade,
                country,
                total_projects,
                total_investment_usd_million AS total_investment,
                avg_investment_usd_million AS avg_investment
            FROM mv_country_investment_summary
            ORDER BY decade DESC, total_investment DESC;
        """
    }
}

async def measure_query(conn, query_name, scenario, query_sql, iterations=10):
    db_times = []
    wall_times = []
    
    # Warm-up run to load data in database buffer cache
    try:
        await conn.execute(query_sql)
    except Exception as e:
        logger.error(f"Gagal mengeksekusi query untuk {query_name} ({scenario}): {e}")
        return None
        
    for _ in range(iterations):
        # 1. Measure DB Internal Time using EXPLAIN ANALYZE
        explain_sql = f"EXPLAIN (ANALYZE, FORMAT JSON) {query_sql}"
        explain_res = await conn.fetchval(explain_sql)
        # Parse JSON
        explain_data = json.loads(explain_res)[0]
        exec_time = explain_data.get("Execution Time", 0.0) # in ms
        plan_time = explain_data.get("Planning Time", 0.0)   # in ms
        db_times.append(exec_time + plan_time)
        
        # 2. Measure Wall Clock Time (includes network round trip)
        t0 = time.perf_counter()
        await conn.execute(query_sql)
        wall_time_ms = (time.perf_counter() - t0) * 1000
        wall_times.append(wall_time_ms)
        
    avg_db_time = sum(db_times) / len(db_times)
    avg_wall_time = sum(wall_times) / len(wall_times)
    
    return {
        "avg_db_time": avg_db_time,
        "avg_wall_time": avg_wall_time
    }

async def run_benchmark():
    logger.info("=== MEMULAI RUN BENCHMARK PERFORMA ===")
    logger.info("Menghubungkan ke Supabase...")
    conn = await asyncpg.connect(DB_URI)
    
    results = {}
    
    for q_name, scenarios in QUERIES.items():
        logger.info(f"\nMenguji {q_name}...")
        results[q_name] = {}
        for scenario, sql in scenarios.items():
            logger.info(f"  Menjalankan skenario: {scenario.upper()}...")
            stats = await measure_query(conn, q_name, scenario, sql)
            if stats:
                results[q_name][scenario] = stats
                logger.info(f"    -> DB Time: {stats['avg_db_time']:.2f} ms | Wall Time: {stats['avg_wall_time']:.2f} ms")
                
    await conn.close()
    
    # Generate Markdown Report
    generate_report(results)
    logger.info("\n=== BENCHMARK SELESAI & LAPORAN DI-GENERATE ===")

def generate_report(results):
    lines = [
        "# Laporan Hasil Benchmark Performa Database DWH",
        "",
        "**Tanggal Pengujian**: 2026-06-01  ",
        "**Database**: PostgreSQL Cloud (Supabase)  ",
        "**Metrik**: Rata-rata dari 10 kali eksekusi (dalam milidetik - ms)  ",
        "",
        "Laporan ini membandingkan 3 skenario arsitektur:",
        "1. **Flat Table** (Tabel tunggal hasil import mentah tanpa partisi dan indeks)",
        "2. **Star Schema** (Dimensi terpisah + Fakta terpartisi per dekade + indeks B-Tree/BRIN)",
        "3. **Materialized View** (Agregasi pra-hitung/tersimpan di database)",
        "",
        "---",
        ""
    ]
    
    for q_name, stats in results.items():
        flat_db = stats.get("flat", {}).get("avg_db_time", 0.0)
        flat_wall = stats.get("flat", {}).get("avg_wall_time", 0.0)
        
        star_db = stats.get("star", {}).get("avg_db_time", 0.0)
        star_wall = stats.get("star", {}).get("avg_wall_time", 0.0)
        
        mv_db = stats.get("mv", {}).get("avg_db_time", 0.0)
        mv_wall = stats.get("mv", {}).get("avg_wall_time", 0.0)
        
        # Calculate improvements
        star_imp = ((flat_db - star_db) / flat_db * 100) if flat_db else 0
        mv_imp = ((flat_db - mv_db) / flat_db * 100) if flat_db else 0
        
        lines.append(f"## {q_name}")
        lines.append("")
        lines.append("| Skenario | Waktu DB Internal (ms) | Waktu Total + Jaringan (ms) | Peningkatan Kecepatan (DB Time) |")
        lines.append("| :--- | :---: | :---: | :---: |")
        lines.append(f"| **Flat Table** | {flat_db:.2f} ms | {flat_wall:.2f} ms | (Baseline) |")
        lines.append(f"| **Star Schema (Optimized)** | {star_db:.2f} ms | {star_wall:.2f} ms | **{star_imp:.1f}% Lebih Cepat** |")
        lines.append(f"| **Materialized View** | {mv_db:.2f} ms | {mv_wall:.2f} ms | **{mv_imp:.1f}% Lebih Cepat** |")
        lines.append("")
        lines.append("> [!TIP]")
        lines.append(f"> Menggunakan **Materialized View** memangkas waktu proses di database menjadi **{mv_db:.2f} ms** dibandingkan **{flat_db:.2f} ms** pada tabel flat.")
        lines.append("")
        lines.append("---")
        lines.append("")
        
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        
    # Print to stdout
    print("\n" + "\n".join(lines))

if __name__ == "__main__":
    asyncio.run(run_benchmark())
