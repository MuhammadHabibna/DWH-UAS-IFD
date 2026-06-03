"""
main_pipeline.py — Async Orchestrator pipeline ETL IFC Investment DWH
Mata Kuliah: Data Warehouse | UAS SADA 2026
Anggota 1 — Data Engineer

Pipeline:
    Extract (async, per dekade)
        → Transform (async.gather, semua batch concurrent)
            → [Handoff ke load.py — Anggota 2]

Cara menjalankan:
    # Dari root proyek:
    python -m etl.main_pipeline

    # Atau dengan argumen untuk menjalankan batch tertentu saja:
    python -m etl.main_pipeline --batch 2000-2009

    # Untuk menyimpan output CSV (debug):
    python -m etl.main_pipeline --save-csv
"""

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

import asyncpg
import pandas as pd

from etl.config import DB_URI
from etl.extract import extract_all
from etl.transform import transform_all
from etl.load import load_batch_async, refresh_views_async

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# ORCHESTRATOR UTAMA
# ─────────────────────────────────────────────────────────────────────────────

async def run_pipeline(
    batch_filter: str | None = None,
    save_csv: bool = False,
) -> dict[str, pd.DataFrame]:
    """
    Jalankan pipeline ETL end-to-end (Extract → Transform).
    Load diserahkan ke load.py milik Anggota 2.

    Parameters
    ----------
    batch_filter : jika diisi, hanya proses satu batch (mis. "2000-2009")
    save_csv     : jika True, simpan DataFrame hasil transform ke folder output/

    Returns
    -------
    dict[str, pd.DataFrame]  — hasil transform, siap di-load
    """
    pipeline_start = time.perf_counter()
    logger.info("╔══════════════════════════════════════════════════╗")
    logger.info("║   ETL PIPELINE — IFC Investment DWH              ║")
    logger.info("║   UAS SADA 2026 | Anggota 1 — Data Engineer      ║")
    logger.info("╚══════════════════════════════════════════════════╝\n")

    # ── 1. EXTRACT ────────────────────────────────────────────────────────────
    t0 = time.perf_counter()
    batches_raw = await extract_all()
    t_extract = time.perf_counter() - t0
    logger.info(f"⏱  Extract selesai dalam {t_extract:.2f} detik\n")

    # Opsional: filter satu batch saja
    if batch_filter:
        if batch_filter not in batches_raw:
            available = list(batches_raw.keys())
            logger.error(
                f"Batch '{batch_filter}' tidak ditemukan. "
                f"Pilihan yang tersedia: {available}"
            )
            sys.exit(1)
        batches_raw = {batch_filter: batches_raw[batch_filter]}
        logger.info(f"[Filter aktif] Hanya memproses batch: {batch_filter}\n")

    # ── 2. TRANSFORM ──────────────────────────────────────────────────────────
    t0 = time.perf_counter()
    batches_clean = await transform_all(batches_raw)
    t_transform = time.perf_counter() - t0
    logger.info(f"⏱  Transform selesai dalam {t_transform:.2f} detik\n")

    # ── OPSIONAL: Simpan ke CSV untuk debugging ────────────────────────────────
    if save_csv:
        await _save_batches_to_csv(batches_clean)

    # ── 3. LOAD KE SUPABASE ──────────────────────────────────────────────────
    logger.info("=== FASE LOAD DIMULAI ===")
    logger.info("  Menghubungkan ke Supabase PostgreSQL...")

    t0 = time.perf_counter()
    total_facts = 0
    total_skipped = 0

    try:
        conn = await asyncpg.connect(DB_URI)
        logger.info("  Koneksi ke Supabase berhasil!\n")

        for label, df_clean in batches_clean.items():
            batch_counts = await load_batch_async(conn, df_clean, label)
            total_facts += batch_counts.get("fact_investment", 0)
            total_skipped += batch_counts.get("skipped", 0)

        # Refresh Materialized Views
        logger.info("\n  Menyegarkan Materialized Views...")
        await refresh_views_async(conn)
        await conn.close()
    except Exception as e:
        logger.error(f"  Gagal pada fase Load: {e}")
        raise

    t_load = time.perf_counter() - t0
    logger.info(f"\n⏱  Load selesai dalam {t_load:.2f} detik")
    logger.info("=== FASE LOAD SELESAI ===\n")

    # ── SUMMARY ───────────────────────────────────────────────────────────────
    total_rows = sum(len(df) for df in batches_clean.values())
    t_total = time.perf_counter() - pipeline_start

    logger.info("┌─────────────────────────────────────────────────────┐")
    logger.info("│  RINGKASAN PIPELINE                                 │")
    logger.info("├──────────────────────────┬──────────────────────────┤")
    logger.info(f"│  {'Batch':<24} │ {'Baris Bersih':>22} │")
    logger.info("├──────────────────────────┼──────────────────────────┤")
    for label, df in batches_clean.items():
        logger.info(f"│  {label:<24} │ {len(df):>22,} │")
    logger.info("├──────────────────────────┼──────────────────────────┤")
    logger.info(f"│  {'TOTAL BARIS CSV':<24} │ {total_rows:>22,} │")
    logger.info(f"│  {'FAKTA DIMUAT':<24} │ {total_facts:>22,} │")
    logger.info(f"│  {'BARIS DILEWATI':<24} │ {total_skipped:>22,} │")
    logger.info("├──────────────────────────┼──────────────────────────┤")
    logger.info(f"│  {'Waktu Extract':<24} │ {t_extract:>20.2f}s │")
    logger.info(f"│  {'Waktu Transform':<24} │ {t_transform:>20.2f}s │")
    logger.info(f"│  {'Waktu Load':<24} │ {t_load:>20.2f}s │")
    logger.info(f"│  {'Total Waktu':<24} │ {t_total:>20.2f}s │")
    logger.info("└──────────────────────────┴──────────────────────────┘")

    logger.info(
        "\n✅ Pipeline Extract→Transform→Load SELESAI. "
        "Data tersimpan di Supabase PostgreSQL Cloud.\n"
    )

    return batches_clean


# ─────────────────────────────────────────────────────────────────────────────
# HELPER: SIMPAN OUTPUT KE CSV
# ─────────────────────────────────────────────────────────────────────────────

async def _save_batches_to_csv(batches: dict[str, pd.DataFrame]) -> None:
    """
    Simpan setiap batch ke file CSV di folder output/ (untuk keperluan debug).
    Anggota 2 tidak perlu ini — mereka load dari DataFrame langsung.
    """
    import aiofiles

    output_dir = Path(__file__).resolve().parent.parent / "output"
    output_dir.mkdir(exist_ok=True)

    for label, df in batches.items():
        path = output_dir / f"cleaned_{label}.csv"
        csv_content = df.to_csv(index=False)
        async with aiofiles.open(path, "w", encoding="utf-8") as f:
            await f.write(csv_content)
        logger.info(f"  💾 Disimpan: {path} ({len(df):,} baris)")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )


def main():
    parser = argparse.ArgumentParser(
        description="ETL Pipeline — IFC Investment Data Warehouse (Anggota 1)"
    )
    parser.add_argument(
        "--batch",
        type=str,
        default=None,
        help="Hanya proses satu batch dekade, mis. '2000-2009'",
    )
    parser.add_argument(
        "--save-csv",
        action="store_true",
        help="Simpan output transform ke folder output/ (untuk debug)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Tampilkan log DEBUG",
    )
    args = parser.parse_args()

    _setup_logging(args.verbose)

    result = asyncio.run(
        run_pipeline(
            batch_filter=args.batch,
            save_csv=args.save_csv,
        )
    )

    # Kembalikan dict untuk dipakai modul lain (mis. load.py)
    return result


if __name__ == "__main__":
    main()