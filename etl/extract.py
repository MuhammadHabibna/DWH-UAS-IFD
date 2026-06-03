"""
extract.py — Fase Extract: baca CSV secara async, pecah per batch dekade
Mata Kuliah: Data Warehouse | UAS SADA 2026
Anggota 1 — Data Engineer

Tanggung jawab:
- Baca CSV dengan penanganan UTF-8 BOM
- Rename kolom sesuai COLUMN_MAP
- Pecah data menjadi batch periodik per dekade
- Kembalikan dict {label_dekade: DataFrame}
"""

import asyncio
import io
import logging
from pathlib import Path

import aiofiles
import pandas as pd

from etl.config import (
    CSV_PATH,
    COLUMN_MAP,
    DATE_BATCH_COLUMN,
    DECADE_BATCHES,
    NUMERIC_COLUMNS,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# FUNGSI UTAMA
# ─────────────────────────────────────────────────────────────────────────────

async def extract_all() -> dict[str, pd.DataFrame]:
    """
    Entry point fase Extract.

    Membaca seluruh CSV secara async, melakukan rename kolom,
    lalu memecah data ke dalam batch per dekade.

    Returns
    -------
    dict[str, pd.DataFrame]
        Key  : label dekade, mis. "1994-1999", "2000-2009", dst.
        Value: DataFrame berisi baris yang tanggal pengungkapannya
               jatuh dalam rentang dekade tersebut.
               Baris tanpa tanggal valid → dimasukkan ke batch terakhir.
    """
    logger.info("=== FASE EXTRACT DIMULAI ===")
    logger.info(f"Membaca file: {CSV_PATH}")

    raw_content = await _read_file_async(CSV_PATH)
    df_raw = _parse_csv(raw_content)

    logger.info(f"Total baris terbaca : {len(df_raw):,}")
    logger.info(f"Total kolom         : {len(df_raw.columns)}")

    df_renamed = _rename_columns(df_raw)
    batches = _split_into_decade_batches(df_renamed)

    _log_batch_summary(batches)
    logger.info("=== FASE EXTRACT SELESAI ===\n")

    return batches


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

async def _read_file_async(path: Path) -> str:
    """
    Baca file CSV secara asynchronous menggunakan aiofiles.
    Menangani UTF-8 BOM (byte order mark) yang ada di header file IFC.
    """
    async with aiofiles.open(path, mode="r", encoding="utf-8-sig") as f:
        content = await f.read()
    logger.debug(f"File berhasil dibaca ({len(content):,} karakter)")
    return content


def _parse_csv(raw_content: str) -> pd.DataFrame:
    """
    Parse string CSV ke DataFrame.
    Semua kolom dibaca sebagai string agar tidak ada konversi otomatis
    yang membuang informasi — konversi tipe dilakukan di fase Transform.
    """
    df = pd.read_csv(
        io.StringIO(raw_content),
        dtype=str,          # semua kolom → string, konversi tipe di Transform
        keep_default_na=False,  # jangan konversi "NA" string menjadi NaN dulu
    )
    # Hapus baris yang sepenuhnya kosong (artefak Excel/CSV export)
    df.dropna(how="all", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename kolom CSV asli ke nama internal berdasarkan COLUMN_MAP.
    Kolom yang tidak ada di COLUMN_MAP dibuang (tidak relevan untuk DWH).
    """
    # Ambil hanya kolom yang ada di COLUMN_MAP dan juga ada di DataFrame
    cols_to_keep = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}

    missing = set(COLUMN_MAP.keys()) - set(df.columns)
    if missing:
        logger.warning(f"Kolom tidak ditemukan di CSV (diabaikan): {missing}")

    df = df[list(cols_to_keep.keys())].rename(columns=cols_to_keep)
    logger.debug(f"Kolom setelah rename: {list(df.columns)}")
    return df


def _split_into_decade_batches(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Pecah DataFrame menjadi beberapa batch berdasarkan tahun di kolom
    `date_disclosed`. Simulasi incremental/periodic load per dekade.

    Baris dengan tanggal kosong atau tidak valid dimasukkan ke batch
    terakhir (2020-2029) agar tidak hilang dari pipeline.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame dengan kolom `date_disclosed` (masih string, mis. "03/15/2005")

    Returns
    -------
    dict[str, pd.DataFrame]  key = "YYYY-YYYY", value = subset DataFrame
    """
    internal_col = DATE_BATCH_COLUMN  # nama asli, sebelum rename ke date_disclosed

    # Kolom sudah di-rename — pakai nama internal
    date_col = "date_disclosed"

    # Parse tanggal sementara hanya untuk keperluan binning dekade
    # Error=coerce → tanggal tidak valid menjadi NaT
    date_series = pd.to_datetime(df[date_col], format="%m/%d/%Y", errors="coerce")
    years = date_series.dt.year

    batches: dict[str, pd.DataFrame] = {}
    last_label = None

    for (start, end) in DECADE_BATCHES:
        label = f"{start}-{end}"
        last_label = label
        mask = (years >= start) & (years <= end)
        subset = df[mask].copy()
        subset.reset_index(drop=True, inplace=True)
        batches[label] = subset

    # Baris dengan tahun NaT atau di luar semua rentang → masuk batch terakhir
    all_valid_mask = pd.Series(False, index=df.index)
    for (start, end) in DECADE_BATCHES:
        all_valid_mask |= (years >= start) & (years <= end)

    leftover = df[~all_valid_mask].copy()
    if not leftover.empty:
        logger.warning(
            f"{len(leftover)} baris tanpa tanggal valid — digabung ke batch '{last_label}'"
        )
        batches[last_label] = pd.concat(
            [batches[last_label], leftover], ignore_index=True
        )

    return batches


def _log_batch_summary(batches: dict[str, pd.DataFrame]) -> None:
    """Cetak ringkasan jumlah baris per batch ke log."""
    total = sum(len(v) for v in batches.values())
    logger.info(f"{'Batch':<12} {'Baris':>8}")
    logger.info(f"{'-'*22}")
    for label, df in batches.items():
        logger.info(f"{label:<12} {len(df):>8,}")
    logger.info(f"{'TOTAL':<12} {total:>8,}")


# ─────────────────────────────────────────────────────────────────────────────
# QUICK TEST (jalankan langsung: python -m etl.extract)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    result = asyncio.run(extract_all())
    for label, df in result.items():
        print(f"\nBatch {label}: {len(df)} baris")
        print(df.head(2).to_string())