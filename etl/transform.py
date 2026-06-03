"""
transform.py — Fase Transform: 6 langkah pembersihan data IFC
Mata Kuliah: Data Warehouse | UAS SADA 2026
Anggota 1 — Data Engineer

6 Langkah Transformasi:
    1. Decode HTML entities  (&amp; → &, dll.)
    2. Strip whitespace       (leading/trailing spaces)
    3. Standardisasi tanggal  (MM/DD/YYYY → YYYY-MM-DD / None)
    4. Handle null values     (numerik → 0.0, tanggal kosong → None)
    5. Normalisasi kategori   ("other" → "Other", string kosong → "Unspecified")
    6. [Deferral] Denormalisasi dimensional → dilakukan di load.py (Anggota 2)

Output: DataFrame flat yang sudah bersih, siap diteruskan ke load.py
"""

import asyncio
import html
import logging
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from etl.config import (
    DATE_COLUMNS,
    DEFAULTS,
    ENV_CATEGORY_LABELS,
    LOG_FILE,
    NUMERIC_COLUMNS,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# DATACLASS UNTUK STATISTIK ANOMALI
# ─────────────────────────────────────────────────────────────────────────────

class TransformStats:
    """Kumpulkan hitungan anomali yang ditemukan dan diperbaiki."""

    def __init__(self):
        self.html_entity_rows: int = 0
        self.whitespace_rows: int = 0
        self.date_converted: dict[str, int] = {}
        self.date_nulled: dict[str, int] = {}
        self.numeric_filled: dict[str, int] = {}
        self.category_normalized: dict[str, int] = {}
        self.empty_to_unspecified: dict[str, int] = {}
        self.batch_label: str = ""

    def to_log_lines(self) -> list[str]:
        lines = [
            f"\n{'='*60}",
            f"TRANSFORM LOG — Batch: {self.batch_label}",
            f"{'='*60}",
            f"[Langkah 1] HTML entity decode       : {self.html_entity_rows:,} baris",
            f"[Langkah 2] Whitespace stripped       : {self.whitespace_rows:,} baris",
            f"[Langkah 3] Konversi tanggal berhasil :",
        ]
        for col, n in self.date_converted.items():
            lines.append(f"             {col:<35}: {n:,} baris")
        lines.append(f"[Langkah 3] Tanggal di-NULL-kan       :")
        for col, n in self.date_nulled.items():
            lines.append(f"             {col:<35}: {n:,} baris")
        lines.append(f"[Langkah 4] Numerik null → 0.0        :")
        for col, n in self.numeric_filled.items():
            lines.append(f"             {col:<35}: {n:,} baris")
        lines.append(f"[Langkah 5] Kategori dinormalisasi    :")
        for col, n in self.category_normalized.items():
            lines.append(f"             {col:<35}: {n:,} baris")
        lines.append(f"[Langkah 5] Kosong → 'Unspecified'   :")
        for col, n in self.empty_to_unspecified.items():
            lines.append(f"             {col:<35}: {n:,} baris")
        lines.append(f"{'='*60}\n")
        return lines


# ─────────────────────────────────────────────────────────────────────────────
# FUNGSI UTAMA
# ─────────────────────────────────────────────────────────────────────────────

async def transform_batch(
    df: pd.DataFrame,
    batch_label: str,
) -> pd.DataFrame:
    """
    Jalankan 6 langkah transformasi pada satu batch DataFrame.

    Parameters
    ----------
    df          : DataFrame hasil extract (string semua)
    batch_label : mis. "1994-1999" — untuk keperluan logging

    Returns
    -------
    pd.DataFrame  — DataFrame bersih, siap untuk load.py
    """
    logger.info(f"[TRANSFORM] Memulai batch '{batch_label}' ({len(df):,} baris)")
    stats = TransformStats()
    stats.batch_label = batch_label

    # Jalankan setiap langkah secara berurutan
    # (await digunakan agar orchestrator bisa menyelang task lain antar-batch)
    df = await asyncio.to_thread(_step1_decode_html_entities, df, stats)
    df = await asyncio.to_thread(_step2_strip_whitespace, df, stats)
    df = await asyncio.to_thread(_step3_standardize_dates, df, stats)
    df = await asyncio.to_thread(_step4_handle_nulls, df, stats)
    df = await asyncio.to_thread(_step5_normalize_categories, df, stats)
    # Langkah 6 (denormalisasi) ada di load.py — bukan tanggung jawab Anggota 1

    await _write_log(stats)
    logger.info(f"[TRANSFORM] Selesai batch '{batch_label}'")
    return df


async def transform_all(
    batches: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """
    Transform semua batch secara concurrent menggunakan asyncio.gather.

    Parameters
    ----------
    batches : output dari extract.extract_all()

    Returns
    -------
    dict[str, pd.DataFrame]  — key tetap sama, value sudah bersih
    """
    logger.info("=== FASE TRANSFORM DIMULAI ===")

    tasks = [
        transform_batch(df, label)
        for label, df in batches.items()
    ]
    results_list = await asyncio.gather(*tasks)

    cleaned: dict[str, pd.DataFrame] = {
        label: df_clean
        for label, df_clean in zip(batches.keys(), results_list)
    }

    logger.info("=== FASE TRANSFORM SELESAI ===\n")
    return cleaned


# ─────────────────────────────────────────────────────────────────────────────
# LANGKAH 1 — Decode HTML Entities
# ─────────────────────────────────────────────────────────────────────────────

def _step1_decode_html_entities(df: pd.DataFrame, stats: TransformStats) -> pd.DataFrame:
    """
    Konversi HTML entity ke karakter asli di semua kolom string.
    Contoh: "&amp;" → "&", "&#39;" → "'", "&amp;amp;" → "&"

    Dataset IFC diketahui mengandung 862 baris (12.4%) dengan &amp;
    terutama di kolom `department`.
    """
    # Polanyala deteksi apakah suatu baris mengandung HTML entity
    html_entity_pattern = re.compile(r"&[a-zA-Z]+;|&#\d+;")

    str_cols = df.select_dtypes(include="object").columns
    affected_mask = pd.Series(False, index=df.index)

    for col in str_cols:
        has_entity = df[col].str.contains(html_entity_pattern, na=False)
        affected_mask |= has_entity
        df[col] = df[col].apply(
            lambda v: html.unescape(v) if isinstance(v, str) else v
        )

    stats.html_entity_rows = int(affected_mask.sum())
    logger.debug(f"  Langkah 1: {stats.html_entity_rows} baris mengandung HTML entity")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# LANGKAH 2 — Strip Whitespace
# ─────────────────────────────────────────────────────────────────────────────

def _step2_strip_whitespace(df: pd.DataFrame, stats: TransformStats) -> pd.DataFrame:
    """
    Hapus leading/trailing whitespace dari semua kolom string.
    Dataset IFC diketahui mengandung 151 baris (2.2%) dengan spasi berlebih.
    """
    str_cols = df.select_dtypes(include="object").columns
    affected_mask = pd.Series(False, index=df.index)

    for col in str_cols:
        original = df[col].copy()
        df[col] = df[col].str.strip()
        changed = (original != df[col]) & original.notna()
        affected_mask |= changed

    stats.whitespace_rows = int(affected_mask.sum())
    logger.debug(f"  Langkah 2: {stats.whitespace_rows} baris mengandung whitespace berlebih")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# LANGKAH 3 — Standardisasi Format Tanggal
# ─────────────────────────────────────────────────────────────────────────────

def _step3_standardize_dates(df: pd.DataFrame, stats: TransformStats) -> pd.DataFrame:
    """
    Konversi format tanggal dari MM/DD/YYYY (US format) ke YYYY-MM-DD (ISO 8601).
    Tanggal kosong / tidak valid → None (akan di-load sebagai NULL ke PostgreSQL).

    Kolom yang diproses: DATE_COLUMNS dari config.py
    """
    for col in DATE_COLUMNS:
        if col not in df.columns:
            continue

        original = df[col].copy()

        # Konversi: error='coerce' → nilai tidak valid menjadi NaT
        parsed = pd.to_datetime(df[col], format="%m/%d/%Y", errors="coerce")

        # Hitung berapa yang berhasil dan berapa yang gagal (null/invalid)
        was_not_empty = original.str.strip().str.len().gt(0)
        converted_ok  = parsed.notna()
        became_null   = was_not_empty & ~converted_ok

        stats.date_converted[col] = int(converted_ok.sum())
        stats.date_nulled[col]    = int(became_null.sum())

        # Format ke ISO 8601 string; None untuk NaT
        df[col] = parsed.dt.strftime("%Y-%m-%d").where(converted_ok, other=None)

        logger.debug(
            f"  Langkah 3 [{col}]: "
            f"{stats.date_converted[col]} berhasil, "
            f"{stats.date_nulled[col]} di-NULL-kan"
        )

    return df


# ─────────────────────────────────────────────────────────────────────────────
# LANGKAH 4 — Handle Null Values
# ─────────────────────────────────────────────────────────────────────────────

def _step4_handle_nulls(df: pd.DataFrame, stats: TransformStats) -> pd.DataFrame:
    """
    Tangani nilai kosong:
    - Kolom numerik (Loan, Equity, Guarantee, Risk Mgmt, Total) → 0.0
      Note: Risk Mgmt kosong di 97% baris, Guarantee di 93.8%
    - Kolom tanggal sudah ditangani di langkah 3 (None/NULL)
    - Kolom string kosong ditangani di langkah 5
    """
    for col in NUMERIC_COLUMNS:
        if col not in df.columns:
            continue

        # Hitung berapa yang kosong (string kosong atau betul-betul NaN)
        is_empty = df[col].str.strip().eq("") | df[col].isna()
        count_empty = int(is_empty.sum())
        stats.numeric_filled[col] = count_empty

        # Konversi ke float; nilai tidak valid → 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        logger.debug(f"  Langkah 4 [{col}]: {count_empty} nilai kosong → 0.0")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# LANGKAH 5 — Normalisasi Kategori
# ─────────────────────────────────────────────────────────────────────────────

def _step5_normalize_categories(df: pd.DataFrame, stats: TransformStats) -> pd.DataFrame:
    """
    Normalisasi kolom kategorik:
    a) Title Case: "other" → "Other" (474 baris diketahui lowercase)
    b) String kosong → nilai default dari DEFAULTS di config.py
    c) Tambah kolom `env_category_label` dari peta kode → label

    Kolom yang dicek: semua string kolom yang ada di DEFAULTS.
    """

    # a) Normalisasi ke Title Case untuk kolom kategorik non-numerik
    title_case_cols = [
        c for c in ["industry", "status", "product_line", "document_type", "env_category_code"]
        if c in df.columns
    ]

    for col in title_case_cols:
        original = df[col].copy()
        # Hanya Title Case jika semua huruf kecil (hindari merusak "FI-1" dll.)
        df[col] = df[col].apply(_safe_title_case)
        changed = (original != df[col]) & original.notna() & original.ne("")
        stats.category_normalized[col] = int(changed.sum())
        logger.debug(f"  Langkah 5 Title Case [{col}]: {stats.category_normalized[col]} baris")

    # b) String kosong → "Unspecified"
    for col, default_val in DEFAULTS.items():
        if col not in df.columns:
            continue
        is_empty = df[col].str.strip().eq("") | df[col].isna()
        count_empty = int(is_empty.sum())
        stats.empty_to_unspecified[col] = count_empty
        df[col] = df[col].where(~is_empty, other=default_val)
        logger.debug(f"  Langkah 5 Default [{col}]: {count_empty} → '{default_val}'")

    # c) Tambah kolom label deskriptif untuk env_category_code
    if "env_category_code" in df.columns:
        # Ekstrak kode singkat dari format seperti "B - Limited" -> "B"
        df["env_category_code"] = df["env_category_code"].apply(
            lambda x: str(x).split(" - ")[0].strip() if pd.notna(x) and " - " in str(x) else x
        )
        df["env_category_label"] = df["env_category_code"].map(
            ENV_CATEGORY_LABELS
        ).fillna("Unspecified")

    return df


def _safe_title_case(value: str) -> str:
    """
    Title Case hanya jika semua karakter adalah huruf kecil.
    Pertahankan nilai seperti "FI-1", "Active", "HOLD" apa adanya.
    """
    if not isinstance(value, str) or value == "":
        return value
    if value == value.lower():
        return value.title()
    return value


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING KE FILE
# ─────────────────────────────────────────────────────────────────────────────

async def _write_log(stats: TransformStats) -> None:
    """Tulis ringkasan anomali ke file log secara async."""
    import aiofiles
    lines = stats.to_log_lines()
    content = "\n".join(lines) + "\n"

    async with aiofiles.open(LOG_FILE, mode="a", encoding="utf-8") as f:
        await f.write(content)

    # Juga print ke logger
    for line in lines:
        logger.info(line)


# ─────────────────────────────────────────────────────────────────────────────
# QUICK TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    from etl.extract import extract_all

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

    async def _test():
        batches = await extract_all()
        cleaned = await transform_all(batches)
        for label, df in cleaned.items():
            print(f"\nBatch {label}: {len(df)} baris")
            print(df.dtypes)
            print(df.head(3).to_string())

    asyncio.run(_test())