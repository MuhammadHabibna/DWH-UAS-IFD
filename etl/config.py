"""
config.py — Konfigurasi terpusat pipeline ETL IFC Investment DWH
Mata Kuliah: Data Warehouse | UAS SADA 2026
"""

import os
from pathlib import Path

# DATABASE

DB_URI = "postgresql://postgres.bbbszbykqcxrxnfszvmc:DWH-UAS-IFC@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres"

# Alias agar kode Anggota 1 (extract/transform) tetap jalan
DATABASE_URL = DB_URI

# PATH

# Root proyek: satu level di atas folder etl/
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Path CSV — milik Anggota 2, kita ikuti namanya
DATASET_PATH = "ifc_investment_services_projects.csv"

# Alias Path object untuk dipakai Anggota 1
CSV_PATH = PROJECT_ROOT / DATASET_PATH
if not CSV_PATH.exists():
    # Jika tidak ketemu, coba cari di folder induk (satu level di atas)
    alt_path = PROJECT_ROOT.parent / DATASET_PATH
    if alt_path.exists():
        CSV_PATH = alt_path


# Folder log transformasi (dibuat otomatis)
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "transform_log.txt"

# RENTANG TAHUN

START_YEAR = 1994
END_YEAR   = 2026

# Turunan untuk kebutuhan batch per dekade (Anggota 1)
DECADE_BATCHES: list[tuple[int, int]] = [
    (1994, 1999),
    (2000, 2009),
    (2010, 2019),
    (2020, 2029),
]


# NAMA KOLOM CSV → NAMA INTERNAL


DATE_BATCH_COLUMN = "Date Disclosed"

COLUMN_MAP: dict[str, str] = {
    # Identitas proyek
    "Project Number":                                           "project_number",
    "Project Name":                                             "project_name",
    "Document Type":                                            "document_type",
    "Project Url":                                              "project_url",
    # Perusahaan & lokasi
    "Company Name":                                             "company_name",
    "Country":                                                  "country",
    "IFC Country Code":                                         "ifc_country_code",
    "WB Country Code":                                          "wb_country_code",
    # Industri & kategori
    "Industry":                                                 "industry",
    "Department":                                               "department",
    "Environmental Category":                                   "env_category_code",
    # Status & produk
    "Status":                                                   "status",
    "Product Line":                                             "product_line",
    # Tanggal
    "Date Disclosed":                                           "date_disclosed",
    "Projected Board Date":                                     "projected_board_date",
    "IFC Approval Date":                                        "approval_date",
    "IFC Signed Date":                                          "signed_date",
    "IFC Invested Date":                                        "invested_date",
    # Numerik / measures
    "IFC investment for Loan(Million - USD)":                   "loan_usd_million",
    "IFC investment for Equity(Million - USD)":                 "equity_usd_million",
    "IFC investment for Guarantee(Million - USD)":              "guarantee_usd_million",
    "IFC investment for Risk Management(Million - USD)":        "risk_mgmt_usd_million",
    "Total IFC investment as approved by Board(Million - USD)": "total_investment_usd_million",
    # Metadata
    "As of Date":                                               "as_of_date",
}

DATE_COLUMNS: list[str] = [
    "date_disclosed",
    "projected_board_date",
    "approval_date",
    "signed_date",
    "invested_date",
]

NUMERIC_COLUMNS: list[str] = [
    "loan_usd_million",
    "equity_usd_million",
    "guarantee_usd_million",
    "risk_mgmt_usd_million",
    "total_investment_usd_million",
]


# LABEL & DEFAULT KATEGORI


ENV_CATEGORY_LABELS: dict[str, str] = {
    "A":    "High Risk",
    "B":    "Medium Risk",
    "C":    "Low Risk",
    "FI":   "Financial Intermediary",
    "FI-1": "Financial Intermediary - High Risk",
    "FI-2": "Financial Intermediary - Medium Risk",
    "FI-3": "Financial Intermediary - Low Risk",
}

DEFAULTS: dict[str, str] = {
    "product_line":      "Unspecified",
    "env_category_code": "U",
    "industry":          "Unspecified",
    "department":        "Unspecified",
    "status":            "Unspecified",
    "document_type":     "Unspecified",
}