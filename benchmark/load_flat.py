import asyncio
import logging
import sys
import time
import html
from pathlib import Path
import pandas as pd
import asyncpg

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("load_flat")

DB_URI = "postgresql://postgres.bbbszbykqcxrxnfszvmc:DWH-UAS-IFC@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATASET_PATH = PROJECT_ROOT / "ifc_investment_services_projects.csv"

# Column maps & default cleaning parameters
COLUMN_MAP = {
    "Project Number":                                           "project_number",
    "Project Name":                                             "project_name",
    "Document Type":                                            "document_type",
    "Project Url":                                              "project_url",
    "Company Name":                                             "company_name",
    "Country":                                                  "country",
    "IFC Country Code":                                         "ifc_country_code",
    "WB Country Code":                                          "wb_country_code",
    "Industry":                                                 "industry",
    "Department":                                               "department",
    "Environmental Category":                                   "env_category",
    "Status":                                                   "status",
    "Product Line":                                             "product_line",
    "Date Disclosed":                                           "date_disclosed",
    "Projected Board Date":                                     "projected_board_date",
    "IFC Approval Date":                                        "approval_date",
    "IFC Signed Date":                                          "signed_date",
    "IFC Invested Date":                                        "invested_date",
    "IFC investment for Loan(Million - USD)":                   "loan_usd_million",
    "IFC investment for Equity(Million - USD)":                 "equity_usd_million",
    "IFC investment for Guarantee(Million - USD)":              "guarantee_usd_million",
    "IFC investment for Risk Management(Million - USD)":        "risk_mgmt_usd_million",
    "Total IFC investment as approved by Board(Million - USD)": "total_investment_usd_million",
    "As of Date":                                               "as_of_date",
}

DATE_COLUMNS = [
    "date_disclosed",
    "projected_board_date",
    "approval_date",
    "signed_date",
    "invested_date",
]

NUMERIC_COLUMNS = [
    "loan_usd_million",
    "equity_usd_million",
    "guarantee_usd_million",
    "risk_mgmt_usd_million",
    "total_investment_usd_million",
]

def clean_text(text):
    if not isinstance(text, str):
        return ""
    text_decoded = html.unescape(text)
    return " ".join(text_decoded.split())

def convert_date(date_str):
    if not isinstance(date_str, str) or date_str.strip() == "":
        return None
    try:
        dt = pd.to_datetime(date_str, format="%m/%d/%Y", errors="coerce")
        if pd.isna(dt):
            return None
        return dt.date()
    except Exception:
        return None

def convert_numeric(val):
    if pd.isna(val) or str(val).strip() == "":
        return 0.0
    try:
        return float(str(val).strip())
    except ValueError:
        return 0.0

async def load_flat_table():
    logger.info("=== PROSES PENGUNGGAHAN TABEL FLAT DIMULAI ===")
    
    if not DATASET_PATH.exists():
        logger.error(f"Dataset tidak ditemukan di {DATASET_PATH}")
        sys.exit(1)
        
    logger.info("Membaca CSV...")
    df = pd.read_csv(DATASET_PATH, encoding="utf-8-sig", dtype=str, keep_default_na=False)
    
    # Hapus baris kosong
    df.dropna(how="all", inplace=True)
    
    logger.info("Melakukan pembersihan data...")
    # Rename kolom
    cols_to_keep = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    df = df[list(cols_to_keep.keys())].rename(columns=cols_to_keep)
    
    # Clean text columns
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        if col not in DATE_COLUMNS and col not in NUMERIC_COLUMNS:
            df[col] = df[col].apply(clean_text)
            
    # Clean numeric columns
    for col in NUMERIC_COLUMNS:
        df[col] = df[col].apply(convert_numeric)
        
    # Clean date columns
    for col in DATE_COLUMNS:
        df[col] = df[col].apply(convert_date)
        
    # Set default values for empty string columns
    df["product_line"] = df["product_line"].replace("", "Unspecified")
    df["env_category"] = df["env_category"].replace("", "Unspecified")
    df["industry"] = df["industry"].replace("", "Unspecified")
    df["department"] = df["department"].replace("", "Unspecified")
    df["status"] = df["status"].replace("", "Unspecified")
    df["document_type"] = df["document_type"].replace("", "Unspecified")
    
    # Convert dataframe to tuples list for inserting
    rows = []
    for _, r in df.iterrows():
        rows.append((
            r["project_number"], r["project_name"], r.get("document_type", "Unspecified"), r.get("project_url", ""),
            r.get("company_name", "Unknown Company"), r.get("country", "Unknown Country"), 
            r.get("ifc_country_code", ""), r.get("wb_country_code", ""),
            r.get("industry", "Unspecified"), r.get("department", "Unspecified"),
            r.get("env_category", "Unspecified"), r.get("status", "Unspecified"),
            r.get("product_line", "Unspecified"), r["date_disclosed"],
            r["projected_board_date"], r["approval_date"], r["signed_date"], r["invested_date"],
            float(r["loan_usd_million"]), float(r["equity_usd_million"]),
            float(r["guarantee_usd_million"]), float(r["risk_mgmt_usd_million"]),
            float(r["total_investment_usd_million"]), r.get("as_of_date", "")
        ))

    logger.info("Menghubungkan ke Supabase...")
    conn = await asyncpg.connect(DB_URI)
    
    # Create Table
    logger.info("Membuat tabel flat_investment jika belum ada...")
    create_table_ddl = """
    CREATE TABLE IF NOT EXISTS flat_investment (
        project_number VARCHAR(100),
        project_name VARCHAR(500),
        document_type VARCHAR(200),
        project_url VARCHAR(1000),
        company_name VARCHAR(255),
        country VARCHAR(150),
        ifc_country_code VARCHAR(50),
        wb_country_code VARCHAR(50),
        industry VARCHAR(150),
        department VARCHAR(150),
        env_category VARCHAR(150),
        status VARCHAR(100),
        product_line VARCHAR(150),
        date_disclosed DATE,
        projected_board_date DATE,
        approval_date DATE,
        signed_date DATE,
        invested_date DATE,
        loan_usd_million NUMERIC(15,2),
        equity_usd_million NUMERIC(15,2),
        guarantee_usd_million NUMERIC(15,2),
        risk_mgmt_usd_million NUMERIC(15,2),
        total_investment_usd_million NUMERIC(15,2),
        as_of_date VARCHAR(50)
    );
    """
    await conn.execute(create_table_ddl)
    
    # Kosongkan tabel terlebih dahulu agar tidak duplikat jika dirun ulang
    logger.info("Mengosongkan tabel flat_investment...")
    await conn.execute("TRUNCATE TABLE flat_investment;")
    
    # Insert massal
    logger.info(f"Mengunggah {len(rows):,} baris ke flat_investment...")
    insert_sql = """
    INSERT INTO flat_investment (
        project_number, project_name, document_type, project_url,
        company_name, country, ifc_country_code, wb_country_code,
        industry, department, env_category, status,
        product_line, date_disclosed, projected_board_date, approval_date,
        signed_date, invested_date, loan_usd_million, equity_usd_million,
        guarantee_usd_million, risk_mgmt_usd_million, total_investment_usd_million,
        as_of_date
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24);
    """
    t0 = time.perf_counter()
    await conn.executemany(insert_sql, rows)
    dur = time.perf_counter() - t0
    
    logger.info(f"Berhasil mengunggah {len(rows):,} baris dalam {dur:.2f} detik!")
    await conn.close()
    logger.info("=== PROSES PENGUNGGAHAN SELESAI ===")

if __name__ == "__main__":
    asyncio.run(load_flat_table())
