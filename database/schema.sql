-- ============================================================================
-- UAS SADA 2026 — PROGRAM STUDI S1 SAINS DATA UNESA
-- File: schema.sql
-- Deskripsi: Skema Bintang (Star Schema), Partisi Per Dekade, dan Materialized Views
--            untuk Data Warehouse Investasi IFC (1994-2026)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. PEMBUATAN TABEL DIMENSI
-- ----------------------------------------------------------------------------

-- Dimensi Proyek (dim_project)
CREATE TABLE IF NOT EXISTS dim_project (
    project_number VARCHAR(50) PRIMARY KEY,
    project_name VARCHAR(255) NOT NULL,
    document_type VARCHAR(100),
    project_url VARCHAR(500)
);

-- Dimensi Perusahaan (dim_company)
CREATE TABLE IF NOT EXISTS dim_company (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL UNIQUE
);

-- Dimensi Lokasi Geografis (dim_location)
CREATE TABLE IF NOT EXISTS dim_location (
    location_id SERIAL PRIMARY KEY,
    country VARCHAR(100) NOT NULL UNIQUE,
    ifc_country_code VARCHAR(10),
    wb_country_code VARCHAR(10)
);

-- Dimensi Sektor Industri & Departemen (dim_industry)
CREATE TABLE IF NOT EXISTS dim_industry (
    industry_id SERIAL PRIMARY KEY,
    industry VARCHAR(100) NOT NULL,
    department VARCHAR(100) NOT NULL,
    CONSTRAINT uq_industry_dept UNIQUE (industry, department)
);

-- Dimensi Jenis Produk Investasi (dim_product_line)
CREATE TABLE IF NOT EXISTS dim_product_line (
    product_line_id SERIAL PRIMARY KEY,
    product_line VARCHAR(100) NOT NULL UNIQUE
);

-- Dimensi Kategori Dampak Lingkungan (dim_env_category)
CREATE TABLE IF NOT EXISTS dim_env_category (
    env_category_id SERIAL PRIMARY KEY,
    category_code VARCHAR(50) NOT NULL UNIQUE,
    category_label VARCHAR(100) NOT NULL
);

-- Dimensi Waktu Detail (dim_date)
CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day_of_month INT NOT NULL,
    decade INT NOT NULL
);

-- ----------------------------------------------------------------------------
-- 2. PEMBUATAN TABEL FAKTA (TERPARTISI)
-- ----------------------------------------------------------------------------

-- Tabel Fakta Utama Investasi (fact_investment)
-- Dipartisi secara Range berdasarkan kolom date_disclosed
CREATE TABLE fact_investment (
    project_number VARCHAR(50) NOT NULL,
    company_id INT NOT NULL,
    location_id INT NOT NULL,
    industry_id INT NOT NULL,
    date_disclosed DATE NOT NULL,
    product_line_id INT NOT NULL,
    env_category_id INT NOT NULL,
    status VARCHAR(50) NOT NULL,
    projected_board_date DATE,
    approval_date DATE,
    signed_date DATE,
    invested_date DATE,
    risk_mgmt_usd_million NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
    guarantee_usd_million NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
    loan_usd_million NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
    equity_usd_million NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
    total_investment_usd_million NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
    
    -- Foreign Key constraints (PostgreSQL 12+ mendukung FK pada partitioned tables)
    CONSTRAINT fk_fact_project FOREIGN KEY (project_number) REFERENCES dim_project(project_number),
    CONSTRAINT fk_fact_company FOREIGN KEY (company_id) REFERENCES dim_company(company_id),
    CONSTRAINT fk_fact_location FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    CONSTRAINT fk_fact_industry FOREIGN KEY (industry_id) REFERENCES dim_industry(industry_id),
    CONSTRAINT fk_fact_date FOREIGN KEY (date_disclosed) REFERENCES dim_date(date_id),
    CONSTRAINT fk_fact_product FOREIGN KEY (product_line_id) REFERENCES dim_product_line(product_line_id),
    CONSTRAINT fk_fact_env FOREIGN KEY (env_category_id) REFERENCES dim_env_category(env_category_id)
) PARTITION BY RANGE (date_disclosed);

-- ----------------------------------------------------------------------------
-- 3. PEMBUATAN PARTISI PER DEKADE (4 PARTISI)
-- ----------------------------------------------------------------------------

-- Dekade 1: 1994-1999
CREATE TABLE fact_investment_1990s PARTITION OF fact_investment
    FOR VALUES FROM ('1994-01-01') TO ('2000-01-01');

-- Dekade 2: 2000-2009
CREATE TABLE fact_investment_2000s PARTITION OF fact_investment
    FOR VALUES FROM ('2000-01-01') TO ('2010-01-01');

-- Dekade 3: 2010-2019
CREATE TABLE fact_investment_2010s PARTITION OF fact_investment
    FOR VALUES FROM ('2010-01-01') TO ('2020-01-01');

-- Dekade 4: 2020-2029
CREATE TABLE fact_investment_2020s PARTITION OF fact_investment
    FOR VALUES FROM ('2020-01-01') TO ('2030-01-01');

-- ----------------------------------------------------------------------------
-- 4. INDEKS OPTIMASI OLAP (DIBUAT PADA TABEL FAKTA INDUK)
-- ----------------------------------------------------------------------------

-- BRIN Index pada kolom tanggal disclosure (sangat cepat untuk filter waktu berurutan)
CREATE INDEX idx_fact_date_brin ON fact_investment USING BRIN (date_disclosed);

-- B-Tree Index standar pada Foreign Keys untuk mengoptimalkan operasi JOIN
CREATE INDEX idx_fact_project_fk ON fact_investment (project_number);
CREATE INDEX idx_fact_company_fk ON fact_investment (company_id);
CREATE INDEX idx_fact_location_fk ON fact_investment (location_id);
CREATE INDEX idx_fact_industry_fk ON fact_investment (industry_id);
CREATE INDEX idx_fact_product_fk ON fact_investment (product_line_id);
CREATE INDEX idx_fact_env_fk ON fact_investment (env_category_id);

-- ----------------------------------------------------------------------------
-- 5. MATERIALIZED VIEWS UNTUK PERFORMA QUERY BENCHMARK
-- ----------------------------------------------------------------------------

-- A. Materialized View Ringkasan Investasi Industri per Tahun
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_industry_yearly_summary AS
SELECT 
    d.year,
    ind.industry,
    ind.department,
    COUNT(f.project_number) AS total_projects,
    SUM(f.loan_usd_million) AS total_loan_usd_million,
    SUM(f.equity_usd_million) AS total_equity_usd_million,
    SUM(f.total_investment_usd_million) AS total_investment_usd_million
FROM fact_investment f
JOIN dim_date d ON f.date_disclosed = d.date_id
JOIN dim_industry ind ON f.industry_id = ind.industry_id
GROUP BY d.year, ind.industry, ind.department
WITH DATA;

-- B. Materialized View Ringkasan Investasi Negara per Dekade
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_country_investment_summary AS
SELECT 
    d.decade,
    loc.country,
    COUNT(f.project_number) AS total_projects,
    SUM(f.total_investment_usd_million) AS total_investment_usd_million,
    AVG(f.total_investment_usd_million) AS avg_investment_usd_million
FROM fact_investment f
JOIN dim_date d ON f.date_disclosed = d.date_id
JOIN dim_location loc ON f.location_id = loc.location_id
GROUP BY d.decade, loc.country
WITH DATA;

-- Buat indeks unik pada materialized view untuk mendukung REFRESH secara CONCURRENT kelak
CREATE UNIQUE INDEX idx_mv_industry_yearly ON mv_industry_yearly_summary (year, industry, department);
CREATE UNIQUE INDEX idx_mv_country_decade ON mv_country_investment_summary (decade, country);
