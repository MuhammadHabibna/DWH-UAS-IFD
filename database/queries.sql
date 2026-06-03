-- ============================================================================
-- UAS SADA 2026 — PROGRAM STUDI S1 SAINS DATA UNESA
-- File: queries.sql
-- Deskripsi: Skema tabel Flat pembanding dan definisi query OLAP benchmark
--            untuk membandingkan performa arsitektur DWH
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. PEMBUATAN TABEL FLAT (Tanpa Partisi, Tanpa Indeks)
-- ----------------------------------------------------------------------------
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

-- ----------------------------------------------------------------------------
-- 2. QUERY 1: ANALISIS SEKTOR INDUSTRI PER TAHUN
-- ----------------------------------------------------------------------------

-- A. Versi Flat (Tabel Tunggal Tanpa Optimasi)
-- SELECT 
--     EXTRACT(YEAR FROM date_disclosed) AS year,
--     industry,
--     department,
--     COUNT(*) AS total_projects,
--     SUM(loan_usd_million) AS total_loan,
--     SUM(equity_usd_million) AS total_equity,
--     SUM(total_investment_usd_million) AS total_investment
-- FROM flat_investment
-- WHERE date_disclosed IS NOT NULL
-- GROUP BY year, industry, department
-- ORDER BY year DESC, total_investment DESC;

-- B. Versi Star Schema (JOIN Multi-tabel + Partisi + Indeks)
-- SELECT 
--     d.year,
--     ind.industry,
--     ind.department,
--     COUNT(f.project_number) AS total_projects,
--     SUM(f.loan_usd_million) AS total_loan,
--     SUM(f.equity_usd_million) AS total_equity,
--     SUM(f.total_investment_usd_million) AS total_investment
-- FROM fact_investment f
-- JOIN dim_date d ON f.date_disclosed = d.date_id
-- JOIN dim_industry ind ON f.industry_id = ind.industry_id
-- GROUP BY d.year, ind.industry, ind.department
-- ORDER BY d.year DESC, total_investment DESC;

-- C. Versi Materialized View (Agregasi Tersimpan)
-- SELECT 
--     year,
--     industry,
--     department,
--     total_projects,
--     total_loan_usd_million AS total_loan,
--     total_equity_usd_million AS total_equity,
--     total_investment_usd_million AS total_investment
-- FROM mv_industry_yearly_summary
-- ORDER BY year DESC, total_investment DESC;


-- ----------------------------------------------------------------------------
-- 3. QUERY 2: ANALISIS INVESTASI NEGARA PER DEKADE
-- ----------------------------------------------------------------------------

-- A. Versi Flat (Tabel Tunggal Tanpa Optimasi)
-- SELECT 
--     (EXTRACT(YEAR FROM date_disclosed)::int / 10) * 10 AS decade,
--     country,
--     COUNT(*) AS total_projects,
--     SUM(total_investment_usd_million) AS total_investment,
--     AVG(total_investment_usd_million) AS avg_investment
-- FROM flat_investment
-- WHERE date_disclosed IS NOT NULL
-- GROUP BY decade, country
-- ORDER BY decade DESC, total_investment DESC;

-- B. Versi Star Schema (JOIN Multi-tabel + Partisi + Indeks)
-- SELECT 
--     d.decade,
--     loc.country,
--     COUNT(f.project_number) AS total_projects,
--     SUM(f.total_investment_usd_million) AS total_investment,
--     AVG(f.total_investment_usd_million) AS avg_investment
-- FROM fact_investment f
-- JOIN dim_date d ON f.date_disclosed = d.date_id
-- JOIN dim_location loc ON f.location_id = loc.location_id
-- GROUP BY d.decade, loc.country
-- ORDER BY d.decade DESC, total_investment DESC;

-- C. Versi Materialized View (Agregasi Tersimpan)
-- SELECT 
--     decade,
--     country,
--     total_projects,
--     total_investment_usd_million AS total_investment,
--     avg_investment_usd_million AS avg_investment
-- FROM mv_country_investment_summary
-- ORDER BY decade DESC, total_investment DESC;
