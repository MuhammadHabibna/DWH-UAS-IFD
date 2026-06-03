# Pembangunan Data Warehouse End-to-End Investasi IFC (UAS SADA 2026)

Proyek ini bertujuan untuk membangun arsitektur Data Warehouse (DWH) secara *end-to-end* untuk menganalisis data proyek investasi global dari **International Finance Corporation (IFC)**, anggota World Bank Group, periode tahun 1994 hingga 2026.

## Kelompok Proyek
* **Tazkia Caecaria Marchanda**
* **Alya Ummi Faricha**
* **Muhammad Habib Nur Aiman**

---

## Teknologi & Fitur Utama
1. **Database Cloud:** PostgreSQL 15 di-hosting pada layanan cloud **Supabase**.
2. **Desain Skema:** Model *Star Schema* dengan 1 tabel fakta sentral (`fact_investment`) dan 7 tabel dimensi (`dim_project`, `dim_company`, `dim_location`, `dim_industry`, `dim_product_line`, `dim_env_category`, dan `dim_date`).
3. **Optimasi Database:** 
   * *Declarative Range Partitioning* (partisi tabel fakta per dekade).
   * Indeks BRIN pada tanggal dan B-Tree pada kolom foreign keys.
   * Agregasi pra-hitung menggunakan *Materialized Views* (`mv_industry_yearly_summary` dan `mv_country_investment_summary`).
4. **ETL Asinkron:** Pipeline ETL dibangun menggunakan Python `asyncio`, `asyncpg`, dan `pandas` untuk proses non-blocking dengan performa tinggi.
5. **OLAP Engine:** Menggunakan framework in-memory **Atoti** untuk pemodelan Cube analitik interaktif.

---

## Struktur Repositori
* `database/` : File DDL untuk skema, indeks, partisi, extensions, dan kueri analitis.
* `etl/` : Modul asinkron Python untuk ekstraksi, transformasi, pemuatan data, dan berkas CSV dataset.
* `atoti/` : File Python dan Jupyter Notebook untuk penyusunan DataMart OLAP.
* `benchmark/` : File skrip pengujian performa query perbandingan flat table vs optimized schema.

---

## Cara Menjalankan

### 1. Inisialisasi Database
Jalankan file SQL di folder `database/` ke server PostgreSQL Anda:
* `extensions.sql` (mengaktifkan ekstensi)
* `schema.sql` (membuat tabel dimensi, fakta, partisi, indeks, dan materialized views)

### 2. Jalankan Pipeline ETL
```bash
python -m etl.main
```

### 3. Jalankan Pengujian Benchmark Performa
```bash
python benchmark/run_benchmark.py
```

### 4. Jalankan Server OLAP Atoti
```bash
python atoti/datamart.py
```
Akses dashboard OLAP Atoti melalui browser di alamat: `http://localhost:9090`
