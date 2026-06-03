-- ============================================================================
-- UAS SADA 2026 — PROGRAM STUDI S1 SAINS DATA UNESA
-- File: extensions.sql
-- Deskripsi: Mengaktifkan ekstensi PostgreSQL untuk optimasi dan monitoring OLAP
-- ============================================================================

-- Ekstensi untuk mencatat statistik eksekusi query (sangat berguna untuk benchmark performa)
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Ekstensi untuk mendukung indeks GIN pada tipe data dasar (mengoptimalkan pencarian komposit multidimensi)
CREATE EXTENSION IF NOT EXISTS btree_gin;
