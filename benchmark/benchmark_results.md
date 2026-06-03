# Laporan Hasil Benchmark Performa Database DWH

**Tanggal Pengujian**: 2026-06-01  
**Database**: PostgreSQL Cloud (Supabase)  
**Metrik**: Rata-rata dari 10 kali eksekusi (dalam milidetik - ms)  

Laporan ini membandingkan 3 skenario arsitektur:
1. **Flat Table** (Tabel tunggal hasil import mentah tanpa partisi dan indeks)
2. **Star Schema** (Dimensi terpisah + Fakta terpartisi per dekade + indeks B-Tree/BRIN)
3. **Materialized View** (Agregasi pra-hitung/tersimpan di database)

---

## Query 1: Analisis Sektor Industri per Tahun

| Skenario | Waktu DB Internal (ms) | Waktu Total + Jaringan (ms) | Peningkatan Kecepatan (DB Time) |
| :--- | :---: | :---: | :---: |
| **Flat Table** | 13.91 ms | 109.55 ms | (Baseline) |
| **Star Schema (Optimized)** | 16.48 ms | 115.21 ms | **-18.5% Lebih Cepat** |
| **Materialized View** | 2.21 ms | 112.97 ms | **84.1% Lebih Cepat** |

> [!TIP]
> Menggunakan **Materialized View** memangkas waktu proses di database menjadi **2.21 ms** dibandingkan **13.91 ms** pada tabel flat.

---

## Query 2: Analisis Investasi Negara per Dekade

| Skenario | Waktu DB Internal (ms) | Waktu Total + Jaringan (ms) | Peningkatan Kecepatan (DB Time) |
| :--- | :---: | :---: | :---: |
| **Flat Table** | 7.25 ms | 52.42 ms | (Baseline) |
| **Star Schema (Optimized)** | 10.40 ms | 49.05 ms | **-43.4% Lebih Cepat** |
| **Materialized View** | 0.73 ms | 39.59 ms | **89.9% Lebih Cepat** |

> [!TIP]
> Menggunakan **Materialized View** memangkas waktu proses di database menjadi **0.73 ms** dibandingkan **7.25 ms** pada tabel flat.

---
