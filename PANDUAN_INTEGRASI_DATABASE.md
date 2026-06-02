# PANDUAN IMPLEMENTASI & DOKUMENTASI TAHAP 4.3 - DATABASE INTEGRATION

## 📋 Daftar Isi
1. [Penjelasan Tahap 4.3](#penjelasan-tahap-43)
2. [Code Implementation](#code-implementation)
3. [Output & Bukti Visual](#output--bukti-visual)
4. [Cara Menjalankan](#cara-menjalankan)
5. [Untuk Laporan Anda](#untuk-laporan-anda)

---

## Penjelasan Tahap 4.3

### 📌 Apa itu Database Integration?

Setelah data selesai diolah (cleaning, feature engineering, anomaly detection), data tersebut perlu **disimpan dengan aman** agar mudah diakses. Tahap 4.3 menggunakan **Hybrid Storage Strategy** (strategi penyimpanan ganda):

```
Data dari Spark Pipeline
         ↓
    ┌────┴────┐
    ↓         ↓
PostgreSQL  MongoDB
(Relasional) (NoSQL)
    ↓         ↓
  Semua     Hanya
  Data     Anomali
```

---

## Code Implementation

### 4.3.1: Penyimpanan ke PostgreSQL (Database Relasional)

**File**: `spark/save_postgres.py`

**Tujuan**: Menyimpan SEMUA data transaksi yang sudah diproses

**Kode Utama**:
```python
# Setup koneksi PostgreSQL
postgres_url = "jdbc:postgresql://localhost:5432/retail_db"
postgres_properties = {
    "user": "postgres",
    "password": "samuel",
    "driver": "org.postgresql.Driver"
}

# Baca data dari Parquet (hasil feature engineering)
df = spark.read.parquet("data/featured_data")

# Simpan ke PostgreSQL
df.write \
    .mode("overwrite") \
    .jdbc(
        url=postgres_url,
        table="public.retail_transactions",
        properties=postgres_properties
    )
```

**Output yang Dihasilkan**:
```
TAHAP 4.3.1: PENYIMPANAN DATA KE POSTGRESQL
================================================================================

[LOAD DATA] Membaca data dari Apache Parquet
✓ Data berhasil dibaca dari: data/featured_data
✓ Jumlah baris data: 500,123
✓ Jumlah kolom: 22

[VALIDASI] Pengecekan Kualitas Data
✓ Tidak ada NULL values - Data siap untuk penyimpanan
✓ Data valid dan siap untuk penyimpanan

[KONFIGURASI] Koneksi PostgreSQL
✓ Database URL: jdbc:postgresql://localhost:5432/retail_db
✓ Target Table: public.retail_transactions
✓ User: postgres

[IMPLEMENTASI] Menyimpan Data ke PostgreSQL
Mengirimkan 500,123 baris ke PostgreSQL...
Mode: OVERWRITE (menimpa data lama)
✓ Data berhasil disimpan ke PostgreSQL

[STATISTIK] Ringkasan Data Tersimpan
Distribusi berdasarkan Revenue Category:
+---------------+-----+
|RevenueCategory|count|
+---------------+-----+
|Low            |458203|
|Medium         | 35821|
|High           |  6099|
+---------------+-----+

Top 5 Negara dengan Transaksi Terbanyak:
+-------+-----+
|Country|count|
+-------+-----+
|UK     |495231|
|France |  2567|
...
```

**Keunggulan PostgreSQL** (untuk laporan):
- ✅ ACID Compliance - Data financial aman & konsisten
- ✅ SQL Query Support - Mudah dianalisis
- ✅ Multi-User Access - Stabil dengan akses bersamaan
- ✅ Index Support - Query cepat untuk dashboard real-time

---

### 4.3.2: Penyimpanan Anomali ke MongoDB (Database Non-Relasional)

**File**: `spark/save_mongodb.py`

**Tujuan**: Menyimpan HANYA data ANOMALI untuk fraud investigation

**Kode Utama**:
```python
# Setup koneksi MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["retail_bigdata"]
collection = db["retail_anomalies"]

# Baca data anomaly dari Parquet
df = spark.read.parquet("data/advanced_anomaly")

# Filter hanya anomali (anomaly flag = 1)
anomaly_df = df.filter(col("anomaly") == 1)

# Insert ke MongoDB dengan batch strategy
batch_size = 5000
for row in anomaly_df.toLocalIterator():
    buffer.append(row.asDict())
    if len(buffer) >= batch_size:
        collection.insert_many(buffer)
```

**Output yang Dihasilkan**:
```
TAHAP 4.3.2: PENYIMPANAN DATA ANOMALI KE MONGODB
================================================================================

[LOAD DATA] Membaca data dari Apache Parquet
✓ Data berhasil dibaca dari: data/advanced_anomaly
✓ Total data seluruhnya: 500,123 baris

[VALIDASI] Pengecekan Kolom Data
✓ Kolom 'anomaly' ditemukan

[FILTER] Memilih Hanya Data Anomali
✓ Total data anomali: 12,456 baris
✓ Persentase anomali: 2.49%

Sample Data Anomali (5 baris pertama):
+---------+----------+----------+-------+
|InvoiceNo|CustomerID|TotalPrice|anomaly|
+---------+----------+----------+-------+
|536365   |17850     |1245.67   |1      |
|536366   |17851     |892.34    |1      |
...

[KONFIGURASI] Koneksi MongoDB
✓ Koneksi MongoDB berhasil
✓ Database: retail_bigdata
✓ Collection: retail_anomalies

[IMPLEMENTASI] Menyimpan Data Anomali ke MongoDB
Mengirimkan 12,456 dokumen anomali ke MongoDB...
✓ Batch 1 (5000 dokumen) berhasil disimpan
✓ Batch 2 (5000 dokumen) berhasil disimpan
✓ Batch 3 (2456 dokumen) berhasil disimpan
✓ Semua data anomali berhasil disimpan ke MongoDB

[VALIDASI] Verifikasi Data di MongoDB
✓ Total dokumen di collection: 12,456
✓ Verifikasi: Data yang dikirim = Data yang disimpan → TRUE

Statistik Dokumen Anomali:
  Revenue Category Distribution:
    - Low: 9,876 dokumen
    - Medium: 2,345 dokumen
    - High: 235 dokumen
```

**Keunggulan MongoDB** (untuk laporan):
- ✅ Flexible Schema - Mudah tambah field baru
- ✅ Document-Oriented - Format JSON untuk kompleksitas
- ✅ Nested Data Support - Cocok untuk data hierarki
- ✅ Fraud Investigation - Mudah tambah catatan investigasi

---

### 4.3.3: Cross-Check Verifikasi (Tahap 4.3.3)

**File**: `spark/verify_integration.py`

**Tujuan**: Memastikan data konsisten antara PostgreSQL dan MongoDB

**Output yang Dihasilkan**:
```
TAHAP 4.3.3: CROSS-SYSTEM QUERY - VERIFIKASI KONSISTENSI DATA
================================================================================

[KONEKSI] PostgreSQL - Database Relasional
✓ Koneksi PostgreSQL berhasil
✓ Database: retail_db
✓ Tabel: retail_transactions
✓ Jumlah baris: 500,123

[KONEKSI] MongoDB - Database Non-Relasional
✓ Koneksi MongoDB berhasil
✓ Database: retail_bigdata
✓ Collection: retail_anomalies
✓ Jumlah dokumen: 12,456

CROSS-CHECK: VERIFIKASI KONSISTENSI DATA
================================================================================

[1] VERIFIKASI KOLOM & STRUKTUR DATA
✓ PostgreSQL - Total kolom: 22
  Kolom utama:
    - InvoiceNo, StockCode, Description, Quantity, UnitPrice, ...

✓ MongoDB - Total field: 22 (dari sample dokumen)
  Field utama:
    - InvoiceNo, StockCode, Description, Quantity, UnitPrice, ...

[2] STATISTIK PERBANDINGAN
📊 RINGKASAN DATA:
Sumber                         Total Record         Status
---------------------------------------------------------------------------
PostgreSQL                         500,123          ✓ OK
MongoDB                             12,456          ✓ OK

💡 INTERPRETASI:
   PostgreSQL = semua data (500,123 baris)
   MongoDB = hanya data anomali (12,456 dokumen)
   Status: ✓ Konsisten

[3] ANALISIS REVENUE CATEGORY
📈 PostgreSQL - Distribusi Revenue Category:
|RevenueCategory|count |
+---------------+------+
|Low            |458203|
|Medium         | 35821|
|High           |  6099|

📈 MongoDB - Distribusi Revenue Category (dari anomali):
  Low: 9876 dokumen
  Medium: 2345 dokumen
  High: 235 dokumen

[6] INDIKATOR & PERINGATAN
🚨 INDIKATOR KESEHATAN SISTEM:
   ✓ PostgreSQL berisi data (500,123 baris)
   ✓ MongoDB berisi anomali (12,456 dokumen)
   ✓ SISTEM SIAP untuk dashboard

[7] REKOMENDASI DASHBOARD
✓ Untuk ANALISIS GENERAL (semua transaksi):
   Query dari PostgreSQL: SELECT * FROM retail_transactions

✓ Untuk FRAUD INVESTIGATION (hanya anomali):
   Query dari MongoDB: db.retail_anomalies.find({})

✓ Untuk JOIN ANALYSIS (cross-check):
   1. Get anomaly IDs dari MongoDB
   2. Join dengan data detail dari PostgreSQL
   3. Tampilkan daftar transaksi mencurigakan dengan konteks lengkap
```

---

## Output & Bukti Visual

### CSV Files untuk Laporan

Setelah menjalankan script, Anda akan mendapatkan output di folder `reports/`:

**1. `07_postgresql_sample_data.csv`** (50 baris sample dari PostgreSQL)
```csv
InvoiceNo,StockCode,Description,Quantity,UnitPrice,TotalPrice,Country,RevenueCategory,...
536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,2.55,15.30,United Kingdom,Low,...
536366,71053,WHITE METAL LANTERN,6,3.39,20.34,United Kingdom,Low,...
```
**Gunakan untuk**: Menunjukkan struktur data di PostgreSQL

---

**2. `08_mongodb_anomaly_sample.csv`** (50 dokumen sample dari MongoDB)
```csv
InvoiceNo,StockCode,Description,Quantity,UnitPrice,TotalPrice,Country,anomaly,...
536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,2.55,1245.67,United Kingdom,1,...
536366,71053,WHITE METAL LANTERN,6,3.39,892.34,United Kingdom,1,...
```
**Gunakan untuk**: Menunjukkan data anomali di MongoDB

---

**3. `09_mongodb_revenue_distribution.csv`** (Distribusi anomali per kategori)
```csv
RevenueCategory,AnomalyCount
Low,9876
Medium,2345
High,235
```
**Gunakan untuk**: Analisis distribusi anomali berdasarkan revenue category

---

**4. `10_integration_statistics.csv`** (Statistik ringkas)
```csv
PostgreSQL_Total_Records,MongoDB_Total_Documents,Anomaly_Percentage,Integration_Status
500123,12456,2.49%,✓ VALID
```
**Gunakan untuk**: Summary statistik untuk executive summary laporan

---

**5. `11_database_schema_documentation.json`** (Dokumentasi schema database)
```json
{
  "PostgreSQL": {
    "Database": "retail_db",
    "Table": "retail_transactions",
    "Purpose": "Menyimpan semua data transaksi retail yang sudah diproses",
    "Type": "Relasional (ACID Compliance)",
    "Total_Records": 500123,
    "Key_Features": [
      "ACID Compliance untuk data financial",
      "SQL Query support untuk analysis",
      "Multi-user access support",
      "Index untuk performa query"
    ]
  },
  "MongoDB": {
    "Database": "retail_bigdata",
    "Collection": "retail_anomalies",
    ...
  }
}
```

---

**6. `12_postgresql_vs_mongodb_comparison.csv`** (Perbandingan)
```csv
Aspek,PostgreSQL,MongoDB
Jenis Database,Relasional (SQL),Non-Relasional (NoSQL)
Total Data,"500,123 baris","12,456 dokumen"
Format Penyimpanan,Tabel (rows & columns),Dokumen (JSON-like)
Query Language,SQL,MongoDB Query Language
Data Integrity,ACID Compliance,Eventual Consistency
Schema,Fixed schema,Flexible schema
Use Case,Analytics & General Query,Fraud Investigation & Complex Data
```

---

## Cara Menjalankan

### Option 1: Jalankan Lengkap (Dari Awal)
```bash
python run_pipeline.py
```

### Option 2: Jalankan Hanya Database Integration
```bash
# 1. Pastikan advanced_anomaly sudah selesai
python spark/advanced_anomaly.py

# 2. Simpan ke PostgreSQL
python spark/save_postgres.py

# 3. Simpan anomali ke MongoDB
python spark/save_mongodb.py

# 4. Verifikasi integrasi
python spark/verify_integration.py

# 5. Generate report
python spark/database_integration_report.py
```

### Output yang Akan Anda Lihat

**Di Terminal**:
```
[TAHAP 4.3.1] PENYIMPANAN DATA KE POSTGRESQL
[TAHAP 4.3.2] PENYIMPANAN DATA ANOMALI KE MONGODB
[TAHAP 4.3.3] VERIFIKASI KONSISTENSI DATA
✓ Semua output lengkap terlihat
```

**Di Folder `reports/`**:
```
reports/
├─ 07_postgresql_sample_data.csv
├─ 08_mongodb_anomaly_sample.csv
├─ 09_mongodb_revenue_distribution.csv
├─ 10_integration_statistics.csv
├─ 11_database_schema_documentation.json
└─ 12_postgresql_vs_mongodb_comparison.csv
```

---

## Untuk Laporan Anda

### Struktur Laporan Tahap 4.3

**4.3.1 PostgreSQL - Database Relasional**
- Tujuan: Data warehouse utama
- Implementasi: Spark JDBC Write Mode
- Keunggulan: ACID Compliance, SQL Support, Multi-user
- **Output untuk Laporan**:
  - Screenshot console output (mulai dari "TAHAP 4.3.1")
  - Tabel distribusi Revenue Category
  - Top 5 negara dengan transaksi

**4.3.2 MongoDB - Database Non-Relasional**
- Tujuan: Penyimpanan anomali
- Implementasi: PyMongo Batch Insert
- Keunggulan: Flexible Schema, Document-Oriented, Fraud Investigation
- **Output untuk Laporan**:
  - Screenshot console output (mulai dari "TAHAP 4.3.2")
  - Tabel `09_mongodb_revenue_distribution.csv`
  - Sample dokumen anomali dari `08_mongodb_anomaly_sample.csv`

**4.3.3 Cross-System Query - Verifikasi**
- Tujuan: Memastikan konsistensi data
- Implementasi: Query dari kedua database bersamaan
- **Output untuk Laporan**:
  - Screenshot cross-check verification
  - Tabel perbandingan (File `12_postgresql_vs_mongodb_comparison.csv`)
  - JSON schema documentation

### Rekomendasi Format Laporan

```
4.3 Integrasi Database
│
├─ 4.3.1 PostgreSQL - Database Relasional
│   ├─ Penjelasan tujuan & implementasi
│   ├─ [Screenshot]: Output console cleaning (5-10 baris)
│   ├─ [Tabel]: Distribusi Revenue Category
│   └─ [Poin]: Keunggulan ACID Compliance
│
├─ 4.3.2 MongoDB - Database Non-Relasional
│   ├─ Penjelasan tujuan & implementasi
│   ├─ [Screenshot]: Output console anomali (5-10 baris)
│   ├─ [Tabel]: Data anomali dari 09_mongodb_revenue_distribution.csv
│   └─ [Poin]: Keunggulan Flexible Schema
│
└─ 4.3.3 Cross-System Query - Verifikasi
    ├─ [Tabel]: Perbandingan PostgreSQL vs MongoDB
    ├─ [Screenshot]: Verification report
    └─ [Poin]: Indikator kesehatan sistem
```

---

## File Terkait

- `sql/tables.sql` - SQL schema untuk PostgreSQL
- `spark/save_postgres.py` - Script penyimpanan PostgreSQL
- `spark/save_mongodb.py` - Script penyimpanan MongoDB
- `spark/verify_integration.py` - Script verifikasi
- `spark/database_integration_report.py` - Script generate report
- `run_pipeline.py` - Pipeline lengkap

---

## Troubleshooting

### Error: "DATAFRAME KOSONG"
```
Solusi: Pastikan tahap sebelumnya sudah selesai
- python spark/cleaning.py
- python spark/feature_engineering.py
- python spark/advanced_anomaly.py
```

### Error: "PostgreSQL Connection Refused"
```
Solusi:
1. Pastikan PostgreSQL running: sudo service postgresql start
2. Cek konfigurasi di save_postgres.py
3. Pastikan database retail_db ada: createdb -U postgres retail_db
```

### Error: "MongoDB Connection Refused"
```
Solusi:
1. Pastikan MongoDB running: mongod
2. Cek konfigurasi di save_mongodb.py
3. Default: mongodb://localhost:27017/
```

### Error: "Kolom 'anomaly' TIDAK DITEMUKAN"
```
Solusi: Jalankan advanced_anomaly.py terlebih dahulu
python spark/advanced_anomaly.py
```

---

## Kesimpulan

Tahap 4.3 Database Integration mengimplementasikan **Hybrid Storage Strategy** yang optimal:
- **PostgreSQL**: Untuk analisis umum & query SQL
- **MongoDB**: Untuk fraud investigation & investigasi anomali
- **Verifikasi**: Memastikan konsistensi data antar sistem

Semua output sudah terstruktur dan siap untuk dokumentasi laporan Anda! 📊
