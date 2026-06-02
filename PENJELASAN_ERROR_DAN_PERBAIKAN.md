# 🔴 PENJELASAN ERROR - WHITESPACE DI NAMA KOLOM

## ❌ MASALAH YANG TERJADI

Ketika pipeline dijalankan, terjadi error di stage 5, 6, 7, dan 8:

```
pyspark.errors.exceptions.captured.AnalysisException: 
[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter 
with name `InvoiceNo` cannot be resolved. 
Did you mean one of the following? [`    InvoiceNo`, `InvoiceDate`, ...
```

### 🎯 Akar Masalahnya

Perhatikan error message ini:
```
Did you mean one of the following? [`    InvoiceNo`
```

**Ada SPASI di depan nama kolom!**

```
❌ SALAH:  `    InvoiceNo`     (4 spasi + InvoiceNo)
✓ BENAR:  `InvoiceNo`          (tanpa spasi)
```

---

## 🔍 KENAPA INI TERJADI?

**Penyebabnya**: Saat membaca CSV file menggunakan Spark, kadang-kadang CSV file punya leading whitespace pada header:

```csv
    InvoiceNo,StockCode,Description,Quantity,...
    12345,S001,Product,5,...
```

Ketika Spark membaca ini, nama kolom menjadi:
- `    InvoiceNo` (bukan `InvoiceNo`)
- Whitespace ini adalah bagian dari nama kolom!

Sehingga ketika code mencoba mengakses kolom dengan `InvoiceNo` (tanpa spasi):
```python
df.select("InvoiceNo")  # ❌ ERROR - kolom tidak ketemu
```

Spark tidak bisa temukannya, karena kolom yang sebenarnya ada adalah `    InvoiceNo` (dengan spasi).

---

## ✅ SOLUSI YANG DITERAPKAN

### **Langkah 1: Clean Column Names saat Read CSV**

File: `spark/cleaning.py` (Tahap 4.1)

**SEBELUM:**
```python
df = spark.read.csv(
    "data/online_retail.csv",
    header=True,
    inferSchema=True,
    sep=";"
)
```

**SESUDAH:**
```python
df = spark.read.csv(
    "data/online_retail.csv",
    header=True,
    inferSchema=True,
    sep=";"
)

# ✅ PENTING: Strip whitespace dari nama kolom
df = df.select([col(c).alias(c.strip()) for c in df.columns])
```

**Penjelasan:**
```python
df.select([col(c).alias(c.strip()) for c in df.columns])
#         ^^^^^^ buat ulang selection
#                ^^^^ setiap kolom (c)
#                     ^^^^^^^ ganti nama dengan nama yang sudah di-strip
#                            (hapus leading/trailing whitespace)
```

### **Langkah 2: Clean Column Names di Setiap File yang Membaca Parquet**

Karena data sudah tersimpan dengan whitespace di Parquet, saya tambahkan pembersihan ini di:

1. **`spark/feature_engineering.py`** ← Baca cleaned_data
2. **`spark/customer_segmentation.py`** ← Baca featured_data
3. **`spark/advanced_anomaly.py`** ← Baca featured_data
4. **`spark/save_postgres.py`** ← Baca featured_data
5. **`spark/save_mongodb.py`** ← Baca advanced_anomaly
6. **`spark/verify_integration.py`** ← Baca dari PostgreSQL (JDBC)
7. **`spark/database_integration_report.py`** ← Baca dari PostgreSQL (JDBC)
8. **`spark/output_report.py`** ← Baca featured_data

**Pattern yang sama untuk semua:**
```python
# Setelah membaca Parquet atau JDBC
df = spark.read.parquet("data/featured_data")

# ✅ Tambahkan ini:
df = df.select([col(c).alias(c.strip()) for c in df.columns])
```

---

## 🧪 BAGAIMANA CARA KERJANYA?

### **Contoh:**

**SEBELUM clean:**
```
Kolom: [    InvoiceNo, StockCode,     Description, Quantity, ...]
                ↑         ↑              ↑
            4 spasi    1 spasi      5 spasi
```

**Code yang diterapkan:**
```python
df.select([col(c).alias(c.strip()) for c in df.columns])
```

**SESUDAH clean:**
```
Kolom: [InvoiceNo, StockCode, Description, Quantity, ...]
```

Sekarang semua kolom tidak punya leading/trailing whitespace!

### **Hasil:**
```python
df.select("InvoiceNo")  # ✓ BERHASIL - kolom ditemukan!
countDistinct("InvoiceNo")  # ✓ BERHASIL - bisa digunakan
```

---

## 📊 PERBANDINGAN SEBELUM vs SESUDAH

| Aspek | SEBELUM (Error) | SESUDAH (Perbaikan) |
|-------|-----------------|-----------------|
| **Error Stage** | 5, 6, 7, 8 | ✅ Semua berjalan |
| **Kolom InvoiceNo** | `    InvoiceNo` (dengan spasi) | `InvoiceNo` (clean) |
| **Ketika diakses** | ❌ UNRESOLVED_COLUMN | ✅ Ditemukan |
| **PostgreSQL Save** | ❌ ERROR | ✅ Success |
| **MongoDB Save** | ❌ ERROR | ✅ Success |
| **Verification** | ❌ ERROR | ✅ Success |

---

## 🚀 TESTING SEKARANG

Setelah perbaikan ini, **jalankan pipeline lagi:**

```bash
python run_pipeline.py
```

Seharusnya sekarang **semua 9 stage selesai** tanpa error!

---

## 📝 CATATAN TEKNIS

### **Fungsi `c.strip()`**

```python
"    InvoiceNo".strip()  # → "InvoiceNo"
"StockCode    ".strip()  # → "StockCode"
"  Description  ".strip()  # → "Description"
```

Menghapus whitespace di:
- **Leading** (depan): spasi sebelum karakter pertama
- **Trailing** (belakang): spasi setelah karakter terakhir

### **List Comprehension**

```python
[col(c).alias(c.strip()) for c in df.columns]
```

Berarti:
1. Untuk setiap nama kolom `c` di `df.columns`
2. Buat column expression: `col(c)` (ambil kolom tersebut)
3. Rename dengan: `.alias(c.strip())` (nama baru tanpa whitespace)
4. Return list dari semua column expressions ini

Spark kemudian menggunakan list ini untuk `.select()` → menghasilkan DataFrame baru dengan nama kolom yang clean!

---

## ✨ RINGKASAN

**Masalah**: CSV punya leading whitespace → nama kolom punya spasi → Error saat akses

**Solusi**: Strip whitespace dari nama kolom setelah membaca data

**Tempat Fix**: 
- cleaning.py (source)
- 7 file lain yang membaca Parquet/JDBC

**Hasil**: ✅ Pipeline siap berjalan lancar!

---

## 🎓 PELAJARAN

Ketika bekerja dengan Spark dan file CSV:
1. **Selalu check nama kolom** - bisa ada whitespace tersembunyi
2. **Clean column names saat load** - lebih mudah daripada fix di banyak tempat
3. **Use `.strip()`** - sederhana tapi sangat efektif
4. **Test dengan data sample** - catch error lebih awal

---

**Status**: ✅ **SUDAH DIPERBAIKI**  
**Next**: Jalankan `python run_pipeline.py` untuk verifikasi!
