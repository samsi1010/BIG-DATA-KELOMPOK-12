"""
Quick Test Script untuk Feature Engineering + Output Report
Jalankan hanya tahap 1-3 untuk testing tanpa dependencies lainnya

Usage: python test_feature_engineering.py
"""

import os

print("\n" + "=" * 80)
print("QUICK TEST: FEATURE ENGINEERING + OUTPUT REPORT")
print("=" * 80)

# 1. Cleaning
print("\n[1/3] Data Cleaning...")
print("-" * 80)
result = os.system("python spark/cleaning.py")
if result != 0:
    print("❌ Data Cleaning GAGAL")
    exit(1)

# 2. Feature Engineering
print("\n[2/3] Feature Engineering...")
print("-" * 80)
result = os.system("python spark/feature_engineering.py")
if result != 0:
    print("❌ Feature Engineering GAGAL")
    exit(1)

# 3. Output Report (untuk dokumentasi)
print("\n[3/3] Generating Output Report...")
print("-" * 80)
result = os.system("python spark/output_report.py")
if result != 0:
    print("❌ Output Report GAGAL")
    exit(1)

print("\n" + "=" * 80)
print("✓ TEST SELESAI")
print("=" * 80)
print("\n📊 Output Reports dapat dilihat di:")
print("   📁 reports/")
print("      ├─ 01_country_mapping.csv")
print("      ├─ 02_sample_featured_data.csv")
print("      ├─ 03_revenue_category_distribution.csv")
print("      ├─ 04_top_products.csv")
print("      └─ 05_top_customers.csv")
print("\n💾 Data tersimpan di:")
print("   ├─ data/cleaned_data/ (Parquet)")
print("   ├─ data/featured_data/ (Parquet)")
print("   └─ data/featured_data_sample/ (CSV)")
