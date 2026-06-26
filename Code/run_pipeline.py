import os

print("=" * 80)
print("MENJALANKAN BIG DATA RETAIL PIPELINE - COMPLETE WORKFLOW")
print("=" * 80)

# =====================================================
# 1. DATA CLEANING
# =====================================================

print("\n[1/10] Data Cleaning...")
os.system("python spark/cleaning.py")

# =====================================================
# 2. EXPORT CLEANED DATASET KE CSV
# =====================================================

print("\n[2/10] Export Cleaned Dataset CSV...")
os.system("python spark/export_cleaned_csv.py")

# =====================================================
# 3. FEATURE ENGINEERING
# =====================================================

print("\n[3/10] Feature Engineering...")
os.system("python spark/feature_engineering.py")

# =====================================================
# 4. FEATURE ENGINEERING REPORT
# =====================================================

print("\n[4/10] Generating Feature Engineering Report...")
os.system("python spark/output_report.py")

# =====================================================
# 5. ANOMALY DETECTION
# =====================================================

print("\n[5/10] Anomaly Detection...")
os.system("python spark/advanced_anomaly.py")

# =====================================================
# 6. CUSTOMER SEGMENTATION
# =====================================================

print("\n[6/10] Customer Segmentation...")
os.system("python spark/customer_segmentation.py")

# =====================================================
# 7. SAVE TO POSTGRESQL
# =====================================================

print("\n[7/10] Saving to PostgreSQL...")
os.system("python spark/save_postgres.py")

# =====================================================
# 8. SAVE TO MONGODB
# =====================================================

print("\n[8/10] Saving to MongoDB...")
os.system("python spark/save_mongodb.py")

# =====================================================
# 9. VERIFY DATABASE INTEGRATION
# =====================================================

print("\n[9/10] Verifying Database Integration...")
os.system("python spark/verify_integration.py")

# =====================================================
# 10. DATABASE INTEGRATION REPORT
# =====================================================

print("\n[10/10] Generating Integration Report...")
os.system("python spark/database_integration_report.py")

# =====================================================
# FINISH
# =====================================================

print("\n" + "=" * 80)
print("✓ PIPELINE SELESAI")
print("=" * 80)

print("\nOUTPUT DATASET")
print("   • data/cleaned_data/")
print("   • data/cleaned_retail.csv")
print("   • data/featured_data/")
print("   • data/advanced_anomaly/")

print("\nOUTPUT REPORTS")
print("   • reports/data_cleaning_report.csv")
print("   • reports/01-05_*.csv")
print("   • reports/06-12_*.csv")
print("   • reports/*.json")

print("\nDATABASE")
print("   • PostgreSQL : retail_transactions")
print("   • MongoDB    : retail_anomalies")

print("\nDASHBOARD")
print("   • Streamlit Dashboard siap dijalankan")