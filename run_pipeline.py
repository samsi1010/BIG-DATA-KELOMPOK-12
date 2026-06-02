import os

print("=" * 80)
print("MENJALANKAN BIG DATA RETAIL PIPELINE - COMPLETE WORKFLOW")
print("=" * 80)

# 1. Cleaning
print("\n[1/9] Data Cleaning...")
os.system("python spark/cleaning.py")

# 2. Feature Engineering
print("\n[2/9] Feature Engineering...")
os.system("python spark/feature_engineering.py")

# 3. Generate Output Report (Feature Engineering)
print("\n[3/9] Generating Feature Engineering Report...")
os.system("python spark/output_report.py")

# 4. Anomaly Detection
print("\n[4/9] Anomaly Detection...")
os.system("python spark/advanced_anomaly.py")

# 5. Customer Segmentation
print("\n[5/9] Customer Segmentation...")
os.system("python spark/customer_segmentation.py")

# 6. Save to PostgreSQL
print("\n[6/9] Saving to PostgreSQL (Database Relasional)...")
os.system("python spark/save_postgres.py")

# 7. Save to MongoDB
print("\n[7/9] Saving to MongoDB (Database Non-Relasional)...")
os.system("python spark/save_mongodb.py")

# 8. Verify Integration
print("\n[8/9] Verifying Database Integration (Cross-Check)...")
os.system("python spark/verify_integration.py")

# 9. Generate Integration Report
print("\n[9/9] Generating Integration Report...")
os.system("python spark/database_integration_report.py")

print("\n" + "=" * 80)
print("✓ PIPELINE SELESAI!")
print("=" * 80)
print("\n📁 Output Reports tersedia di folder: reports/")
print("\nDokumentasi:")
print("   • Feature Engineering: 01-05_*.csv")
print("   • Database Integration: 06-12_*.csv & .json")

