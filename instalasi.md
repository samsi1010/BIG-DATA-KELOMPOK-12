BIG DATA RETAIL ANOMALY PROJECT

Menjalankan Project di Local
Dokumen ini menjelaskan seluruh tahapan untuk menjalankan proyek Big Data Retail Anomaly Detection pada komputer lokal.

1. Clone Repository
Clone repository dari GitHub.
git clone https://github.com/USERNAME/bigdata-retail-anomaly.git
cd bigdata-retail-anomaly

2. Persyaratan Software
Pastikan software berikut telah terinstal.
Software	Versi yang Disarankan
Python	3.11 atau 3.12
Java JDK	17
PostgreSQL	16 atau terbaru
MongoDB Community	8 atau terbaru
Git	Terbaru
Visual Studio Code	Opsional

3. Membuat Virtual Environment
Windows PowerShell
python -m venv .venv
Aktifkan Virtual Environment
PowerShell
.\.venv\Scripts\Activate.ps1
Command Prompt
.venv\Scripts\activate.bat
Linux / macOS
python3 -m venv .venv
source .venv/bin/activate
Apabila berhasil maka terminal akan menampilkan
(.venv)

4. Install Dependency Python
Upgrade pip
python -m pip install --upgrade pip
Install seluruh dependency
pip install -r requirements.txt
Apabila terdapat package yang belum tersedia, jalankan
pip install pyspark
pip install pandas
pip install numpy
pip install scikit-learn
pip install matplotlib
pip install streamlit
pip install sqlalchemy
pip install psycopg2-binary
pip install pymongo
pip install pyarrow
Pastikan PySpark berhasil terinstall
python -c "import pyspark; print(pyspark.__version__)"

5. Install Java
Apache Spark memerlukan Java.
Pastikan Java telah terinstall.
Cek versi Java
java -version
Apabila belum tersedia, install Java JDK 17.
Setelah itu tambahkan Environment Variable
JAVA_HOME
Contoh
C:\Program Files\Java\jdk-17
Tambahkan juga
%JAVA_HOME%\bin
ke PATH.

6. Install PostgreSQL
Install PostgreSQL.
Buat database baru
CREATE DATABASE retail_bigdata;
Sesuaikan konfigurasi database pada file
spark/save_postgres.py
Contoh
url = "jdbc:postgresql://localhost:5432/retail_bigdata"

properties = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

7. Install MongoDB
Install MongoDB Community Edition.
Pastikan service MongoDB aktif.
Default connection
mongodb://localhost:27017
Sesuaikan apabila menggunakan konfigurasi lain.

8. JDBC PostgreSQL
Download JDBC Driver PostgreSQL.
Simpan file
postgresql-42.x.x.jar
ke folder
jars/
Contoh
jars/
    postgresql-42.7.3.jar

9. Dataset
Karena ukuran dataset melebihi batas GitHub, dataset tidak disimpan langsung pada repository.
Masukkan dataset ke folder
data/
Contoh
data/
    online_retail.csv.gz
atau
data/
    online_retail.csv
Pastikan nama file sesuai dengan yang digunakan pada file
spark/cleaning.py

10. Struktur Folder
Sebelum menjalankan project, struktur folder minimal adalah
bigdata-retail-anomaly/

dashboard/
data/
jars/
reports/
spark/
sql/

requirements.txt
run_pipeline.py
Folder data setelah pipeline selesai
data/

online_retail.csv.gz

cleaned_data/

featured_data/

featured_data_sample/

advanced_anomaly/

customer_segments/

cleaned_retail.csv

11. Menjalankan Pipeline
Aktifkan Virtual Environment
Windows
.\.venv\Scripts\Activate.ps1
Kemudian jalankan pipeline
python run_pipeline.py
Pipeline akan menjalankan proses berikut
1.	Data Cleaning
2.	Export Cleaned Dataset CSV
3.	Feature Engineering
4.	Feature Engineering Report
5.	Advanced Anomaly Detection
6.	Customer Segmentation
7.	Save Data ke PostgreSQL
8.	Save Data ke MongoDB
9.	Verifikasi Integrasi Database
10.	Generate Database Integration Report

12. Output Pipeline
Setelah pipeline selesai akan dihasilkan
data/

cleaned_data/

cleaned_retail.csv

featured_data/

featured_data_sample/

advanced_anomaly/

customer_segments/
Report
reports/

data_cleaning_report.csv

01-05_*.csv

06-12_*.csv

*.json

13. Database
Data transaksi akan disimpan pada PostgreSQL.
Data anomaly akan disimpan pada MongoDB.
Pastikan kedua database aktif sebelum menjalankan pipeline.

14. Menjalankan Dashboard
Masuk ke folder dashboard
cd dashboard
Jalankan Streamlit
streamlit run app.py
Dashboard akan tersedia pada
http://localhost:8501

15. Troubleshooting
ModuleNotFoundError
Install kembali dependency
pip install -r requirements.txt
atau
pip install pyspark pandas numpy scikit-learn matplotlib streamlit psycopg2-binary pymongo sqlalchemy pyarrow

Java Gateway Process Exited
Pastikan
java -version
berjalan dengan benar.
Pastikan JAVA_HOME telah dikonfigurasi.

PostgreSQL Connection Error
Periksa
•	PostgreSQL aktif
•	Database retail_bigdata sudah dibuat
•	Username dan password sesuai

MongoDB Connection Error
Pastikan MongoDB Service berjalan.

Dataset Tidak Ditemukan
Pastikan dataset berada pada
data/online_retail.csv.gz
atau
data/online_retail.csv
sesuai dengan path pada file spark/cleaning.py.

Dashboard Tidak Berjalan
Install Streamlit
pip install streamlit
Kemudian jalankan kembali
streamlit run dashboard/app.py

