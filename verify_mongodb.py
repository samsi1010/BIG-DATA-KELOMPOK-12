from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['retail_db']
coll = db['retail_anomalies']

total = coll.count_documents({})
anom = coll.count_documents({'anomaly': 1})
sample = coll.find_one()

print('✅ MONGODB VERIFICATION')
print(f'📊 Total Documents: {total}')
print(f'🔴 Anomalies: {anom} ({round(100*anom/total,2)}%)')
print(f'📋 Sample Record:')
print(f'  - InvoiceDate: {sample.get("InvoiceDate")}')
print(f'  - Hour: {sample.get("Hour")}')
print(f'  - Day: {sample.get("Day")}')
print(f'  - MonthNum: {sample.get("MonthNum")}')
print(f'  - anomaly_score: {sample.get("anomaly_score")}')

# Check for records with valid hour values (not None)
valid_hour = coll.count_documents({'Hour': {'$ne': None}})
print(f'\n✅ Records with valid Hour field: {valid_hour}')

client.close()
