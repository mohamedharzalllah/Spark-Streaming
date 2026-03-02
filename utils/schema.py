# Write your csv to json transformation code here 
import csv
import json

csv_file = "C:\\Users\\lenovo\\OneDrive\\Documents\\m2 IASD\\flux de donnée\\tp\\Spark-Streaming\\data\\transactions.csv"
json_file = "C:\\Users\\lenovo\\OneDrive\\Documents\\m2 IASD\\flux de donnée\\tp\\Spark-Streaming\\data\\transactions.json"

data = []

with open(csv_file, mode="r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        data.append({
            "transaction_id": row["transaction_id"],
            "user_id": row["user_id"],
            "amount": row["amount"],
            "timestamp": row["timestamp"]
        })

with open(json_file, mode="w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)

print(f"✅ Converted {csv_file} → {json_file}")


