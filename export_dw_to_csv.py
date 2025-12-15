import os
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB  = os.getenv("MONGO_DB")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

tables = {
    "dw_yields.csv": "dw_yields",
    "dw_social.csv": "dw_social",
    "dw_satellite.csv": "dw_satellite"
}

for filename, collection in tables.items():
    print(f"Exporting: {collection} → {filename}")
    data = list(db[collection].find({}, {"_id": 0}))
    if not data:
        print(f" WARNING: {collection} is EMPTY!")
        continue
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)

print("\n EXPORT COMPLETE ")
