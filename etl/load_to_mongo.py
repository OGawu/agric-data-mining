from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

# Create a MongoDB client with TLS settings
client = MongoClient(
    os.getenv("MONGO_URI"),
    tls=True,
    tlsAllowInvalidCertificates=True
)

db = client[os.getenv("MONGO_DB")]

def load_dataframe_to_mongo(df, collection_name: str):
    """Insert pandas DataFrame into a MongoDB collection."""
    records = df.to_dict(orient="records")

    if not records:
        print(f"[INFO] No records to insert into {collection_name}")
        return

    try:
        db[collection_name].insert_many(records)
        print(f"[SUCCESS] Inserted {len(records)} records into '{collection_name}'")
    except Exception as e:
        print(f"[ERROR] Failed to insert into {collection_name}: {e}")
