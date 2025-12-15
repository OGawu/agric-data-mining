from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

client = MongoClient(
    os.getenv("MONGO_URI"),
    tls=True,
    tlsAllowInvalidCertificates=True
)

db = client[os.getenv("MONGO_DB")]

def update_metadata(collection_name, df, layer="RAW", description=""):
    """Automatically update metadata for a collection after ETL operations."""

    # Collection-level metadata
    db["meta_collections"].update_one(
        {"collection_name": collection_name},
        {
            "$set": {
                "collection_name": collection_name,
                "layer": layer,
                "description": description,
                "record_count": int(len(df)),
                "fields": df.columns.tolist(),
                "last_updated": datetime.utcnow()
            }
        },
        upsert=True
    )

    # Field-level metadata
    for col in df.columns:
        # Convert numpy types → Python types
        sample_value = df[col].iloc[0] if len(df) > 0 else None
        nullable = bool(df[col].isnull().any())

        # Convert dtype to string
        dtype = str(df[col].dtype)

        # Ensure example_value is serializable
        if hasattr(sample_value, "item"):
            sample_value = sample_value.item()

        db["meta_fields"].update_one(
            {"collection_name": collection_name, "field_name": col},
            {
                "$set": {
                    "collection_name": collection_name,
                    "field_name": col,
                    "data_type": dtype,
                    "nullable": nullable,   # Now real Python bool
                    "example_value": sample_value,
                    "last_updated": datetime.utcnow()
                }
            },
            upsert=True
        )

    print(f"[METADATA] Updated metadata for '{collection_name}'")
