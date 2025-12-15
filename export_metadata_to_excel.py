import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

# Connect to MongoDB
client = MongoClient(
    os.getenv("MONGO_URI"),
    tls=True,
    tlsAllowInvalidCertificates=True
)

db = client[os.getenv("MONGO_DB")]

def export_metadata():
    """Export metadata collections from MongoDB to an Excel file."""
    
    print("\n EXPORTING METADATA TO EXCEL ")

    # Fetch metadata 
    collections_data = list(db["meta_collections"].find({}, {"_id": 0}))
    fields_data = list(db["meta_fields"].find({}, {"_id": 0}))

    # Convert to DataFrames
    df_collections = pd.DataFrame(collections_data)
    df_fields = pd.DataFrame(fields_data)

    # Write to Excel 
    output_file = "agri_metadata_auto.xlsx"

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        df_collections.to_excel(writer, sheet_name="Collections", index=False)
        df_fields.to_excel(writer, sheet_name="Fields", index=False)

    print(f"[SUCCESS] Metadata exported to {output_file}")
    print("Sheets included: Collections, Fields")
    print("=========================================\n")

if __name__ == "__main__":
    export_metadata()
