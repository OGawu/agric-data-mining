import os
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

# ---------------------------------------------
# LOAD ENVIRONMENT VARIABLES
# ---------------------------------------------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB  = os.getenv("MONGO_DB")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

print("\n BUILDING DATA WAREHOUSE (DW) ")

# 1. DW YIELDS (from stg_yields)
print("[1/3] Building DW_YIELDS ...")

pipeline_yields = [
    {
        "$group": {
            "_id": {
                "farmer_id": "$farmer_id",
                "district": "$district",
                "crop": "$crop",
                "season": "$season"
            },
            "total_yield_tons": { "$sum": "$yield_tons" },
            "avg_yield_tpha": { "$avg": "$yield_tpha" },
            "harvest_count": { "$sum": 1 }
        }
    },
    {
        "$project": {
            "_id": 0,
            "farmer_id": "$_id.farmer_id",
            "district": "$_id.district",
            "crop": "$_id.crop",
            "season": "$_id.season",
            "total_yield_tons": 1,
            "avg_yield_tpha": 1,
            "harvest_count": 1
        }
    }
]

dw_yields = list(db["stg_yields"].aggregate(pipeline_yields))
db["dw_yields"].delete_many({})
if dw_yields:
    db["dw_yields"].insert_many(dw_yields)

print(f"  DW_YIELDS rows: {len(dw_yields)}")

# 2. DW SATELLITE (from stg_satellite)
print("[2/3] Building DW_SATELLITE ...")

pipeline_sat = [
    {
        "$group": {
            "_id": {
                "farmer_id": "$farmer_id",
                "district": "$district"
            },
            "avg_ndvi": { "$avg": "$ndvi" },
            "max_ndvi": { "$max": "$ndvi" },
            "min_ndvi": { "$min": "$ndvi" },
            "records": { "$sum": 1 }
        }
    },
    {
        "$project": {
            "_id": 0,
            "farmer_id": "$_id.farmer_id",
            "district": "$_id.district",
            "avg_ndvi": 1,
            "max_ndvi": 1,
            "min_ndvi": 1,
            "records": 1
        }
    }
]

dw_sat = list(db["stg_satellite"].aggregate(pipeline_sat))
db["dw_satellite"].delete_many({})
if dw_sat:
    db["dw_satellite"].insert_many(dw_sat)

print(f"   DW_SATELLITE rows: {len(dw_sat)}")



# 3. DW SOCIAL (from stg_social)
print("[3/3] Building DW_SOCIAL ...")

pipeline_social = [
    {
        "$group": {
            "_id": {
                "farmer_id": "$farmer_id",
                "district": "$district"
            },
            "avg_sentiment": { "$avg": "$sentiment" },
            "positive_count": {
                "$sum": {
                    "$cond": [{ "$gt": ["$sentiment", 0] }, 1, 0]
                }
            },
            "negative_count": {
                "$sum": {
                    "$cond": [{ "$lt": ["$sentiment", 0] }, 1, 0]
                }
            },
            "total_posts": { "$sum": 1 }
        }
    },
    {
        "$project": {
            "_id": 0,
            "farmer_id": "$_id.farmer_id",
            "district": "$_id.district",
            "avg_sentiment": 1,
            "positive_count": 1,
            "negative_count": 1,
            "total_posts": 1
        }
    }
]

dw_social = list(db["stg_social"].aggregate(pipeline_social))
db["dw_social"].delete_many({})
if dw_social:
    db["dw_social"].insert_many(dw_social)

print(f"  DW_SOCIAL rows: {len(dw_social)}")


print("\n DW BUILD COMPLETE ")
