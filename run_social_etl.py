from etl.social_extract import fetch_social_data
from etl.social_transform import clean_social
from etl.load_to_mongo import load_dataframe_to_mongo
from etl.metadata_writer import update_metadata

print("\n SOCIAL MEDIA ETL STARTED ")

# Extract
df_raw = fetch_social_data()

# Load RAW
load_dataframe_to_mongo(df_raw, "raw_social")
update_metadata("raw_social", df_raw, layer="RAW", description="Raw social media farmer posts")

# Transform
df_clean = clean_social(df_raw)

# Load STAGING
load_dataframe_to_mongo(df_clean, "stg_social")
update_metadata("stg_social", df_clean, layer="STG", description="Cleaned posts with sentiment score")

print("\n SOCIAL MEDIA ETL COMPLETED SUCCESSFULLY \n")
