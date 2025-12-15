from etl.satellite_extract import fetch_satellite_data
from etl.satellite_transform import clean_satellite
from etl.load_to_mongo import load_dataframe_to_mongo
from etl.metadata_writer import update_metadata

print("\n SATELLITE ETL STARTED ")

# Extract
df_raw = fetch_satellite_data()

# Load RAW
load_dataframe_to_mongo(df_raw, "raw_satellite")
update_metadata("raw_satellite", df_raw, layer="RAW", description="Raw NDVI, rainfall and temperature metrics")

# Transform
df_clean = clean_satellite(df_raw)

# Load STAGING
load_dataframe_to_mongo(df_clean, "stg_satellite")
update_metadata("stg_satellite", df_clean, layer="STG", description="Cleaned satellite metrics with drought flag")

print("\n SATELLITE ETL COMPLETED SUCCESSFULLY \n")
