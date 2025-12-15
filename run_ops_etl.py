from etl.ops_extract import fetch_ops_data
from etl.ops_transform import clean_ops
from etl.load_to_mongo import load_dataframe_to_mongo
from etl.metadata_writer import update_metadata

print("\n AGRIC OPS ETL STARTED ")

# Extract
farmers_raw, yields_raw = fetch_ops_data()

# Load RAW
load_dataframe_to_mongo(farmers_raw, "raw_farmers")
update_metadata("raw_farmers", farmers_raw, layer="RAW", description="Raw farmer registration data")

load_dataframe_to_mongo(yields_raw, "raw_yields")
update_metadata("raw_yields", yields_raw, layer="RAW", description="Raw farmer yield reports")

# Transform
farmers_clean, yields_clean = clean_ops(farmers_raw, yields_raw)

# Load STAGING
load_dataframe_to_mongo(farmers_clean, "stg_farmers")
update_metadata("stg_farmers", farmers_clean, layer="STG", description="Cleaned and standardized farmer data")

load_dataframe_to_mongo(yields_clean, "stg_yields")
update_metadata("stg_yields", yields_clean, layer="STG", description="Cleaned yield data with yield_tpha metric")

print("\n ETL COMPLETED SUCCESSFULLY \n")
