import pandas as pd

def clean_ops(farmers: pd.DataFrame, yields: pd.DataFrame):
    """Clean and transform operational farmer and yield data."""
    
    # Ensure consistent ID types
    farmers["farmer_id"] = farmers["farmer_id"].astype(str)
    yields["farmer_id"] = yields["farmer_id"].astype(str)

    # Parse harvest date
    yields["harvest_date"] = pd.to_datetime(
        yields["harvest_date"], errors="coerce"
    )

    # Derived metric: Yield per hectare
    yields["yield_tpha"] = yields["yield_tons"] / yields["area_ha"]

    # Clean district names
    farmers["district"] = farmers["district"].str.title()
    yields["district"] = yields["district"].str.title()

    print("[SUCCESS] Transformed farmers and yields data")

    return farmers, yields
