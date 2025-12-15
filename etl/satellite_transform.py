import pandas as pd

def clean_satellite(df: pd.DataFrame) -> pd.DataFrame:
    """Clean satellite metrics for staging use."""
    
    # Parse date
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    
    # Standardize district names
    df["district"] = df["district"].str.title()
    
    # Ensure numeric types
    df["avg_ndvi"] = pd.to_numeric(df["avg_ndvi"], errors="coerce")
    df["rainfall_mm"] = pd.to_numeric(df["rainfall_mm"], errors="coerce")
    df["temp_c"] = pd.to_numeric(df["temp_c"], errors="coerce")

    # Optional simple drought alert
    df["drought_flag"] = df["avg_ndvi"].apply(lambda x: 1 if x < 0.45 else 0)

    print("[SUCCESS] Transformed satellite metrics (cleaned)")
    return df
