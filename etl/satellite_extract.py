import pandas as pd

DATA_DIR = "data"

def fetch_satellite_data():
    """Load satellite_metrics.csv into a DataFrame."""
    try:
        df = pd.read_csv(f"{DATA_DIR}/satellite_metrics.csv")
        print("[SUCCESS] Extracted satellite_metrics.csv")
        return df
    except Exception as e:
        print(f"[ERROR] Failed to load satellite_metrics.csv: {e}")
        raise
