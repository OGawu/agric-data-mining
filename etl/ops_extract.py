import pandas as pd

DATA_DIR = "data"

def fetch_ops_data():
    """Load farmers.csv and yields.csv from the data directory."""
    try:
        farmers = pd.read_csv(f"{DATA_DIR}/farmers.csv")
        yields = pd.read_csv(f"{DATA_DIR}/yields.csv")

        print("[SUCCESS] Extracted farmers.csv and yields.csv")
        return farmers, yields
    
    except Exception as e:
        print(f"[ERROR] Failed to load CSV files: {e}")
        raise
