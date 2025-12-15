import pandas as pd

DATA_DIR = "data"

def fetch_social_data():
    """Load social_posts.csv into a DataFrame."""
    try:
        df = pd.read_csv(f"{DATA_DIR}/social_posts.csv")
        print("[SUCCESS] Extracted social_posts.csv")
        return df
    except Exception as e:
        print(f"[ERROR] Failed to load social_posts.csv: {e}")
        raise
