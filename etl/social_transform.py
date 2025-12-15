import pandas as pd

NEGATIVE_WORDS = ["drought", "wilting", "late", "worried", "dry", "fail", "support", "problem"]
POSITIVE_WORDS = ["good", "rain", "healthy", "improving"]

def sentiment_score(text: str) -> float:
    """Very simple rule-based sentiment score."""
    text = text.lower()
    score = 0

    for w in NEGATIVE_WORDS:
        if w in text:
            score -= 1

    for w in POSITIVE_WORDS:
        if w in text:
            score += 1

    return score

def clean_social(df: pd.DataFrame) -> pd.DataFrame:
    """Clean raw social media posts and add sentiment score."""
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["district"] = df["district"].str.title()

    # Apply sentiment scoring
    df["sentiment_score"] = df["text"].apply(sentiment_score)

    print("[SUCCESS] Transformed social posts (cleaned + sentiment)")
    return df
