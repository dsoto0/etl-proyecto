import pandas as pd
import unicodedata


def normalize_text(value):

    if pd.isna(value) or value is None:
        return None

    s = str(value).strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    s = " ".join(s.split())
    return s


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()
    for col in df.columns:
        df[col] = df[col].apply(normalize_text)
    return df
