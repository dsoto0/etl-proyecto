import pandas as pd
import unicodedata

def normalize_text(value):
    if pd.isna(value):
        return None
    value = value.strip()
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('utf-8')
    return value

def clean_dataframe(df):
    for col in df.columns:
        df[col] = df[col].apply(normalize_text)
    return df
