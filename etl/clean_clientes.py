import pandas as pd
import unicodedata

"""Limpieza básica de datos en un DataFrame de pandas. Normaliza texto eliminando espacios extra y caracteres especiales."""
def normalize_text(value):

    if pd.isna(value) or value is None:
        return None

    s = str(value).strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    s = " ".join(s.split())
    return s

"""Aplica la normalización de texto a todas las columnas del DataFrame."""
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()
    for col in df.columns:
        df[col] = df[col].apply(normalize_text)
    return df
