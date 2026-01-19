import pandas as pd
import csv

_NULLS = {"", "null", "none", "nan", "na", "n/a"}

def read_csv_safe(path):
    df = pd.read_csv(
        path,
        sep=";",
        encoding="utf-8",
        dtype=str,
        on_bad_lines="skip",
        engine="python",             # más tolerante
        quoting=csv.QUOTE_MINIMAL,
        escapechar="\\",
        keep_default_na=False,       # evita que pandas invente los NaN “solo”
    )

    # Normaliza nulos: "" / "NULL" / "None" / "nan" -> None
    df = df.applymap(lambda x: None if x is None else (None if str(x).strip().lower() in _NULLS else str(x)))

    return df
