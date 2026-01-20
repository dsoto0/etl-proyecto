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
    def _norm(x):
        if x is None:
            return None
        s = str(x).strip()
        return None if s.lower() in _NULLS else str(x)

    # pandas nuevos: DataFrame.map (recomendado). pandas antiguos: applymap
    if hasattr(df, "map"):
        df = df.map(_norm)
    else:
        df = df.applymap(_norm)

    return df

