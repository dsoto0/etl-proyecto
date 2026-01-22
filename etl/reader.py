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
        engine="python",
        quoting=csv.QUOTE_MINIMAL,
        escapechar="\\",
        keep_default_na=False,       # evita que pandas invente los NaN “solo”
    )

    #  normalizacion  del nombre de la columna de clientes
    df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()
    if "cod cliente" in df.columns:
        df.rename(columns={"cod cliente": "cod_cliente"}, inplace=True)


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