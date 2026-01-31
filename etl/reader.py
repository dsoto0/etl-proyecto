import pandas as pd
import csv
from pathlib import Path


_NULLS = {"", "null", "none", "nan", "na", "n/a"}
# Leer CSV con formato especial (líneas entrecomilladas y separadas por ;)

def _read_quoted_semicolon_lines(path: Path, encoding: str) -> pd.DataFrame:
    rows = []
    with open(path, "r", encoding=encoding, errors="strict") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # quitar comillas exteriores si existen
            if len(line) >= 2 and line[0] == '"' and line[-1] == '"':
                line = line[1:-1]
            rows.append(line.split(";"))

    if not rows:
        return pd.DataFrame()

    header = [h.strip() for h in rows[0]]
    data = rows[1:]
    return pd.DataFrame(data, columns=header)


def read_csv_safe(path):
    path = Path(path)

    encodings = ["utf-8-sig", "utf-8", "cp1252", "latin1"]
    last_err = None

    for enc in encodings:
        try:
            df = pd.read_csv(
                path,
                sep=";",
                encoding=enc,
                dtype=str,
                on_bad_lines="skip",
                engine="python",
                quoting=csv.QUOTE_MINIMAL,
                escapechar="\\",
                keep_default_na=False,
            )

            # Si pandas lo leyó como 1 sola columna → formato entrecomillado raro
            if df.shape[1] == 1:
                col = df.columns[0]
                if ";" in col:
                    df = _read_quoted_semicolon_lines(path, enc)

            # Normalizar columnas
            df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()
            if "cod cliente" in df.columns:
                df.rename(columns={"cod cliente": "cod_cliente"}, inplace=True)

            # Normalizar nulos
            def _norm(x):
                if x is None:
                    return None
                s = str(x).strip()
                return None if s.lower() in _NULLS else str(x)

            if hasattr(df, "map"):
                df = df.map(_norm)
            else:
                df = df.applymap(_norm)

            return df

        except Exception as e:
            last_err = e

    raise last_err
