import pandas as pd

def read_csv_safe(path):
    return pd.read_csv(
        path,
        sep=';',
        encoding='utf-8',
        dtype=str,
        on_bad_lines='skip'
    )
