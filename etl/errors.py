import pandas as pd
from pathlib import Path

# Evita FutureWarning de downcasting en replace (pandas recientes)
try:
    pd.set_option("future.no_silent_downcasting", True)
except Exception:
    pass


def write_errors_by_source(error_dfs: list[pd.DataFrame], output_dir: str = "errors"):

    if not error_dfs:
        return

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    errors_df = pd.concat(error_dfs, ignore_index=True)

    # Si no hay 'origen', no escribimos nada
    if "origen" not in errors_df.columns:
        return

    for origen, group in errors_df.groupby("origen"):
        group = group.copy()

        group = group.replace(r"^\s*$", pd.NA, regex=True)
        group.dropna(axis=1, how="all", inplace=True)