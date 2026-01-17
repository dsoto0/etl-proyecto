import pandas as pd
from pathlib import Path


def write_errors_by_source(error_dfs: list[pd.DataFrame], output_dir: str = "errors"):
    """
    Genera 2 CSV separados por origen:
      - rows_rejected_clientes.csv
      - rows_rejected_tarjetas.csv
    Evita el "autorrellenado" de columnas al mezclar.
    """
    if not error_dfs:
        return

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    errors_df = pd.concat(error_dfs, ignore_index=True)

    if "origen" not in errors_df.columns:
        # fallback: si no existe, lo deja en uno único
        errors_df.to_csv(out_dir / "rows_rejected.csv", index=False)
        return

    for origen, group in errors_df.groupby("origen"):
        fname = f"rows_rejected_{origen.lower()}.csv"
        group.to_csv(out_dir / fname, index=False)
        print(f"Filas erróneas ({origen}) registradas en: {out_dir / fname}")

