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

        origen_up = str(origen).upper()

        if origen_up == "TARJETAS":

            desired = [
                "origen", "error", "error_detalle",
                "cod_cliente", "fecha_exp",
                "numero_tarjeta_masked", "numero_tarjeta_hash",
            ]
            cols = [c for c in desired if c in group.columns] + [c for c in group.columns if c not in desired]
            group = group[cols]

        elif origen_up == "CLIENTES":
            desired = [
                "origen", "error", "error_detalle",
                "cod_cliente", "nombre", "apellido1", "apellido2",
                "dni", "correo", "telefono",
                "DNI_OK", "DNI_KO",
                "Telefono_OK", "Telefono_KO",
                "Correo_OK", "Correo_KO",
            ]
            cols = [c for c in desired if c in group.columns] + [c for c in group.columns if c not in desired]
            group = group[cols]

        fname = f"rows_rejected_{str(origen).lower()}.csv"
        group.to_csv(out_dir / fname, index=False)
        print(f"Filas erróneas ({origen}) registradas en: {out_dir / fname}")
