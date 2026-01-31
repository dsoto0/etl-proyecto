import pandas as pd
from pathlib import Path

try:
    pd.set_option("future.no_silent_downcasting", True)
except Exception:
    pass


def write_rows_rejected_clientes_tarjetas(
        error_dfs: list[pd.DataFrame],
        output_dir: str = "errors",
        include_motivo: bool = True,
        logger=None
):
    # Combina dataframes de errores y genera CSV separados para CLIENTES y TARJETAS
    if not error_dfs:
        if logger:
            logger.info("No hay filas erróneas. No se generan CSV de rechazadas.")
        return

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    errors_df = pd.concat(error_dfs, ignore_index=True)

    # Limpieza suave
    errors_df = errors_df.replace(r"^\s*$", pd.NA, regex=True)

    # Normalizar nombres de columnas (por si vienen con espacios, BOM, etc.)
    def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
        df.columns = (
            df.columns.astype(str)
            .str.replace("\ufeff", "", regex=False)
            .str.strip()
        )
        rename = {
            "Cod cliente": "cod_cliente",
            "COD_CLIENTE": "cod_cliente",
            "cod cliente": "cod_cliente",

            "numero tarjeta": "numero_tarjeta",
            "Número tarjeta": "numero_tarjeta",
            "Numero_tarjeta": "numero_tarjeta",

            # por si tu validador usa estos nombres
            "numero_tarjeta_mask": "numero_tarjeta_masked",
            "tarjeta_masked": "numero_tarjeta_masked",
            "tarjeta_hash": "numero_tarjeta_hash",
        }
        df.rename(columns=rename, inplace=True)
        return df

    errors_df = _normalize_cols(errors_df)

    # Detectar columna “origen”
    origin_col = None
    for c in ["origen", "source_file", "file", "archivo"]:
        if c in errors_df.columns:
            origin_col = c
            break

    # Separar por origen (preferente). Si falla, por columnas.
    df_c = pd.DataFrame()
    df_t = pd.DataFrame()

    if origin_col is not None:
        origin_series = errors_df[origin_col].astype(str)
        is_clientes = origin_series.str.contains(r"clientes", case=False, na=False)
        is_tarjetas = origin_series.str.contains(r"tarjetas", case=False, na=False)
        df_c = errors_df[is_clientes].copy()
        df_t = errors_df[is_tarjetas].copy()

    if df_c.empty and df_t.empty:
        cols = set(errors_df.columns.str.lower())
        tiene_tarjetas = any(c in cols for c in ["numero_tarjeta", "numero_tarjeta_masked", "numero_tarjeta_hash", "cvv", "fecha_exp"])
        tiene_clientes = any(c in cols for c in ["dni", "correo", "telefono", "apellido1", "apellido2", "nombre"])

        if tiene_tarjetas and not tiene_clientes:
            df_t = errors_df.copy()
        elif tiene_clientes and not tiene_tarjetas:
            df_c = errors_df.copy()
        else:
            # Intento por fila si hay campos de tarjeta
            candidates = [c for c in ["numero_tarjeta", "numero_tarjeta_masked", "numero_tarjeta_hash", "cvv", "fecha_exp"] if c in errors_df.columns]
            if candidates:
                mask_t = errors_df[candidates].notna().any(axis=1)
                df_t = errors_df[mask_t].copy()
                df_c = errors_df[~mask_t].copy()
            else:
                df_c = errors_df.copy()

    #  Columnas deseadas EXACTAS
    cols_clientes = [
        "cod_cliente", "nombre", "apellido1", "apellido2", "dni", "correo", "telefono",
        "DNI_OK", "DNI_KO", "Telefono_OK", "Telefono_KO", "Correo_OK", "Correo_KO"
    ]
    cols_tarjetas = ["cod_cliente", "fecha_exp", "numero_tarjeta_masked", "numero_tarjeta_hash"]

    # Obtener texto de error (error_detalle)
    def _get_error_series(df: pd.DataFrame):
        if "motivo" in df.columns:
            return df["motivo"].astype(str)
        if "error_detalle" in df.columns:
            return df["error_detalle"].astype(str)
        if "error" in df.columns:
            return df["error"].astype(str)
        return pd.Series([""] * len(df), index=df.index)

    def _select_exact(df: pd.DataFrame, cols: list[str], add_error: bool = True) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=cols + (["error"] if add_error else []))

        # Asegurar columnas
        for c in cols:
            if c not in df.columns:
                df[c] = pd.NA

        out = df[cols].copy()

        # Añadir error al final
        if add_error and include_motivo:
            out["error"] = _get_error_series(df)

        return out

    out_clientes = _select_exact(df_c, cols_clientes, add_error=True)
    out_tarjetas = _select_exact(df_t, cols_tarjetas, add_error=True)

    # Guardar (sobrescribe cada run)
    path_c = out_dir / "Clientes.rows_rejected.csv"
    path_t = out_dir / "Tarjetas.rows_rejected.csv"

    if len(out_clientes) > 0:
        out_clientes.to_csv(path_c, index=False, encoding="utf-8", sep=";")
        if logger:
            logger.warning(f"Generado {path_c} -> {len(out_clientes)} filas rechazadas (CLIENTES)")
    else:
        if logger:
            logger.info("CLIENTES: no hay filas rechazadas. No se genera CSV.")

    if len(out_tarjetas) > 0:
        out_tarjetas.to_csv(path_t, index=False, encoding="utf-8", sep=";")
        if logger:
            logger.warning(f"Generado {path_t} -> {len(out_tarjetas)} filas rechazadas (TARJETAS)")
    else:
        if logger:
            logger.info("TARJETAS: no hay filas rechazadas. No se genera CSV.")
