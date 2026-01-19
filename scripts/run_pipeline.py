import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from etl.file_discovery import discover_files
from etl.reader import read_csv_safe
from etl.clean_clientes import clean_dataframe
from etl.validate_clientes import validate_clientes
from etl.clean_tarjetas import clean_tarjetas
from etl.validate_tarjetas import validate_tarjetas
from etl.errors import write_errors_by_source
from etl.logger import setup_logger

INPUT_PATH = PROJECT_ROOT / "data" / "raw"
OUTPUT_PATH = PROJECT_ROOT / "data" / "output"
ERRORS_PATH = PROJECT_ROOT / "errors"
LOG_FILE = PROJECT_ROOT / "logs" / "etl.log"


def _count_errors(err_list) -> int:
    return sum(len(df) for df in err_list) if err_list else 0


def _log_error_details(logger, err_list, title: str):
    if not err_list:
        return
    try:
        import pandas as pd
        merged = pd.concat(err_list, ignore_index=True)
        if "error_detalle" in merged.columns:
            top = merged["error_detalle"].value_counts().head(5)
            for motivo, cnt in top.items():
                logger.warning(f"{title} motivo='{motivo}' -> {cnt}")
        else:
            logger.warning(f"{title} (sin columna error_detalle)")
    except Exception:
        logger.exception("No se pudo calcular el resumen de motivos de error")


def main():
    logger = setup_logger(log_file=str(LOG_FILE))
    logger.info("Inicio del pipeline ETL")

    INPUT_PATH.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    ERRORS_PATH.mkdir(parents=True, exist_ok=True)

    clientes, tarjetas, ignored = discover_files(str(INPUT_PATH))
    logger.info(f"Clientes encontrados: {len(clientes)}")
    logger.info(f"Tarjetas encontradas: {len(tarjetas)}")
    logger.info(f"Ficheros ignorados: {len(ignored)}")

    all_errors = []

    #  CLIENTES
    for file in clientes:
        try:
            logger.info(f"Procesando CLIENTES: {file.name}")
            df = read_csv_safe(file)
            logger.info(f"Filas leídas CLIENTES: {len(df)}")

            df = clean_dataframe(df)
            df, errs = validate_clientes(df)

            rejected = _count_errors(errs)
            if rejected > 0:
                logger.warning(f"Filas rechazadas CLIENTES: {rejected}")
                _log_error_details(logger, errs, title="CLIENTES")
            else:
                logger.info("Filas rechazadas CLIENTES: 0")

            all_errors.extend(errs)

            out = OUTPUT_PATH / f"{file.stem}.cleaned.csv"
            df.to_csv(out, index=False)
            logger.info(f"Archivo generado: {out}")

        except Exception:
            logger.exception(f"Error procesando CLIENTES: {file.name}")

    #Clean de tarjetas por separado
    for file in tarjetas:
        try:
            logger.info(f"Procesando TARJETAS: {file.name}")
            df = read_csv_safe(file)
            logger.info(f"Filas leídas TARJETAS: {len(df)}")

            df = clean_tarjetas(df)          # SOLO transforma (mask/hash + borra cvv)
            df, errs = validate_tarjetas(df) # SOLO valida (devuelve df_valid + errs)

            rejected = _count_errors(errs)
            if rejected > 0:
                logger.warning(f"Filas rechazadas TARJETAS: {rejected}")
                _log_error_details(logger, errs, title="TARJETAS")
            else:
                logger.info("Filas rechazadas TARJETAS: 0")

            all_errors.extend(errs)

            # NO guarda los flags OK/KO en el output final de tarjetas
            df.drop(
                columns=[
                    "card_clean",
                    "CodCliente_OK", "CodCliente_KO",
                    "FechaExp_OK", "FechaExp_KO",
                    "Tarjeta_OK", "Tarjeta_KO",
                ],
                inplace=True,
                errors="ignore"
            )

            out = OUTPUT_PATH / f"{file.stem}.cleaned.csv"
            df.to_csv(out, index=False)
            logger.info(f"Archivo generado: {out}")

        except Exception:
            logger.exception(f"Error procesando TARJETAS: {file.name}")

    #  ERRORES (separados por origen)
    total_rejected = _count_errors(all_errors)
    if total_rejected > 0:
        write_errors_by_source(all_errors, output_dir=str(ERRORS_PATH))
        logger.warning(f"Total filas erróneas registradas: {total_rejected}")
    else:
        logger.info("No hay filas erróneas. No se generan CSV de rechazadas")

    logger.info("Fin del pipeline ETL")


if __name__ == "__main__":
    main()
