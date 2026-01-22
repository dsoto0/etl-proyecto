import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values


def _yn_to_bool(x):
    if pd.isna(x):
        return None
    s = str(x).strip().upper()
    if s == "Y":
        return True
    if s == "N":
        return False
    if s in ("TRUE", "T", "1"):
        return True
    if s in ("FALSE", "F", "0"):
        return False
    return None


def _clean_env(v: str | None, default: str = "") -> str:
    if v is None:
        v = default
    return v.strip().strip('"').strip("'")


def _get_conn():
    """
    Conexión a PostgreSQL leyendo variables desde .env del proyecto.
    Forzamos client_encoding a UTF8 en libpq para evitar problemas de encoding en Windows.
    """
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env", override=True)

    host = _clean_env(os.getenv("PGHOST", "localhost"))
    port = int(_clean_env(os.getenv("PGPORT", "5432")))
    dbname = _clean_env(os.getenv("PGDATABASE", "etlproyect"))  
    user = _clean_env(os.getenv("PGUSER", "postgres"))
    password = _clean_env(os.getenv("PGPASSWORD", ""))

    
    os.environ["PGCLIENTENCODING"] = "UTF8"
    os.environ["PGOPTIONS"] = "-c client_encoding=UTF8"

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        connect_timeout=5,
    )


def _load_clientes_csv(conn, csv_path: Path, logger=None):
    # Leer como texto para evitar conversiones raras
    df = pd.read_csv(csv_path, dtype=str)

    # Normaliza nombres de columnas
    df.columns = [c.strip() for c in df.columns]
    if "cod cliente" in df.columns:
        df = df.rename(columns={"cod cliente": "cod_cliente"})

    # Renombrar flags a nombres de BD
    rename_flags = {
        "DNI_OK": "dni_ok",
        "DNI_KO": "dni_ko",
        "Telefono_OK": "telefono_ok",
        "Telefono_KO": "telefono_ko",
        "Correo_OK": "correo_ok",
        "Correo_KO": "correo_ko",
    }
    df = df.rename(columns=rename_flags)

    # Si por cualquier motivo quedaran duplicadas, nos quedamos con la primera
    df = df.loc[:, ~df.columns.duplicated()]

    # Convertir flags Y/N a booleanos
    for col in ["dni_ok", "dni_ko", "telefono_ok", "telefono_ko", "correo_ok", "correo_ko"]:
        if col in df.columns:
            df[col] = df[col].map(_yn_to_bool)

    # Columnas esperadas por la tabla
    cols = [
        "cod_cliente", "nombre", "apellido1", "apellido2",
        "dni", "correo", "telefono",
        "dni_ok", "dni_ko", "telefono_ok", "telefono_ko", "correo_ok", "correo_ko",
    ]

    # Asegurar que existen todas las columnas
    for c in cols:
        if c not in df.columns:
            df[c] = None

    df = df[cols]

    rows = [tuple(x) for x in df.to_numpy()]

    sql = """
    INSERT INTO clientes (
      cod_cliente, nombre, apellido1, apellido2, dni, correo, telefono,
      dni_ok, dni_ko, telefono_ok, telefono_ko, correo_ok, correo_ko
    )
    VALUES %s
    ON CONFLICT (cod_cliente) DO UPDATE SET
      nombre=EXCLUDED.nombre,
      apellido1=EXCLUDED.apellido1,
      apellido2=EXCLUDED.apellido2,
      dni=EXCLUDED.dni,
      correo=EXCLUDED.correo,
      telefono=EXCLUDED.telefono,
      dni_ok=EXCLUDED.dni_ok,
      dni_ko=EXCLUDED.dni_ko,
      telefono_ok=EXCLUDED.telefono_ok,
      telefono_ko=EXCLUDED.telefono_ko,
      correo_ok=EXCLUDED.correo_ok,
      correo_ko=EXCLUDED.correo_ko
    ;
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()

    if logger:
        logger.info(f"BD: clientes cargados desde {csv_path.name} -> {len(rows)} filas")



def _load_tarjetas_csv(conn, csv_path: Path, logger=None):
    df = pd.read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]

    cols = ["cod_cliente", "fecha_exp", "numero_tarjeta_masked", "numero_tarjeta_hash"]
    df = df[[c for c in cols if c in df.columns]]

    rows = [tuple(x) for x in df.to_numpy()]

    # IMPORTANTE: requiere UNIQUE(cod_cliente, numero_tarjeta_hash) en la tabla tarjetas
    sql = """
    INSERT INTO tarjetas (cod_cliente, fecha_exp, numero_tarjeta_masked, numero_tarjeta_hash)
    VALUES %s
    ON CONFLICT (cod_cliente, numero_tarjeta_hash) DO UPDATE SET
      fecha_exp=EXCLUDED.fecha_exp,
      numero_tarjeta_masked=EXCLUDED.numero_tarjeta_masked
    ;
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)

    conn.commit()

    if logger:
        logger.info(f"BD: tarjetas cargadas desde {csv_path.name} -> {len(rows)} filas")


def load_cleaned_to_postgres(output_dir: Path, logger=None):
    output_dir = Path(output_dir)

    clientes_files = sorted(output_dir.glob("Clientes-*.cleaned.csv"))
    tarjetas_files = sorted(output_dir.glob("Tarjetas-*.cleaned.csv"))

    if logger:
        logger.info(f"BD: found cleaned clientes={len(clientes_files)} tarjetas={len(tarjetas_files)}")

    conn = _get_conn()
    try:
        # Primero clientes (por FK)
        for f in clientes_files:
            _load_clientes_csv(conn, f, logger=logger)

        # Luego tarjetas
        for f in tarjetas_files:
            _load_tarjetas_csv(conn, f, logger=logger)
    finally:
        conn.close()
