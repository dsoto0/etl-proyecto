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


def ensure_schema(conn, logger=None):
    """
    Crea las tablas en public si no existen.
    Esto hace que si borras tablas y vuelves a ejecutar el pipeline, se vuelvan a crear.
    """
    ddl = """
          CREATE TABLE IF NOT EXISTS public.clientes (
                                                         cod_cliente VARCHAR(10) PRIMARY KEY,
              nombre VARCHAR(100),
              apellido1 VARCHAR(100),
              apellido2 VARCHAR(100),
              dni VARCHAR(20),
              correo VARCHAR(150),
              telefono VARCHAR(30),
              dni_ok BOOLEAN,
              dni_ko BOOLEAN,
              telefono_ok BOOLEAN,
              telefono_ko BOOLEAN,
              correo_ok BOOLEAN,
              correo_ko BOOLEAN
              );

          CREATE TABLE IF NOT EXISTS public.tarjetas (
                                                         id_tarjeta BIGSERIAL PRIMARY KEY,
                                                         cod_cliente VARCHAR(10) NOT NULL,
              fecha_exp VARCHAR(7),
              numero_tarjeta_masked VARCHAR(25),
              numero_tarjeta_hash VARCHAR(80) NOT NULL,
              CONSTRAINT fk_tarjetas_cliente
              FOREIGN KEY (cod_cliente) REFERENCES public.clientes(cod_cliente)
              ON UPDATE CASCADE ON DELETE CASCADE,
              CONSTRAINT uq_tarjeta_cliente_hash
              UNIQUE (cod_cliente, numero_tarjeta_hash)
              ); \
          """
    with conn.cursor() as cur:
        # Debug útil para ver dónde estás conectado
        cur.execute("SELECT current_user, current_database(), current_schema();")
        user, dbname, schema = cur.fetchone()
        if logger:
            logger.info(f"BD: conectado como user='{user}' db='{dbname}' schema='{schema}'")

        cur.execute(ddl)

    conn.commit()
    if logger:
        logger.info("BD: tablas verificadas/creadas (public.clientes, public.tarjetas)")


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
          INSERT INTO public.clientes (
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
          ; \
          """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)

    conn.commit()
    if logger:
        logger.info(f"BD: clientes cargados desde {csv_path.name} -> {len(rows)} filas")


def _load_tarjetas_csv(conn, csv_path: Path, logger=None):
    df = pd.read_csv(csv_path, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    cols = ["cod_cliente", "fecha_exp", "numero_tarjeta_masked", "numero_tarjeta_hash"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]

    rows = [tuple(x) for x in df.to_numpy()]

    sql = """
          INSERT INTO public.tarjetas (cod_cliente, fecha_exp, numero_tarjeta_masked, numero_tarjeta_hash)
          VALUES %s
              ON CONFLICT (cod_cliente, numero_tarjeta_hash) DO UPDATE SET
              fecha_exp=EXCLUDED.fecha_exp,
                                                                    numero_tarjeta_masked=EXCLUDED.numero_tarjeta_masked
          ; \
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
        # ✅ esto es lo que te faltaba: si borras tablas, aquí se recrean
        ensure_schema(conn, logger=logger)

        # Primero clientes (por FK)
        for f in clientes_files:
            _load_clientes_csv(conn, f, logger=logger)

        # Luego tarjetas
        for f in tarjetas_files:
            _load_tarjetas_csv(conn, f, logger=logger)
    finally:
        conn.close()