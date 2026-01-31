import os
import re
from datetime import datetime
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
  # Carga variables de entorno desde .env en el directorio padre del proyecto
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env", override=True)

    host = _clean_env(os.getenv("PGHOST", "localhost"))
    port = int(_clean_env(os.getenv("PGPORT", "5432")))
    dbname = _clean_env(os.getenv("PGDATABASE", "etlproyect"))
    user = _clean_env(os.getenv("PGUSER", "postgres"))
    password = _clean_env(os.getenv("PGPASSWORD", ""))

    os.environ["PGCLIENTENCODING"] = "UTF8"

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
    Nota: si cambias constraints y la tabla ya existe, debes hacer DROP TABLE antes.
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

          -- TARJETAS: 1 fila por cod_cliente (evita duplicados)
          CREATE TABLE IF NOT EXISTS public.tarjetas (
                                                         cod_cliente VARCHAR(10) PRIMARY KEY,
              fecha_exp VARCHAR(7),
              numero_tarjeta_masked VARCHAR(25),
              numero_tarjeta_hash VARCHAR(80) NOT NULL,
              CONSTRAINT fk_tarjetas_cliente
              FOREIGN KEY (cod_cliente)
              REFERENCES public.clientes(cod_cliente)
              ON UPDATE CASCADE ON DELETE CASCADE
              );
          """

    with conn.cursor() as cur:
        cur.execute("SELECT current_user, current_database(), current_schema();")
        user, dbname, schema = cur.fetchone()
        if logger:
            logger.info(f"BD: conectado como user='{user}' db='{dbname}' schema='{schema}'")

        cur.execute(ddl)

    conn.commit()
    if logger:
        logger.info("BD: tablas verificadas/creadas (public.clientes, public.tarjetas)")


def _load_clientes_csv(conn, csv_path: Path, logger=None):
    # Carga un CSV de clientes (cleaned) haciendo UPSERT en public.clientes
    df = pd.read_csv(csv_path, dtype=str, sep=";", encoding="utf-8")

    df.columns = [c.strip() for c in df.columns]
    if "cod cliente" in df.columns:
        df = df.rename(columns={"cod cliente": "cod_cliente"})

    rename_flags = {
        "DNI_OK": "dni_ok",
        "DNI_KO": "dni_ko",
        "Telefono_OK": "telefono_ok",
        "Telefono_KO": "telefono_ko",
        "Correo_OK": "correo_ok",
        "Correo_KO": "correo_ko",
    }
    df = df.rename(columns=rename_flags)

    df = df.loc[:, ~df.columns.duplicated()]

    for col in ["dni_ok", "dni_ko", "telefono_ok", "telefono_ko", "correo_ok", "correo_ko"]:
        if col in df.columns:
            df[col] = df[col].map(_yn_to_bool)

    cols = [
        "cod_cliente", "nombre", "apellido1", "apellido2",
        "dni", "correo", "telefono",
        "dni_ok", "dni_ko", "telefono_ok", "telefono_ko", "correo_ok", "correo_ko",
    ]

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
                                               correo_ko=EXCLUDED.correo_ko;
          """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)

    conn.commit()
    if logger:
        logger.info(f"BD: clientes cargados desde {csv_path.name} -> {len(rows)} filas")


def _existing_client_codes(conn) -> set[str]:
    # Devuelve el set de cod_cliente existentes en public.clientes
    with conn.cursor() as cur:
        cur.execute("SELECT cod_cliente FROM public.clientes;")
        return {r[0] for r in cur.fetchall()}


#  TARJETAS: consolidación por cod_cliente (mayor fecha_exp)

def _merge_tarjetas_keep_latest(tarjetas_files: list[Path]) -> pd.DataFrame:
    # Lee múltiples CSV de tarjetas cleaned y los fusiona,
    dfs = []
    for f in tarjetas_files:
        #  cada uno en un DataFrame
        dfs.append(pd.read_csv(f, dtype=str, sep=";", encoding="utf-8"))

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)
    df.columns = [c.strip() for c in df.columns]

    if "cod cliente" in df.columns:
        df.rename(columns={"cod cliente": "cod_cliente"}, inplace=True)

    for c in ["cod_cliente", "fecha_exp", "numero_tarjeta_masked", "numero_tarjeta_hash"]:
        if c not in df.columns:
            df[c] = None

    df["_fecha_dt"] = pd.to_datetime(df["fecha_exp"].astype(str).str.strip() + "-01", errors="coerce")

    # Ordenar y quedarnos con la más “nueva”
    df = df.sort_values(by=["cod_cliente", "_fecha_dt"], ascending=[True, True])
    df = df.drop_duplicates(subset=["cod_cliente"], keep="last")

    df = df.drop(columns=["_fecha_dt"])
    return df[["cod_cliente", "fecha_exp", "numero_tarjeta_masked", "numero_tarjeta_hash"]]


def _insert_tarjetas_df(conn, df: pd.DataFrame, logger=None, source="merged"):
    if df is None or df.empty:
        if logger:
            logger.info("BD: no hay tarjetas para insertar (df vacío)")
        return

    df = df.copy()
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
              ON CONFLICT (cod_cliente) DO UPDATE SET
              fecha_exp=EXCLUDED.fecha_exp,
                                               numero_tarjeta_masked=EXCLUDED.numero_tarjeta_masked,
                                               numero_tarjeta_hash=EXCLUDED.numero_tarjeta_hash;
          """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)

    conn.commit()
    if logger:
        logger.info(f"BD: tarjetas cargadas desde {source} -> {len(rows)} filas")


#  Patrón para identificar tarjetas cleaned con fecha ISO
_RX_TARJETAS = re.compile(r"^Tarjetas-(\d{4}-\d{2}-\d{2})\.cleaned\.csv$", re.IGNORECASE)

def _tarjetas_fecha(csv_path: Path):
    m = _RX_TARJETAS.match(csv_path.name)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y-%m-%d").date()


def load_cleaned_to_postgres(output_dir: Path, logger=None):
    output_dir = Path(output_dir)

    clientes_files = sorted(output_dir.glob("Clientes-*.cleaned.csv"))
    tarjetas_files = sorted(output_dir.glob("Tarjetas-*.cleaned.csv"))

    if logger:
        logger.info(f"BD: found cleaned clientes={len(clientes_files)} tarjetas={len(tarjetas_files)}")

    # Ordenar tarjetas por fecha si se puede (para logging)
    tarjetas_con_fecha = []
    for f in tarjetas_files:
        fecha = _tarjetas_fecha(f)
        if fecha:
            tarjetas_con_fecha.append((f, fecha))
    if tarjetas_con_fecha:
        tarjetas_files = [x[0] for x in sorted(tarjetas_con_fecha, key=lambda x: x[1])]

    conn = _get_conn()
    try:
        ensure_schema(conn, logger=logger)

        #  CLIENTES: base + incremental (UPSERT)
        for f in clientes_files:
            _load_clientes_csv(conn, f, logger=logger)

        #  TARJETAS: fusionar y quedarse con mayor fecha_exp por cod_cliente
        df_merged = _merge_tarjetas_keep_latest(tarjetas_files)

        if logger:
            logger.info(f"BD: tarjetas merged -> {len(df_merged)} filas tras deduplicar por cod_cliente")

        #  filtro FK: solo clientes existentes
        existing = _existing_client_codes(conn)
        before = len(df_merged)
        df_merged = df_merged[df_merged["cod_cliente"].isin(existing)]
        dropped = before - len(df_merged)

        if logger and dropped:
            logger.warning(f"BD: tarjetas descartadas por FK (cliente inexistente) -> {dropped} filas")

        # Si es TOTAL, truncamos antes
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE public.tarjetas;")
        conn.commit()

        _insert_tarjetas_df(conn, df_merged, logger=logger, source="merged tarjetas (max fecha_exp)")

    finally:
        conn.close()
