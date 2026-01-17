import pandas as pd
import re
import hashlib
import os

# IMPORTANTE: lo ideal es definir CARD_SALT en variable de entorno y NO commitearla
SALT = os.getenv("CARD_SALT", "etl_grupo_salt")


def normalize_card(card: str) -> str | None:
    if pd.isna(card) or card is None:
        return None
    digits = re.sub(r"[^0-9]", "", str(card))
    return digits if digits else None


def mask_card(card_digits: str) -> str | None:
    if not card_digits or len(card_digits) < 4:
        return None
    return f"XXXX-XXXX-XXXX-{card_digits[-4:]}"


def hash_card(card_digits: str) -> str | None:
    if not card_digits:
        return None
    return hashlib.sha256((SALT + card_digits).encode("utf-8")).hexdigest()


def clean_tarjetas(df: pd.DataFrame):
    """
    - Limpia formatos de tarjeta
    - Genera numero_tarjeta_masked y numero_tarjeta_hash
    - Elimina cvv COMPLETAMENTE del fichero final
    Devuelve:
      - df_limpio
      - lista de dataframes con filas rechazadas (incluye columna 'error')
    """
    df = df.copy()
    errors = []

    df.columns = [c.strip().lower() for c in df.columns]

    df["cod_cliente"] = df["cod_cliente"].astype(str).str.strip()
    df["fecha_exp"] = df["fecha_exp"].astype(str).str.strip()

    df["card_clean"] = df["numero_tarjeta"].apply(normalize_card)
    df["numero_tarjeta_masked"] = df["card_clean"].apply(mask_card)
    df["numero_tarjeta_hash"] = df["card_clean"].apply(hash_card)

    invalid = df[df["card_clean"].isna()]
    if not invalid.empty:
        invalid = invalid.copy()
        invalid["error"] = "tarjeta_invalida"
        errors.append(invalid)

    # Eliminar sensibles: numero_tarjeta original + cvv
    df.drop(columns=["numero_tarjeta", "cvv", "card_clean"], inplace=True, errors="ignore")

    return df, errors
