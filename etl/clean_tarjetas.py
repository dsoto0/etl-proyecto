import pandas as pd
import re
import hashlib
import os

SALT = os.getenv("CARD_SALT", "etl_grupo_salt")

"""Limpieza de datos de tarjetas en un DataFrame de pandas. Normaliza números de tarjeta, enmascara y hashea."""
def normalize_card(card: str) -> str | None:
    if pd.isna(card) or card is None:
        return None
    digits = re.sub(r"[^0-9]", "", str(card))
    return digits if digits else None

"""Enmascara el número de tarjeta dejando solo los últimos 4 dígitos visibles."""
def mask_card(card_digits: str) -> str | None:
    if not card_digits or len(card_digits) < 4:
        return None
    return f"XXXX-XXXX-XXXX-{card_digits[-4:]}"

"""Hashea el número de tarjeta usando SHA-256 con una sal definida en la variable de entorno CARD_SALT."""
def hash_card(card_digits: str) -> str | None:
    if not card_digits:
        return None
    return hashlib.sha256((SALT + card_digits).encode("utf-8")).hexdigest()

"""Aplica la limpieza a un DataFrame de tarjetas."""
def clean_tarjetas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    df["cod_cliente"] = df["cod_cliente"].astype(str).str.strip()
    df["fecha_exp"] = df["fecha_exp"].astype(str).str.strip()

    df["card_clean"] = df["numero_tarjeta"].apply(normalize_card)
    df["numero_tarjeta_masked"] = df["card_clean"].apply(mask_card)
    df["numero_tarjeta_hash"] = df["card_clean"].apply(hash_card)

    # Eliminar sensibles (CVV nunca se guarda)
    df.drop(columns=["numero_tarjeta", "cvv"], inplace=True, errors="ignore")

    return df