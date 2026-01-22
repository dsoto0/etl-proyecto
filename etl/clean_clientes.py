import pandas as pd
import re
import unicodedata

def _normalize_text(value):
    if pd.isna(value) or value is None:
        return None
    s = str(value).strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    s = " ".join(s.split())
    return s

def _title_name(s: str) -> str | None:
    """Capitaliza nombres/apellidos respetando guiones: 'ana-maria' -> 'Ana-Maria'."""
    if s is None:
        return None
    parts = str(s).split("-")
    parts = [p[:1].upper() + p[1:].lower() if p else "" for p in parts]
    return "-".join(parts)

def _clean_dni(dni: str) -> str | None:
    """Quita espacios/guiones/puntos y deja en formato 8dig+letra si venía parecido."""
    if dni is None:
        return None
    s = re.sub(r"[\s\-.]", "", str(dni)).upper()
    return s if s else None

def _clean_phone(phone: str) -> str | None:
    """Deja solo dígitos; si viniera prefijo +34 lo conserva como dígitos."""
    if phone is None:
        return None
    digits = re.sub(r"\D", "", str(phone))
    return digits if digits else None

def _mask_dni(dni: str) -> str | None:
    """Enmascara: 12345678Z -> ******78Z (sin exponer el número completo)."""
    if not dni:
        return None
    s = str(dni)
    if len(s) >= 3:
        return "*" * (len(s) - 3) + s[-3:]
    return "***"

def clean_dataframe_clientes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 1) Normalización genérica en todo
    for col in df.columns:
        df[col] = df[col].apply(_normalize_text)

    # 2) Reglas por campo (si existen)
    if "correo" in df.columns:
        df["correo"] = df["correo"].str.lower()

    for c in ["nombre", "apellido1", "apellido2"]:
        if c in df.columns:
            df[c] = df[c].apply(_title_name)

    if "dni" in df.columns:
        df["dni"] = df["dni"].apply(_clean_dni)
        df["dni_masked"] = df["dni"].apply(_mask_dni)  # <- enmascarado

    if "telefono" in df.columns:
        df["telefono"] = df["telefono"].apply(_clean_phone)

    return df