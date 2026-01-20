import pandas as pd
import re

def is_valid_dni(dni: str) -> bool:
    """
    Validación "soft": SOLO formato.
    Acepta 8 dígitos + 1 letra (A-Z).
    No comprueba letra real.
    """
    if not dni:
        return False

    dni = str(dni).strip().upper()
    dni = re.sub(r"[\s\-\.]", "", dni)

    return bool(re.fullmatch(r"\d{8}[A-Z]", dni))


def is_valid_phone(phone: str) -> bool:
    if not phone:
        return False

    phone = re.sub(r"\D", "", str(phone))
    return bool(re.fullmatch(r"[0-9]{9}", phone))


def is_valid_email(email: str) -> bool:
    if not email:
        return False

    email = str(email).strip().lower()
    return bool(re.fullmatch(r"[^@]+@[^@]+\.[^@]+", email))


def validate_clientes(df: pd.DataFrame):
    df = df.copy()
    errors = []

    # Normalización básica (igual que antes)
    df["dni"] = df["dni"].astype(str).str.strip().str.upper()
    df["telefono"] = df["telefono"].astype(str).str.strip()
    df["correo"] = df["correo"].astype(str).str.strip().str.lower()

    # Flags de validación (SIN CAMBIOS)
    df["DNI_OK"] = df["dni"].apply(lambda x: "Y" if is_valid_dni(x) else "N")
    df["DNI_KO"] = df["DNI_OK"].apply(lambda x: "Y" if x == "N" else "N")

    df["Telefono_OK"] = df["telefono"].apply(lambda x: "Y" if is_valid_phone(x) else "N")
    df["Telefono_KO"] = df["Telefono_OK"].apply(lambda x: "Y" if x == "N" else "N")

    df["Correo_OK"] = df["correo"].apply(lambda x: "Y" if is_valid_email(x) else "N")
    df["Correo_KO"] = df["Correo_OK"].apply(lambda x: "Y" if x == "N" else "N")

    # Filas inválidas
    invalid_mask = (
            (df["DNI_OK"] == "N")
            | (df["Telefono_OK"] == "N")
            | (df["Correo_OK"] == "N")
    )

    invalid = df[invalid_mask].copy()

    if not invalid.empty:
        invalid["origen"] = "CLIENTES"
        invalid["error"] = "validacion_cliente"

        def detalle(row):
            motivos = []
            if row["DNI_OK"] == "N":
                motivos.append("dni_invalido")
            if row["Telefono_OK"] == "N":
                motivos.append("telefono_invalido")
            if row["Correo_OK"] == "N":
                motivos.append("correo_invalido")
            return "|".join(motivos) if motivos else "desconocido"

        invalid["error_detalle"] = invalid.apply(detalle, axis=1)
        errors.append(invalid)

    # DataFrame válido
    df_valid = df[~invalid_mask].copy()

    # 🔹 CAMBIO MÍNIMO 1: enmascarar el DNI SOBRE LA MISMA COLUMNA
    df_valid["dni"] = df_valid["dni"].apply(
        lambda x: "******" + x[-3:] if isinstance(x, str) and len(x) >= 3 else x
    )

    # 🔹 CAMBIO MÍNIMO 2: asegurar cod_cliente al principio
    cols = df_valid.columns.tolist()
    if "cod_cliente" in cols:
        cols.insert(0, cols.pop(cols.index("cod_cliente")))
    df_valid = df_valid[cols]

    return df_valid, errors
