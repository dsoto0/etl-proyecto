import pandas as pd
import re


def is_valid_cod_cliente(value: str) -> bool:
    if pd.isna(value):
        return False
    return bool(re.fullmatch(r"C\d{3}", str(value).strip()))


def is_valid_fecha_exp(value: str) -> bool:
    if pd.isna(value):
        return False
    value = str(value).strip()
    if not re.fullmatch(r"\d{4}-\d{2}", value):
        return False
    year = int(value[:4])
    month = int(value[5:7])
    return 1 <= month <= 12 and 1900 <= year <= 2100


def is_valid_card(card_digits: str) -> bool:
    if pd.isna(card_digits):
        return False
    s = str(card_digits).strip()
    return s.isdigit() and len(s) >= 12


def validate_tarjetas(df: pd.DataFrame):

    df = df.copy()
    errors = []

    df["CodCliente_OK"] = df["cod_cliente"].apply(lambda x: "Y" if is_valid_cod_cliente(x) else "N")
    df["CodCliente_KO"] = df["CodCliente_OK"].apply(lambda x: "N" if x == "Y" else "Y")

    df["FechaExp_OK"] = df["fecha_exp"].apply(lambda x: "Y" if is_valid_fecha_exp(x) else "N")
    df["FechaExp_KO"] = df["FechaExp_OK"].apply(lambda x: "N" if x == "Y" else "Y")

    df["Tarjeta_OK"] = df["card_clean"].apply(lambda x: "Y" if is_valid_card(x) else "N")
    df["Tarjeta_KO"] = df["Tarjeta_OK"].apply(lambda x: "N" if x == "Y" else "Y")

    invalid_mask = (df["CodCliente_OK"] == "N") | (df["FechaExp_OK"] == "N") | (df["Tarjeta_OK"] == "N")
    rejected = df[invalid_mask].copy()

    if not rejected.empty:
        rejected["origen"] = "TARJETAS"
        rejected["error"] = "validacion_tarjeta"

        def detalle(row):
            motivos = []
            if row["CodCliente_OK"] == "N":
                motivos.append("cod_cliente_invalido")
            if row["FechaExp_OK"] == "N":
                motivos.append("fecha_exp_invalida")
            if row["Tarjeta_OK"] == "N":
                motivos.append("tarjeta_invalida")
            return "|".join(motivos)

        rejected["error_detalle"] = rejected.apply(detalle, axis=1)
        errors.append(rejected)

    df_valid = df[~invalid_mask].copy()
    return df_valid, errors