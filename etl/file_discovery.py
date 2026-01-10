import re
from pathlib import Path

CLIENTES_PATTERN = re.compile(r"Clientes-\d{4}-\d{2}-\d{2}\.csv$")
TARJETAS_PATTERN = re.compile(r"Tarjetas-\d{4}-\d{2}-\d{2}\.csv$")

def discover_files(input_path: str):
    input_dir = Path(input_path)

    clientes = []
    tarjetas = []
    ignored = []

    for file in input_dir.iterdir():
        if CLIENTES_PATTERN.match(file.name):
            clientes.append(file)
        elif TARJETAS_PATTERN.match(file.name):
            tarjetas.append(file)
        else:
            ignored.append(file)

    return clientes, tarjetas, ignored
