import sys
from pathlib import Path

# Añade la raíz del proyecto al PYTHONPATH
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from etl.file_discovery import discover_files
from etl.reader import read_csv_safe
from etl.cleaning import clean_dataframe

INPUT_PATH = "data/raw"
OUTPUT_PATH = "data/output"

def main():
    clientes, tarjetas, ignored = discover_files(INPUT_PATH)

    print(f"Clientes encontrados: {len(clientes)}")
    print(f"Tarjetas encontradas: {len(tarjetas)}")
    print(f"Ficheros ignorados: {len(ignored)}")

    for file in clientes:
        df = read_csv_safe(file)
        df = clean_dataframe(df)

        output_file = Path(OUTPUT_PATH) / f"{file.stem}.cleaned.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)

        df.to_csv(output_file, index=False)
        print(f"Archivo generado: {output_file}")

if __name__ == "__main__":
    main()
