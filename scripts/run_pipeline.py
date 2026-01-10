from etl.file_discovery import discover_files
from etl.reader import read_csv_safe
from etl.cleaning import clean_dataframe

INPUT_PATH = "data/raw"
OUTPUT_PATH = "data/output"

def main():
    clientes, tarjetas, ignored = discover_files(INPUT_PATH)

    for file in clientes:
        df = read_csv_safe(file)
        df = clean_dataframe(df)
        output_file = f"{OUTPUT_PATH}/{file.stem}.cleaned.csv"
        df.to_csv(output_file, index=False)

if __name__ == "__main__":
    main()
