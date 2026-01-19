import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

def setup_logger(
        name: str = "etl",
        log_file: str = "logs/etl.log",
        level: int = logging.INFO
) -> logging.Logger:
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Evita duplicar handlers si se llama varias veces
    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=1_000_000,   # 1MB
        backupCount=5,
        encoding="utf-8"
    )
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
