import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

def setup_logging(log_dir: str = "logs", filename: str = "pipeline.log") -> None:
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_path = Path(log_dir) / filename

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Clean handlers if re-run in notebooks/REPL
    for h in list(logger.handlers):
        logger.removeHandler(h)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(log_path, maxBytes=2_000_000, backupCount=3)
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
