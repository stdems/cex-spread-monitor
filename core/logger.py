import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging() -> None:
    Path("logs").mkdir(exist_ok=True)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    console.setLevel(logging.INFO)


    # No DEBUG
    info_file = RotatingFileHandler(
        "logs/info.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    info_file.setFormatter(fmt)
    info_file.setLevel(logging.INFO)

    error_file = RotatingFileHandler(
        "logs/errors.log", maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    error_file.setFormatter(fmt)
    error_file.setLevel(logging.WARNING)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(console)
    root.addHandler(info_file)
    root.addHandler(error_file)

    for lib in ("aiogram", "aiohttp", "websockets", "asyncio"):
        logging.getLogger(lib).setLevel(logging.WARNING)
