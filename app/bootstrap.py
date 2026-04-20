from __future__ import annotations

import logging

from app.container import Container
from app.logging import configure_logging
from app.settings import load_settings


def build_container() -> Container:
    configure_logging()
    settings = load_settings()
    logging.getLogger(__name__).info("Genesis bootstrap ready")
    return Container(settings=settings)


def start_runtime(mode: str = "telegram") -> Container:
    container = build_container()
    logging.getLogger(__name__).info("Selected runtime: %s", mode)
    return container
