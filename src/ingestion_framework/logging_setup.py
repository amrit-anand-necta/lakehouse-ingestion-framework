"""Structured logging for the framework.

Every module gets its logger via ``get_logger(__name__)`` so log lines show
exactly which module emitted them. The format includes a ``run_id`` when one
is bound, so all lines from one table-run can be grepped together.
"""

import logging
import sys

_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
_configured = False


def get_logger(name: str) -> logging.Logger:
    """Return a logger configured with the framework's standard format.

    Root configuration happens exactly once (idempotent), no matter how many
    modules call this — re-running ``logging.basicConfig`` style setup in every
    module would duplicate handlers and print every line twice.
    """
    global _configured
    if not _configured:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
        root = logging.getLogger("ingestion_framework")
        root.addHandler(handler)
        root.setLevel(logging.INFO)
        root.propagate = False
        _configured = True

    # Child loggers ("ingestion_framework.readers.csv_reader") inherit the
    # root handler automatically — they need no setup of their own.
    if not name.startswith("ingestion_framework"):
        name = f"ingestion_framework.{name}"
    return logging.getLogger(name)
