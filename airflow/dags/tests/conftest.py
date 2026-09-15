"""
Shared pytest bootstrap for the pipeline tests.

The DAG modules import their libraries as ``modules.x``, which works in the
container because /opt/airflow/dags is on sys.path. Outside it, pytest needs
the same thing — so this puts ``airflow/dags`` on sys.path once, here,
instead of each test file rolling its own (one of which hardcoded an
absolute path from a different machine).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

DAGS_DIR = Path(__file__).resolve().parents[1]
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

# Stores read these at connect time. Tests use mongomock and never reach a
# real server, but the defaults keep them from depending on a developer's
# shell environment.
os.environ.setdefault("MONGODB_DB", "memes_test")
os.environ.setdefault("DOM_COMPRESSION", "zlib")
