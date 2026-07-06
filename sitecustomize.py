#!/usr/bin/env python3
"""
Bootstrap local dependencies from the repository virtual environment.

Python loads ``sitecustomize`` automatically during startup when it is
available on ``sys.path``. This lets the project reuse packages installed in
``.venv`` even when scripts are launched with ``/usr/bin/python``.
"""

from __future__ import annotations

import sys
from pathlib import Path


def _venv_site_packages() -> Path | None:
    repo_root = Path(__file__).resolve().parent
    version = f"python{sys.version_info.major}.{sys.version_info.minor}"
    candidate = repo_root / ".venv" / "lib" / version / "site-packages"
    if candidate.is_dir():
        return candidate
    return None


venv_site = _venv_site_packages()
if venv_site:
    venv_path = str(venv_site)
    if venv_path not in sys.path:
        sys.path.insert(0, venv_path)
