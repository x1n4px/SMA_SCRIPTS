#!/usr/bin/env python3
"""
Compatibility shim for the local virtual environment's ``mysql`` package.

When scripts are launched with ``/usr/bin/python`` from this repository, Python
finds this package first. We extend the package search path so submodules such
as ``mysql.connector`` resolve from ``.venv`` without requiring manual
activation.
"""

from __future__ import annotations

import sys
from pathlib import Path
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

repo_root = Path(__file__).resolve().parent.parent
version = f"python{sys.version_info.major}.{sys.version_info.minor}"
venv_mysql = repo_root / ".venv" / "lib" / version / "site-packages" / "mysql"

if venv_mysql.is_dir():
    venv_mysql_str = str(venv_mysql)
    if venv_mysql_str not in __path__:
        __path__.append(venv_mysql_str)
