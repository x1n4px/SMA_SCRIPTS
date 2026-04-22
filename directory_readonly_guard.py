#!/usr/bin/env python3
"""
Guardas de solo lectura para proteger un directorio durante una ejecucion.
"""

from __future__ import annotations

import builtins
import contextlib
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator, Optional, Union


class ProtectedDirectoryWriteError(PermissionError):
    """Se intento modificar un directorio protegido en modo solo lectura."""


PathInput = Union[str, bytes, os.PathLike]


@dataclass(frozen=True)
class DirectoryFingerprint:
    digest: str
    entries: int


def _to_resolved_path(value) -> Optional[Path]:
    if isinstance(value, int):
        return None
    if isinstance(value, bytes):
        value = os.fsdecode(value)
    try:
        return Path(value).expanduser().resolve()
    except Exception:
        return None


def _is_inside(path: Optional[Path], base_dir: Path) -> bool:
    if path is None:
        return False
    try:
        path.relative_to(base_dir)
        return True
    except ValueError:
        return False


def _assert_not_protected(path_value, base_dir: Path, operation: str) -> None:
    target = _to_resolved_path(path_value)
    if _is_inside(target, base_dir):
        raise ProtectedDirectoryWriteError(
            f"Operacion bloqueada ({operation}) en directorio protegido: {target}"
        )


def fingerprint_directory(base_dir: PathInput) -> DirectoryFingerprint:
    root = Path(base_dir).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        raise FileNotFoundError(f"Directorio no valido para fingerprint: {root}")

    hasher = hashlib.sha256()
    entries = 0

    root_stat = root.stat(follow_symlinks=False)
    hasher.update(
        f".|d|{root_stat.st_mode:o}|{root_stat.st_size}|{root_stat.st_mtime_ns}\n".encode("utf-8")
    )
    entries += 1

    pending = [root]
    while pending:
        current = pending.pop()
        with os.scandir(current) as iterator:
            children = sorted(iterator, key=lambda item: item.name)

        for item in children:
            item_path = Path(item.path)
            rel = item_path.relative_to(root).as_posix()
            stat_data = item.stat(follow_symlinks=False)

            if item.is_dir(follow_symlinks=False):
                kind = "d"
                pending.append(item_path)
            elif item.is_file(follow_symlinks=False):
                kind = "f"
            elif item.is_symlink():
                kind = "l"
            else:
                kind = "o"

            hasher.update(
                f"{rel}|{kind}|{stat_data.st_mode:o}|{stat_data.st_size}|{stat_data.st_mtime_ns}\n".encode(
                    "utf-8"
                )
            )
            entries += 1

            if kind == "l":
                try:
                    target = os.readlink(item.path)
                except OSError:
                    target = "<unreadable>"
                hasher.update(f"{rel}|target|{target}\n".encode("utf-8"))

    return DirectoryFingerprint(digest=hasher.hexdigest(), entries=entries)


@contextlib.contextmanager
def enforce_read_only_directory(
    base_dir: PathInput,
    on_integrity_ok: Optional[Callable[[DirectoryFingerprint, DirectoryFingerprint], None]] = None,
) -> Iterator[DirectoryFingerprint]:
    root = Path(base_dir).expanduser().resolve()
    before = fingerprint_directory(root)

    original_open = builtins.open
    original_os_open = os.open
    original_remove = os.remove
    original_unlink = os.unlink
    original_rename = os.rename
    original_replace = os.replace
    original_mkdir = os.mkdir
    original_rmdir = os.rmdir
    original_chmod = os.chmod
    original_chown = os.chown
    original_utime = os.utime

    write_flags = 0
    for flag_name in ("O_WRONLY", "O_RDWR", "O_APPEND", "O_CREAT", "O_TRUNC"):
        write_flags |= int(getattr(os, flag_name, 0))

    def guarded_open(file, mode="r", *args, **kwargs):
        if any(token in str(mode) for token in ("w", "a", "x", "+")):
            _assert_not_protected(file, root, f"open(mode={mode})")
        return original_open(file, mode, *args, **kwargs)

    def guarded_os_open(path, flags, *args, **kwargs):
        if int(flags) & write_flags:
            _assert_not_protected(path, root, f"os.open(flags={flags})")
        return original_os_open(path, flags, *args, **kwargs)

    def guarded_remove(path, *args, **kwargs):
        _assert_not_protected(path, root, "os.remove")
        return original_remove(path, *args, **kwargs)

    def guarded_unlink(path, *args, **kwargs):
        _assert_not_protected(path, root, "os.unlink")
        return original_unlink(path, *args, **kwargs)

    def guarded_rename(src, dst, *args, **kwargs):
        _assert_not_protected(src, root, "os.rename(src)")
        _assert_not_protected(dst, root, "os.rename(dst)")
        return original_rename(src, dst, *args, **kwargs)

    def guarded_replace(src, dst, *args, **kwargs):
        _assert_not_protected(src, root, "os.replace(src)")
        _assert_not_protected(dst, root, "os.replace(dst)")
        return original_replace(src, dst, *args, **kwargs)

    def guarded_mkdir(path, *args, **kwargs):
        _assert_not_protected(path, root, "os.mkdir")
        return original_mkdir(path, *args, **kwargs)

    def guarded_rmdir(path, *args, **kwargs):
        _assert_not_protected(path, root, "os.rmdir")
        return original_rmdir(path, *args, **kwargs)

    def guarded_chmod(path, *args, **kwargs):
        _assert_not_protected(path, root, "os.chmod")
        return original_chmod(path, *args, **kwargs)

    def guarded_chown(path, *args, **kwargs):
        _assert_not_protected(path, root, "os.chown")
        return original_chown(path, *args, **kwargs)

    def guarded_utime(path, *args, **kwargs):
        _assert_not_protected(path, root, "os.utime")
        return original_utime(path, *args, **kwargs)

    builtins.open = guarded_open
    os.open = guarded_os_open
    os.remove = guarded_remove
    os.unlink = guarded_unlink
    os.rename = guarded_rename
    os.replace = guarded_replace
    os.mkdir = guarded_mkdir
    os.rmdir = guarded_rmdir
    os.chmod = guarded_chmod
    os.chown = guarded_chown
    os.utime = guarded_utime

    try:
        yield before
    finally:
        builtins.open = original_open
        os.open = original_os_open
        os.remove = original_remove
        os.unlink = original_unlink
        os.rename = original_rename
        os.replace = original_replace
        os.mkdir = original_mkdir
        os.rmdir = original_rmdir
        os.chmod = original_chmod
        os.chown = original_chown
        os.utime = original_utime

        after = fingerprint_directory(root)
        if before != after:
            raise ProtectedDirectoryWriteError(
                "Se detectaron cambios en el directorio protegido. "
                f"Antes={before.digest}/{before.entries} | Despues={after.digest}/{after.entries}"
            )
        if on_integrity_ok is not None:
            on_integrity_ok(before, after)
