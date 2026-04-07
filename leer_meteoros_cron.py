#!/usr/bin/env python3
"""
Procesador automático de meteoros orientado a cron.

Objetivos:
- Ejecución no interactiva.
- Logs claros para seguimiento operativo.
- Archivo de log automático en logs/ con fecha completa.
- Evitar solapes entre ejecuciones concurrentes.
"""

from __future__ import annotations

import argparse
import fcntl
import logging
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import mysql.connector
from mysql.connector import Error

try:
    from config_db import DB_CONFIG, CONNECTION_CONFIG, TABLES, validate_config
except ImportError:
    print("ERROR: No se pudo importar config_db.py")
    sys.exit(1)


DEFAULT_RELATIVE_BASE = Path("Carpeta-meteoro-procesado/home/sma/Meteoros/Detecciones")
DEFAULT_LOG_DIR = Path("logs")
DEFAULT_LOCK_FILE = Path("logs/leer_meteoros_cron.lock")
LOADER_SCRIPTS = {
    "z": "CargaInformesZ.py",
    "rad": "CargaInformesRad.py",
    "fot": "CargaInformesFot_MySQL.py",
}
KIND_LABELS = {
    "z": "Informes-Z",
    "rad": "Informes-Radiante",
    "fot": "Informes-Fotometria",
}


@dataclass(frozen=True)
class DetectionDirectory:
    path: Path
    date_str: str
    time_str: str
    dt: datetime

    @property
    def label(self) -> str:
        return (
            f"{self.date_str[:4]}-{self.date_str[4:6]}-{self.date_str[6:8]} "
            f"{self.time_str[:2]}:{self.time_str[2:4]}:{self.time_str[4:6]}"
        )


def default_log_path(log_dir: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return log_dir / f"leer_meteoros_cron_{timestamp}.log"


def configure_logger(verbose: bool, log_file: Path) -> logging.Logger:
    logger = logging.getLogger("leer_meteoros_cron")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    log_file.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def resolve_base_path(user_path: Optional[str]) -> Path:
    if user_path:
        return Path(user_path).expanduser().resolve()
    return (Path(__file__).resolve().parent / DEFAULT_RELATIVE_BASE).resolve()


def resolve_log_path(log_file: Optional[str], log_dir: Optional[str]) -> Path:
    if log_file:
        return Path(log_file).expanduser().resolve()
    base_dir = Path(log_dir).expanduser().resolve() if log_dir else (Path(__file__).resolve().parent / DEFAULT_LOG_DIR).resolve()
    return default_log_path(base_dir)


def acquire_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("w", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise RuntimeError(f"Ya hay otra ejecucion en marcha. Lock activo en {lock_path}")
    handle.write(str(datetime.now()) + "\n")
    handle.flush()
    return handle


def connect_mysql(logger: logging.Logger):
    ok, message = validate_config()
    if not ok:
        raise RuntimeError(f"Configuracion de base de datos invalida: {message}")

    attempts = CONNECTION_CONFIG.get("retry_attempts", 3)
    delay_seconds = CONNECTION_CONFIG.get("retry_delay", 5)
    timeout = CONNECTION_CONFIG.get("connection_timeout", 30)

    config = DB_CONFIG.copy()
    config["connection_timeout"] = timeout

    for attempt in range(1, attempts + 1):
        try:
            logger.info("BD | Conectando a MySQL (%s/%s)", attempt, attempts)
            conn = mysql.connector.connect(**config)
            if conn.is_connected():
                logger.info("BD | Conexion OK")
                return conn
        except Error as err:
            logger.warning("BD | Fallo de conexion (%s/%s): %s", attempt, attempts, err)
            if attempt < attempts:
                import time as time_module

                time_module.sleep(delay_seconds)

    raise RuntimeError(f"No se pudo conectar a MySQL tras {attempts} intentos")


def parse_db_datetime(fecha_val, hora_val) -> Optional[datetime]:
    if not fecha_val or not hora_val:
        return None

    parsed_date: Optional[date] = None
    if isinstance(fecha_val, date):
        parsed_date = fecha_val
    else:
        try:
            parsed_date = datetime.strptime(str(fecha_val), "%Y-%m-%d").date()
        except ValueError:
            return None

    if isinstance(hora_val, time):
        parsed_time = hora_val.replace(microsecond=0)
    else:
        raw = str(hora_val).strip()
        if " " in raw:
            raw = raw.split(" ")[-1]
        try:
            parsed_time = time.fromisoformat(raw)
        except ValueError:
            if "." in raw:
                raw = raw.split(".", 1)[0]
            parts = raw.split(":")
            if len(parts) < 2:
                return None
            if len(parts) == 2:
                parts.append("00")
            try:
                hh = int(parts[0])
                mm = int(parts[1])
                ss = int(float(parts[2]))
                parsed_time = time(hour=hh, minute=mm, second=ss)
            except ValueError:
                return None

    return datetime.combine(parsed_date, parsed_time.replace(microsecond=0))


def get_last_processed_datetime(conn, logger: logging.Logger) -> Optional[datetime]:
    table = TABLES.get("meteoro", "Meteoro")
    query = f"""
        SELECT Fecha, Hora
        FROM {table}
        WHERE Fecha IS NOT NULL AND Hora IS NOT NULL
        ORDER BY Fecha DESC, Hora DESC
        LIMIT 1
    """

    cursor = conn.cursor()
    try:
        cursor.execute(query)
        row = cursor.fetchone()
        if not row:
            logger.info("BD | No hay registros previos en %s", table)
            return None
        parsed = parse_db_datetime(row[0], row[1])
        if parsed is None:
            logger.warning("BD | Fecha/Hora no interpretable: %s / %s", row[0], row[1])
        else:
            logger.info("BD | Ultimo registro Meteoro: %s", parsed.strftime("%Y-%m-%d %H:%M:%S"))
        return parsed
    finally:
        cursor.close()


def scan_detection_directories(base_path: Path, logger: logging.Logger) -> List[DetectionDirectory]:
    if not base_path.exists() or not base_path.is_dir():
        raise FileNotFoundError(f"Ruta base no valida: {base_path}")

    detections: List[DetectionDirectory] = []

    for year_dir in sorted(base_path.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit() or len(year_dir.name) != 4:
            continue
        for date_dir in sorted(year_dir.iterdir()):
            if not date_dir.is_dir() or not date_dir.name.isdigit() or len(date_dir.name) != 8:
                continue
            for time_dir in sorted(date_dir.iterdir()):
                if not time_dir.is_dir() or not time_dir.name.isdigit() or len(time_dir.name) != 6:
                    continue
                try:
                    dt = datetime.strptime(date_dir.name + time_dir.name, "%Y%m%d%H%M%S")
                except ValueError:
                    logger.debug("SCAN | Se omite directorio invalido: %s", time_dir)
                    continue
                detections.append(
                    DetectionDirectory(
                        path=time_dir,
                        date_str=date_dir.name,
                        time_str=time_dir.name,
                        dt=dt,
                    )
                )

    detections.sort(key=lambda d: d.dt)
    return detections


def filter_pending(detections: List[DetectionDirectory], last_processed_dt: Optional[datetime]) -> List[DetectionDirectory]:
    if last_processed_dt is None:
        return detections
    return [d for d in detections if d.dt > last_processed_dt]


def discover_report_targets(detection_dir: Path) -> Dict[str, List[Path]]:
    z_dirs: Set[Path] = set()
    rad_dirs: Set[Path] = set()
    fot_dirs: Set[Path] = set()

    for trayectoria in detection_dir.iterdir():
        if not trayectoria.is_dir() or not trayectoria.name.startswith("Trayectoria-"):
            continue
        for candidate in trayectoria.iterdir():
            if candidate.is_file() and candidate.name.startswith("Informe-Z") and not candidate.name.endswith(".kml"):
                z_dirs.add(trayectoria)
                break

    for candidate in detection_dir.rglob("Informe-Radiante*.inf"):
        if candidate.is_file():
            rad_dirs.add(candidate.parent)

    for candidate in detection_dir.rglob("Informe-fotometria*"):
        if candidate.is_file():
            fot_dirs.add(candidate.parent)

    return {
        "z": sorted(z_dirs),
        "rad": sorted(rad_dirs),
        "fot": sorted(fot_dirs),
    }


def format_target_counts(targets: Dict[str, List[Path]]) -> str:
    return f"Z={len(targets['z'])} | Rad={len(targets['rad'])} | Fot={len(targets['fot'])}"


def execute_loader(script_path: Path, target_dir: Path, timeout_seconds: int) -> Tuple[str, str]:
    try:
        result = subprocess.run(
            [sys.executable, str(script_path), str(target_dir)],
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        return "timeout", "timeout"
    except Exception as exc:
        return "error", str(exc)

    output = (result.stderr or result.stdout or "").strip()
    if result.returncode == 0:
        return "ok", output
    if result.returncode == 2:
        return "obs_not_found", output
    return "error", f"code={result.returncode} {output}".strip()


def process_detections(
    detections: List[DetectionDirectory],
    timeout_seconds: int,
    dry_run: bool,
    logger: logging.Logger,
) -> Dict[str, Dict[str, int]]:
    scripts_dir = Path(__file__).resolve().parent

    summary = {
        "dirs": {"processed": 0, "without_reports": 0},
        "z": {"targets": 0, "ok": 0, "obs_not_found": 0, "error": 0, "timeout": 0},
        "rad": {"targets": 0, "ok": 0, "obs_not_found": 0, "error": 0, "timeout": 0},
        "fot": {"targets": 0, "ok": 0, "obs_not_found": 0, "error": 0, "timeout": 0},
    }

    for index, detection in enumerate(detections, start=1):
        logger.info("DIR | [%s/%s] %s", index, len(detections), detection.label)
        logger.info("DIR | Ruta: %s", detection.path)
        targets = discover_report_targets(detection.path)
        logger.info("DIR | Objetivos detectados: %s", format_target_counts(targets))

        total_targets = sum(len(targets[k]) for k in ("z", "rad", "fot"))
        if total_targets == 0:
            summary["dirs"]["without_reports"] += 1
            logger.info("DIR | Sin informes para procesar")
            continue

        summary["dirs"]["processed"] += 1

        for kind in ("z", "rad", "fot"):
            script_path = scripts_dir / LOADER_SCRIPTS[kind]
            if not targets[kind]:
                continue

            logger.info("BLOQUE | %s | %s objetivos", KIND_LABELS[kind], len(targets[kind]))
            for target_dir in targets[kind]:
                summary[kind]["targets"] += 1

                if dry_run:
                    logger.info("DRY | %s -> %s", LOADER_SCRIPTS[kind], target_dir.name)
                    continue

                status, message = execute_loader(script_path, target_dir, timeout_seconds)
                summary[kind][status] += 1

                if status == "ok":
                    logger.info("OK | %s | %s", kind, target_dir.name)
                elif status == "obs_not_found":
                    logger.warning("WARN | %s | observatorio no encontrado | %s", kind, target_dir.name)
                elif status == "timeout":
                    logger.error("TIMEOUT | %s | %s", kind, target_dir.name)
                else:
                    logger.error("ERROR | %s | %s | %s", kind, target_dir.name, message[:220])

        logger.info("DIR | Fin %s", detection.label)

    return summary


def print_run_header(
    logger: logging.Logger,
    base_path: Path,
    mode: str,
    dry_run: bool,
    timeout_seconds: int,
    log_path: Path,
    selected: List[DetectionDirectory],
) -> None:
    logger.info("=" * 80)
    logger.info("INICIO DE EJECUCION")
    logger.info("Base: %s", base_path)
    logger.info("Modo: %s", mode)
    logger.info("Dry-run: %s", "si" if dry_run else "no")
    logger.info("Timeout por cargador: %ss", timeout_seconds)
    logger.info("Log: %s", log_path)
    logger.info("Directorios seleccionados: %s", len(selected))
    if selected:
        logger.info("Rango: %s -> %s", selected[0].label, selected[-1].label)
    logger.info("=" * 80)


def print_summary(logger: logging.Logger, summary: Dict[str, Dict[str, int]]) -> None:
    logger.info("=" * 80)
    logger.info("RESUMEN FINAL")
    logger.info("Directorios con informes: %s", summary["dirs"]["processed"])
    logger.info("Directorios sin informes: %s", summary["dirs"]["without_reports"])

    for kind in ("z", "rad", "fot"):
        stats = summary[kind]
        logger.info(
            "%s | targets=%s | ok=%s | obs_not_found=%s | error=%s | timeout=%s",
            KIND_LABELS[kind],
            stats["targets"],
            stats["ok"],
            stats["obs_not_found"],
            stats["error"],
            stats["timeout"],
        )
    logger.info("=" * 80)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Procesa detecciones de meteoros en modo automatico apto para cron"
    )
    parser.add_argument("ruta_base", nargs="?", help="Ruta base de detecciones")
    parser.add_argument(
        "--mode",
        choices=["pending", "all"],
        default="pending",
        help="pending: solo posteriores al ultimo Meteoro; all: procesa todo",
    )
    parser.add_argument("--limit", type=int, default=0, help="Maximo de directorios a procesar")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout por cargador en segundos")
    parser.add_argument("--dry-run", action="store_true", help="Simula sin ejecutar scripts de carga")
    parser.add_argument("--verbose", action="store_true", help="Activa logging detallado")
    parser.add_argument("--log-file", help="Ruta completa del log")
    parser.add_argument("--log-dir", help="Directorio para logs automaticos")
    parser.add_argument("--lock-file", help="Ruta del lock para evitar ejecuciones solapadas")
    parser.add_argument("--no-lock", action="store_true", help="Desactiva el lock de ejecucion")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    log_path = resolve_log_path(args.log_file, args.log_dir)
    logger = configure_logger(verbose=args.verbose, log_file=log_path)

    lock_handle = None
    if not args.no_lock:
        try:
            lock_target = (
                Path(args.lock_file).expanduser().resolve()
                if args.lock_file
                else (Path(__file__).resolve().parent / DEFAULT_LOCK_FILE).resolve()
            )
            lock_handle = acquire_lock(lock_target)
            logger.info("LOCK | Activado en %s", lock_target)
        except Exception as exc:
            logger.error("LOCK | %s", exc)
            return 1

    try:
        base_path = resolve_base_path(args.ruta_base)

        try:
            all_detections = scan_detection_directories(base_path, logger)
        except Exception as exc:
            logger.error("SCAN | No se pudo escanear la ruta base: %s", exc)
            return 1

        logger.info("SCAN | Directorios detectados en disco: %s", len(all_detections))
        if not all_detections:
            logger.info("SCAN | No hay directorios para procesar")
            return 0

        last_dt = None
        try:
            conn = connect_mysql(logger)
            try:
                if args.mode == "pending":
                    last_dt = get_last_processed_datetime(conn, logger)
            finally:
                if conn.is_connected():
                    conn.close()
        except Exception as exc:
            logger.error("BD | Error: %s", exc)
            return 1

        selected = all_detections if args.mode == "all" else filter_pending(all_detections, last_dt)

        if args.limit and args.limit > 0:
            selected = selected[: args.limit]

        if not selected:
            logger.info("SELECCION | No hay directorios seleccionados para procesar")
            return 0

        print_run_header(
            logger=logger,
            base_path=base_path,
            mode=args.mode,
            dry_run=args.dry_run,
            timeout_seconds=args.timeout,
            log_path=log_path,
            selected=selected,
        )

        summary = process_detections(
            detections=selected,
            timeout_seconds=args.timeout,
            dry_run=args.dry_run,
            logger=logger,
        )
        print_summary(logger, summary)

        total_errors = summary["z"]["error"] + summary["rad"]["error"] + summary["fot"]["error"]
        total_timeouts = summary["z"]["timeout"] + summary["rad"]["timeout"] + summary["fot"]["timeout"]
        if total_errors > 0 or total_timeouts > 0:
            return 1
        return 0
    finally:
        if lock_handle is not None:
            lock_handle.close()


if __name__ == "__main__":
    sys.exit(main())
