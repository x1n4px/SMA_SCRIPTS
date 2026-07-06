#!/usr/bin/env python3
"""
Reprocess meteor data for a closed date range.

This script:
1. Deletes database rows for meteors whose date is within the requested range.
2. Rebuilds only the event directories that fall inside that same range.

Usage:
    python3 reprocesar_meteoros_desde_fecha.py 20250117 20250118
    python3 reprocesar_meteoros_desde_fecha.py 2025-01-17 2025-01-18 --sin-confirmacion

Notes:
    - The end date must be strictly greater than the start date.
    - The range is inclusive on both ends.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, List, Optional

import mysql.connector
from mysql.connector import Error

try:
    from config_db import DB_CONFIG, CONNECTION_CONFIG, PATHS_CONFIG, TABLES, validate_config
except ImportError:
    print("❌ Error: No se pudo importar config_db.py")
    print("🔧 Asegúrate de que el archivo config_db.py existe en el directorio actual")
    sys.exit(1)


@dataclass(frozen=True)
class EventDir:
    path: Path
    dt: datetime


def normalize_date(value: str) -> date:
    raw = value.strip()
    if "-" in raw:
        raw = raw.replace("-", "")
    if len(raw) != 8 or not raw.isdigit():
        raise ValueError("La fecha debe tener formato YYYYMMDD o YYYY-MM-DD")
    return datetime.strptime(raw, "%Y%m%d").date()


def resolve_base_path(user_path: Optional[str]) -> Path:
    default_relative = Path(PATHS_CONFIG.get("meteor_detections_base", "Carpeta-meteoro-procesado/home/sma/Detecciones"))
    if user_path:
        return Path(user_path).expanduser().resolve()
    if default_relative.is_absolute():
        return default_relative.expanduser().resolve()
    return (Path(__file__).resolve().parent / default_relative).resolve()


def connect_mysql():
    ok, message = validate_config()
    if not ok:
        raise RuntimeError(f"Configuración de base de datos inválida: {message}")
    return mysql.connector.connect(**DB_CONFIG)


def scan_event_directories(base_path: Path, fecha_ini: date, fecha_fin: date) -> List[EventDir]:
    if not base_path.exists() or not base_path.is_dir():
        raise FileNotFoundError(f"Ruta base no válida: {base_path}")

    selected: List[EventDir] = []

    for year_dir in sorted(base_path.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit() or len(year_dir.name) != 4:
            continue
        for date_dir in sorted(year_dir.iterdir()):
            if not date_dir.is_dir() or not date_dir.name.isdigit() or len(date_dir.name) != 8:
                continue
            try:
                dir_date = datetime.strptime(date_dir.name, "%Y%m%d").date()
            except ValueError:
                continue
            if dir_date < fecha_ini or dir_date > fecha_fin:
                continue
            for time_dir in sorted(date_dir.iterdir()):
                if not time_dir.is_dir() or not time_dir.name.isdigit() or len(time_dir.name) != 6:
                    continue
                try:
                    dt = datetime.strptime(date_dir.name + time_dir.name, "%Y%m%d%H%M%S")
                except ValueError:
                    continue
                selected.append(EventDir(path=time_dir.resolve(), dt=dt))

    selected.sort(key=lambda item: item.dt)
    return selected


def show_selection_summary(event_dirs: List[EventDir], fecha_ini: date, fecha_fin: date) -> None:
    print("=" * 72)
    print("SELECCION DE DIRECTORIOS")
    print("=" * 72)
    print(f"Rango: {fecha_ini} -> {fecha_fin} (inclusive)")
    print(f"Directorios seleccionados: {len(event_dirs)}")
    if event_dirs:
        print(f"Primero: {event_dirs[0].path}")
        print(f"Ultimo:  {event_dirs[-1].path}")


def print_db_stats(cursor, fecha_ini: date, fecha_fin: date) -> None:
    tabla_meteoro = TABLES.get("meteoro", "Meteoro")
    tabla_radiante = TABLES.get("radiante", "Informe_Radiante")
    tabla_fotometria = TABLES.get("fotometria", "Informe_Fotometria")
    tabla_informe_z = TABLES.get("informe_z", "Informe_Z")

    queries = [
        ("Meteoro", f"SELECT COUNT(*) FROM {tabla_meteoro} WHERE Fecha BETWEEN %s AND %s"),
        ("Informe_Z", f"SELECT COUNT(*) FROM {tabla_informe_z} WHERE Fecha BETWEEN %s AND %s"),
        ("Informe_Radiante", f"SELECT COUNT(*) FROM {tabla_radiante} WHERE Fecha BETWEEN %s AND %s"),
        ("Informe_Fotometria", f"SELECT COUNT(*) FROM {tabla_fotometria} WHERE Fecha BETWEEN %s AND %s"),
    ]
    print("=" * 72)
    print("REGISTROS ACTUALES EN EL RANGO")
    print("=" * 72)
    for label, query in queries:
        cursor.execute(query, (fecha_ini, fecha_fin))
        count = cursor.fetchone()[0]
        print(f"{label}: {count}")


def delete_range(cursor, fecha_ini: date, fecha_fin: date) -> dict[str, int]:
    tabla_meteoro = TABLES.get("meteoro", "Meteoro")
    tabla_informe_z = TABLES.get("informe_z", "Informe_Z")
    tabla_radiante = TABLES.get("radiante", "Informe_Radiante")
    tabla_fotometria = TABLES.get("fotometria", "Informe_Fotometria")

    deletions: list[tuple[str, str, tuple[date, date]]] = [
        (
            "Elementos_Orbitales",
            f"""
            DELETE eo FROM Elementos_Orbitales eo
            INNER JOIN {tabla_informe_z} iz ON eo.Informe_Z_IdInforme = iz.IdInforme
            INNER JOIN {tabla_meteoro} m ON iz.Meteoro_Identificador = m.Identificador
            WHERE m.Fecha BETWEEN %s AND %s
            """,
            (fecha_ini, fecha_fin),
        ),
        (
            "Lluvia_activa",
            f"""
            DELETE la FROM Lluvia_activa la
            INNER JOIN {tabla_informe_z} iz ON la.Informe_Z_IdInforme = iz.IdInforme
            INNER JOIN {tabla_meteoro} m ON iz.Meteoro_Identificador = m.Identificador
            WHERE m.Fecha BETWEEN %s AND %s
            """,
            (fecha_ini, fecha_fin),
        ),
        (
            "Puntos_ZWO",
            f"""
            DELETE pz FROM Puntos_ZWO pz
            INNER JOIN {tabla_informe_z} iz ON pz.Informe_Z_IdInforme = iz.IdInforme
            INNER JOIN {tabla_meteoro} m ON iz.Meteoro_Identificador = m.Identificador
            WHERE m.Fecha BETWEEN %s AND %s
            """,
            (fecha_ini, fecha_fin),
        ),
        (
            "Trayectoria_medida",
            f"""
            DELETE tm FROM Trayectoria_medida tm
            INNER JOIN {tabla_informe_z} iz ON tm.Informe_Z_IdInforme = iz.IdInforme
            INNER JOIN {tabla_meteoro} m ON iz.Meteoro_Identificador = m.Identificador
            WHERE m.Fecha BETWEEN %s AND %s
            """,
            (fecha_ini, fecha_fin),
        ),
        (
            "Trayectoria_por_regresion",
            f"""
            DELETE tpr FROM Trayectoria_por_regresion tpr
            INNER JOIN {tabla_informe_z} iz ON tpr.Informe_Z_IdInforme = iz.IdInforme
            INNER JOIN {tabla_meteoro} m ON iz.Meteoro_Identificador = m.Identificador
            WHERE m.Fecha BETWEEN %s AND %s
            """,
            (fecha_ini, fecha_fin),
        ),
        (
            "Datos_meteoro_fotometria",
            f"""
            DELETE dmf FROM Datos_meteoro_fotometria dmf
            INNER JOIN {tabla_fotometria} ifot ON dmf.Informe_Fotometria_Identificador = ifot.Identificador
            INNER JOIN {tabla_meteoro} m ON ifot.Meteoro_Identificador = m.Identificador
            WHERE m.Fecha BETWEEN %s AND %s
            """,
            (fecha_ini, fecha_fin),
        ),
        (
            "Estrellas_usadas_para_regresión",
            f"""
            DELETE eur FROM Estrellas_usadas_para_regresión eur
            INNER JOIN {tabla_fotometria} ifot ON eur.Informe_Fotometria_Identificador = ifot.Identificador
            INNER JOIN {tabla_meteoro} m ON ifot.Meteoro_Identificador = m.Identificador
            WHERE m.Fecha BETWEEN %s AND %s
            """,
            (fecha_ini, fecha_fin),
        ),
        (
            "Puntos_del_ajuste",
            f"""
            DELETE pa FROM Puntos_del_ajuste pa
            INNER JOIN {tabla_fotometria} ifot ON pa.Informe_Fotometria_Identificador = ifot.Identificador
            INNER JOIN {tabla_meteoro} m ON ifot.Meteoro_Identificador = m.Identificador
            WHERE m.Fecha BETWEEN %s AND %s
            """,
            (fecha_ini, fecha_fin),
        ),
        (
            "Lluvia_Activa_InfRad",
            f"""
            DELETE lair FROM Lluvia_Activa_InfRad lair
            INNER JOIN {tabla_radiante} ir ON lair.Informe_Radiante_Identificador = ir.Identificador
            INNER JOIN {tabla_meteoro} m ON ir.Meteoro_Identificador = m.Identificador
            WHERE m.Fecha BETWEEN %s AND %s
            """,
            (fecha_ini, fecha_fin),
        ),
        (
            "Trayectoria_estimada",
            f"""
            DELETE te FROM Trayectoria_estimada te
            INNER JOIN {tabla_radiante} ir ON te.Informe_Radiante_Identificador = ir.Identificador
            INNER JOIN {tabla_meteoro} m ON ir.Meteoro_Identificador = m.Identificador
            WHERE m.Fecha BETWEEN %s AND %s
            """,
            (fecha_ini, fecha_fin),
        ),
        (
            "Velociades_Angulares",
            f"""
            DELETE va FROM Velociades_Angulares va
            INNER JOIN {tabla_radiante} ir ON va.Informe_Radiante_Identificador = ir.Identificador
            INNER JOIN {tabla_meteoro} m ON ir.Meteoro_Identificador = m.Identificador
            WHERE m.Fecha BETWEEN %s AND %s
            """,
            (fecha_ini, fecha_fin),
        ),
        (
            "Informe_Z",
            f"""
            DELETE iz FROM {tabla_informe_z} iz
            INNER JOIN {tabla_meteoro} m ON iz.Meteoro_Identificador = m.Identificador
            WHERE m.Fecha BETWEEN %s AND %s
            """,
            (fecha_ini, fecha_fin),
        ),
        (
            "Informe_Fotometria",
            f"""
            DELETE ifot FROM {tabla_fotometria} ifot
            INNER JOIN {tabla_meteoro} m ON ifot.Meteoro_Identificador = m.Identificador
            WHERE m.Fecha BETWEEN %s AND %s
            """,
            (fecha_ini, fecha_fin),
        ),
        (
            "Informe_Radiante",
            f"""
            DELETE ir FROM {tabla_radiante} ir
            INNER JOIN {tabla_meteoro} m ON ir.Meteoro_Identificador = m.Identificador
            WHERE m.Fecha BETWEEN %s AND %s
            """,
            (fecha_ini, fecha_fin),
        ),
        (
            "Meteoro",
            f"""
            DELETE FROM {tabla_meteoro}
            WHERE Fecha BETWEEN %s AND %s
            """,
            (fecha_ini, fecha_fin),
        ),
    ]

    counts: dict[str, int] = {}
    for label, query, params in deletions:
        cursor.execute(query, params)
        counts[label] = cursor.rowcount
        print(f"   ✅ {label}: {counts[label]}")
    return counts


def run_loader(script_path: Path, target_dir: Path, timeout_seconds: int) -> int:
    cmd = [sys.executable, str(script_path), str(target_dir)]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_seconds, check=False)
    if result.returncode == 0:
        return 0

    message = (result.stderr or result.stdout or "").strip()
    print(f"   ❌ {script_path.name} -> {target_dir.name} | code={result.returncode} | {message[:500]}")
    return result.returncode


def rebuild_range(event_dirs: List[EventDir], timeout_seconds: int) -> int:
    repo_root = Path(__file__).resolve().parent
    loaders = [
        repo_root / "CargaInformesZ.py",
        repo_root / "CargaInformesRad.py",
        repo_root / "CargaInformesFot_MySQL.py",
    ]

    print("=" * 72)
    print("RECONSTRUCCION")
    print("=" * 72)

    failures = 0
    for index, event_dir in enumerate(event_dirs, start=1):
        print(f"[{index}/{len(event_dirs)}] {event_dir.path}")
        for loader in loaders:
            rc = run_loader(loader, event_dir.path, timeout_seconds)
            if rc != 0:
                failures += 1
    return 1 if failures else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Elimina registros en un rango cerrado de fechas y los vuelve a generar"
    )
    parser.add_argument("fecha_inicio", help="Fecha inicial (YYYYMMDD o YYYY-MM-DD)")
    parser.add_argument("fecha_fin", help="Fecha final (YYYYMMDD o YYYY-MM-DD)")
    parser.add_argument(
        "--sin-confirmacion",
        action="store_true",
        help="Ejecutar sin pedir confirmacion",
    )
    parser.add_argument(
        "--ruta-base",
        help="Ruta base de detecciones para buscar los directorios",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout por ejecucion de cada cargador en segundos",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    try:
        fecha_ini = normalize_date(args.fecha_inicio)
        fecha_fin = normalize_date(args.fecha_fin)
    except ValueError as exc:
        print(f"❌ {exc}")
        return 1

    if fecha_fin < fecha_ini:
        print("❌ La fecha fin debe ser mayor o igual que la fecha inicio")
        return 1

    base_path = resolve_base_path(args.ruta_base)
    event_dirs = scan_event_directories(base_path, fecha_ini, fecha_fin)

    show_selection_summary(event_dirs, fecha_ini, fecha_fin)
    if not event_dirs:
        print("✅ No hay directorios dentro del rango indicado")
        return 0

    if not args.sin_confirmacion:
        respuesta = input("¿Continuar con el borrado y reprocesado? (escribe SI): ").strip().upper()
        if respuesta != "SI":
            print("❌ Operacion cancelada por el usuario")
            return 0

    try:
        conn = connect_mysql()
    except (Error, RuntimeError) as exc:
        print(f"❌ Error de base de datos: {exc}")
        return 1

    try:
        cursor = conn.cursor()
        print_db_stats(cursor, fecha_ini, fecha_fin)
        print("=" * 72)
        print("BORRADO")
        print("=" * 72)
        delete_range(cursor, fecha_ini, fecha_fin)
        conn.commit()
    except Error as exc:
        conn.rollback()
        print(f"❌ Error al borrar registros: {exc}")
        return 1
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            if conn.is_connected():
                conn.close()
        except Exception:
            pass

    rebuild_rc = rebuild_range(event_dirs, args.timeout)
    if rebuild_rc != 0:
        print("⚠️  El borrado se completó, pero hubo errores al reconstruir")
        return rebuild_rc

    print("✅ Reproceso completado correctamente")
    return 0


if __name__ == "__main__":
    sys.exit(main())
