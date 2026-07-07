# SMA_SCRIPTS

Repositorio de ingesta y procesamiento de informes de meteoros contra MySQL.

## Estructura actual

Se dejó en raíz solo lo operativo del flujo principal y se agrupó el resto por contexto.

```text
SMA_SCRIPTS/
├── leer_meteoros_v2.py              # Orquestador principal (v2)
├── reprocesar_meteoros_desde_fecha.py  # Reproceso por rango de fechas
├── CargaInformesZ.py                # Carga de Informe-Z
├── CargaInformesRad.py              # Carga de Informe-Radiante
├── CargaInformesFot_MySQL.py        # Carga de Informe-fotometria
├── CargaInicial_MySQL.py            # Carga inicial de catálogos/base
├── ProcesadorInformes_MySQL.py      # Procesador unificado (MySQL)
├── eliminar_meteoro.py              # Utilidad de borrado por identificador
├── config_db.py                     # Configuración centralizada de BD
├── crontab_ejemplo.txt              # Ejemplos de cron para v2
├── logs/                            # Logs de ejecución
│
├── docs/                            # Documentación funcional y migración
│   ├── MIGRACION_MYSQL.md
│   ├── README_CONFIGURACION_DB.md
│   └── README_LEER_METEOROS_V2.md
│
├── sql/                             # DDL y esquema de referencia
│   ├── database.sql
│   └── db_ddl.sql
│
├── legacy/                          # Scripts antiguos (no recomendados)
│   ├── CargaInformesFot.py
│   ├── CargaInicial.py
│   ├── leer_meteoros.py
│   └── leer_meteoros_todos.py
│
├── examples/                        # Ejemplos de uso
│   └── ejemplo_uso_config_db.py
├── utils/                           # Utilidades auxiliares
│   └── formato_decimal.py
└── tests/                           # Pruebas
	└── test_formato.py
```

## Flujo recomendado (actual)

El flujo productivo recomendado es:

1. `leer_meteoros_v2.py` escanea detecciones.
2. Consulta el último registro en `Meteoro` (modo `pending`).
3. Ejecuta cargadores por tipo y carpeta objetivo:
   - `CargaInformesZ.py`
   - `CargaInformesRad.py`
   - `CargaInformesFot_MySQL.py`
4. Muestra resumen final y devuelve código de salida para cron.

## Uso rápido

### 1) Configurar BD

Edita `config_db.py` con tus credenciales y base de datos.

### 2) Ejecutar pendientes

```bash
python3 leer_meteoros_v2.py --mode pending
```

### 3) Simular sin insertar

```bash
python3 leer_meteoros_v2.py --dry-run --verbose
```

### 4) Ejecución para cron

```bash
python3 leer_meteoros_v2.py --cron --mode pending --log-file logs/leer_meteoros_v2.log
```

## Reprocesar por rango de fechas

El script `reprocesar_meteoros_desde_fecha.py` sirve para rehacer un intervalo cerrado de datos. Su funcionamiento es:

1. Localiza los directorios de eventos dentro del rango de fechas indicado.
2. Borra de MySQL los registros asociados a esos meteoros.
3. Vuelve a ejecutar los cargadores de `Z`, `Radiante` y `Fotometría` solo para esos directorios.

### Sintaxis

```bash
python3 reprocesar_meteoros_desde_fecha.py FECHA_INICIO FECHA_FIN [--sin-confirmacion] [--ruta-base RUTA] [--timeout SEGUNDOS]
```

### Formato de fechas

- `YYYYMMDD`
- `YYYY-MM-DD`

### Opciones útiles

- `--sin-confirmacion`: ejecuta el borrado y reproceso sin pedir confirmación interactiva.
- `--ruta-base`: fuerza una ruta base distinta para buscar las detecciones.
- `--timeout`: tiempo máximo por cada cargador, en segundos. Por defecto `30`.

### Ejemplo de uso

```bash
python3 reprocesar_meteoros_desde_fecha.py 2025-01-17 2025-01-18 --sin-confirmacion
```

Ese comando elimina y reconstruye todo lo que esté entre el 17 y el 18 de enero de 2025, ambos incluidos.

## Documentación relacionada

- `docs/README_LEER_METEOROS_V2.md`: detalle de opciones de la v2.
- `docs/README_CONFIGURACION_DB.md`: configuración centralizada de conexión.
- `docs/MIGRACION_MYSQL.md`: notas de migración y contexto histórico.
- `sql/database.sql`: esquema actual de base de datos.

## Nota sobre legacy

Los scripts dentro de `legacy/` se conservan por trazabilidad y compatibilidad histórica, pero no son el camino recomendado para nuevas ejecuciones. Para operación diaria usa `leer_meteoros_v2.py`.
