# SMA_SCRIPTS

Repositorio de ingesta y procesamiento de informes de meteoros contra MySQL.

## Estructura actual

Se dejó en raíz solo lo operativo del flujo principal y se agrupó el resto por contexto.

```text
SMA_SCRIPTS/
├── leer_meteoros_v2.py              # Orquestador principal (v2)
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

## Documentación relacionada

- `docs/README_LEER_METEOROS_V2.md`: detalle de opciones de la v2.
- `docs/README_CONFIGURACION_DB.md`: configuración centralizada de conexión.
- `docs/MIGRACION_MYSQL.md`: notas de migración y contexto histórico.
- `sql/database.sql`: esquema actual de base de datos.

## Nota sobre legacy

Los scripts dentro de `legacy/` se conservan por trazabilidad y compatibilidad histórica, pero no son el camino recomendado para nuevas ejecuciones. Para operación diaria usa `leer_meteoros_v2.py`.
