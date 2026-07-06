# leer_meteoros_v2.py

Versión 2 del proceso de lectura y carga de meteoros.

## Qué mejora

- Escaneo estructurado de directorios `YYYY/YYYYMMDD/HHMMSS`.
- Modo `pending` real: solo procesa carpetas posteriores al último registro de `Meteoro`.
- Ejecución ordenada de cargadores por tipo (`Z` -> `Radiante` -> `Fotometría`).
- Menos reprocesado innecesario (procesa por carpeta objetivo, no por archivo repetido).
- Resumen final claro y código de salida útil para cron (`0` OK, `1` con errores/timeout).

## Uso básico

Desde el directorio del proyecto:

```bash
python3 leer_meteoros_v2.py
```

Esto usa la ruta base por defecto:

`Carpeta-meteoro-procesado/home/sma/Detecciones`

## Modos principales

### Solo pendientes (por defecto)

```bash
python3 leer_meteoros_v2.py --mode pending
```

### Procesar todo

```bash
python3 leer_meteoros_v2.py --mode all
```

### Simulación (sin insertar)

```bash
python3 leer_meteoros_v2.py --dry-run --verbose
```

### Cron recomendado

```bash
python3 leer_meteoros_v2.py --cron --mode pending --log-file logs/leer_meteoros_v2.log
```

## Opciones útiles

- `ruta_base` (posicional): cambia la ruta base de detecciones.
- `--limit N`: limita número de directorios.
- `--timeout S`: timeout por ejecución de cargador (por defecto `30`).
- `--yes`: no pide confirmación.
- `--verbose`: logging detallado.

## Compatibilidad

La v2 reutiliza los scripts actuales de carga:

- `CargaInformesZ.py`
- `CargaInformesRad.py`
- `CargaInformesFot_MySQL.py`

No cambia el esquema de la base de datos.
