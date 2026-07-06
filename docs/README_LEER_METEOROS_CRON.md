# leer_meteoros_cron.py

Version orientada a cron del procesado de meteoros.

## Que hace

- Ejecuta el procesado sin pedir confirmacion.
- Por defecto procesa solo pendientes (`--mode pending`).
- Genera un log automatico en `logs/` con fecha completa de ejecucion.
- Usa un lock para evitar que dos cron solapados se pisen.
- Deja una traza mas clara para operacion diaria.

## Uso manual

```bash
python3 leer_meteoros_cron.py
```

El log se genera automaticamente en una ruta tipo:

`logs/leer_meteoros_cron_YYYYMMDD_HHMMSS.log`

## Modos utiles

### Solo pendientes

```bash
python3 leer_meteoros_cron.py --mode pending
```

### Procesar todo

```bash
python3 leer_meteoros_cron.py --mode all
```

### Simulacion

```bash
python3 leer_meteoros_cron.py --dry-run --verbose
```

## Cron recomendado

Cada 10 minutos:

```cron
*/10 * * * * cd /home/in4p/uma/SMA_SCRIPTS && /usr/bin/python3 leer_meteoros_cron.py --mode pending >> /dev/null 2>&1
```

Cada 5 minutos:

```cron
*/5 * * * * cd /home/in4p/uma/SMA_SCRIPTS && /usr/bin/python3 leer_meteoros_cron.py --mode pending >> /dev/null 2>&1
```

## Opciones utiles

- `--timeout S`: timeout por cargador.
- `--limit N`: limita directorios.
- `--verbose`: mas detalle en consola y log.
- `--log-file RUTA`: fija un log concreto.
- `--log-dir RUTA`: cambia el directorio de logs.
- `--no-lock`: desactiva el lock de ejecucion.
