#!/usr/bin/env python3
"""
Script AUTOMATIZADO para procesar TODOS los meteoros pendientes.
Diseñado para ser ejecutado desde cron sin interacción del usuario.
Procesa automáticamente todos los directorios pendientes.
"""

import mysql.connector
from mysql.connector import Error
from datetime import datetime, time
import os
from pathlib import Path
import subprocess
import sys
import logging
from datetime import datetime as dt
import time as time_module  # Para los delays de reintentos

# Importar configuración centralizada
try:
    from config_db import DB_CONFIG, CONNECTION_CONFIG, TABLES, validate_config, get_connection_string
except ImportError:
    print("❌ Error: No se pudo importar config_db.py")
    print("🔧 Asegúrate de que el archivo config_db.py existe en el directorio actual")
    sys.exit(1)

# Configurar logging para cron
log_dir = Path(__file__).parent / "logs"
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"leer_meteoros_todos_{dt.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def conectar_mysql():
    """
    Establece conexión con la base de datos MySQL usando configuración centralizada
    """
    # Validar configuración
    config_valida, mensaje = validate_config()
    if not config_valida:
        logger.error(f"Error en configuración: {mensaje}")
        return None
    
    # Obtener configuración de reintentos
    max_intentos = CONNECTION_CONFIG.get('retry_attempts', 3)
    delay_reintentos = CONNECTION_CONFIG.get('retry_delay', 5)
    
    for intento in range(1, max_intentos + 1):
        try:
            logger.info(f"Intento {intento}/{max_intentos} - Conectando a {get_connection_string()}")
            
            # Usar configuración centralizada
            conexion = mysql.connector.connect(**DB_CONFIG)
            
            if conexion.is_connected():
                logger.info("Conexión exitosa a MySQL")
                db_info = conexion.get_server_info()
                logger.info(f"Versión del servidor MySQL: {db_info}")
                return conexion
                
        except Error as e:
            logger.error(f"Error en intento {intento}: {e}")
            
            if intento < max_intentos:
                logger.info(f"Esperando {delay_reintentos} segundos antes de reintentar...")
                time_module.sleep(delay_reintentos)
            else:
                logger.error(f"No se pudo conectar después de {max_intentos} intentos")
    
    return None

def obtener_ultima_fecha_hora(conexion):
    """
    Obtiene la última fecha y hora de la tabla meteoros
    """
    try:
        cursor = conexion.cursor()
        
        # Usar nombre de tabla desde configuración
        tabla_meteoro = TABLES.get('meteoro', 'Meteoro')
        
        query = f"""
        SELECT Fecha, Hora 
        FROM {tabla_meteoro} 
        ORDER BY fecha DESC, hora DESC 
        LIMIT 1
        """
        
        cursor.execute(query)
        resultado = cursor.fetchone()
        
        if resultado:
            return resultado[0], resultado[1]
        else:
            logger.warning("No se encontraron registros en la tabla meteoros")
            return None, None
            
    except Error as e:
        logger.error(f"Error al ejecutar consulta: {e}")
        return None, None
    finally:
        if cursor:
            cursor.close()

def separar_fecha(fecha):
    """
    Separa la fecha en mes, día y año
    """
    if fecha:
        if hasattr(fecha, 'month'):
            mes = fecha.month
            dia = fecha.day
            año = fecha.year
        else:
            fecha_str = str(fecha)
            partes = fecha_str.split('-')
            if len(partes) == 3:
                año = int(partes[0])
                mes = int(partes[1])
                dia = int(partes[2])
            else:
                logger.error(f"Formato de fecha no reconocido: {fecha_str}")
                return None, None, None
        
        return mes, dia, año
    return None, None, None

def obtener_directorios_pendientes(año_ultimo, mes_ultimo, dia_ultimo, hora_completa):
    """
    Obtiene la lista de directorios pendientes de procesar
    """
    script_dir = Path(__file__).parent
    ruta_base = script_dir / "Carpeta-meteoro-procesado/home/sma/Meteoros/Detecciones"
    
    directorios_pendientes = []
    info_global = {
        'total_directorios': 0,
        'procesados': 0,
        'pendientes': 0,
        'ultima_fecha_procesada': f"{año_ultimo:04d}{mes_ultimo:02d}{dia_ultimo:02d}",
        'ultima_hora_procesada': "000000"
    }
    
    # Convertir la hora de la última inserción a formato comparable
    if hora_completa:
        if isinstance(hora_completa, str):
            partes_hora = hora_completa.split('.')[0].split(':')
            if len(partes_hora) >= 3:
                hora_proc = int(partes_hora[0])
                min_proc = int(partes_hora[1])
                seg_proc = int(partes_hora[2])
                hora_procesada_str = f"{hora_proc:02d}{min_proc:02d}{seg_proc:02d}"
                info_global['ultima_hora_procesada'] = hora_procesada_str
    
    try:
        años_disponibles = sorted([d for d in ruta_base.iterdir() if d.is_dir() and d.name.isdigit()])
        
        for ruta_año in años_disponibles:
            año_actual = int(ruta_año.name)
            
            fechas_disponibles = sorted([d for d in ruta_año.iterdir() if d.is_dir() and len(d.name) == 8 and d.name.isdigit()])
            
            for ruta_fecha in fechas_disponibles:
                fecha_str = ruta_fecha.name
                fecha_num = int(fecha_str)
                fecha_limite = int(f"{año_ultimo:04d}{mes_ultimo:02d}{dia_ultimo:02d}")
                
                try:
                    directorios_hora = sorted([d for d in ruta_fecha.iterdir() if d.is_dir() and len(d.name) == 6 and d.name.isdigit()])
                    info_global['total_directorios'] += len(directorios_hora)
                    
                    if fecha_num > fecha_limite:
                        for directorio in directorios_hora:
                            directorios_pendientes.append({
                                'nombre': directorio.name,
                                'ruta': str(directorio),
                                'fecha': fecha_str,
                                'año': año_actual,
                                'fecha_formato': f"{fecha_str[:4]}-{fecha_str[4:6]}-{fecha_str[6:8]}",
                                'hora_formato': f"{directorio.name[:2]}:{directorio.name[2:4]}:{directorio.name[4:6]}"
                            })
                        
                    elif fecha_num == fecha_limite:
                        for directorio in directorios_hora:
                            if directorio.name > info_global['ultima_hora_procesada']:
                                directorios_pendientes.append({
                                    'nombre': directorio.name,
                                    'ruta': str(directorio),
                                    'fecha': fecha_str,
                                    'año': año_actual,
                                    'fecha_formato': f"{fecha_str[:4]}-{fecha_str[4:6]}-{fecha_str[6:8]}",
                                    'hora_formato': f"{directorio.name[:2]}:{directorio.name[2:4]}:{directorio.name[4:6]}"
                                })
                            else:
                                info_global['procesados'] += 1
                    else:
                        info_global['procesados'] += len(directorios_hora)
                        
                except Exception as e:
                    logger.warning(f"Error procesando fecha {fecha_str}: {e}")
                    continue
                    
    except Exception as e:
        logger.error(f"Error al analizar directorios: {e}")
        return [], {}
    
    info_global['pendientes'] = len(directorios_pendientes)
    
    return directorios_pendientes, info_global

def procesar_todos_los_informes(directorio_path):
    """
    Procesa secuencialmente todos los tipos de informes en el directorio
    """
    resultados = {
        'informes_z': {'procesados': 0, 'errores': 0},
        'informes_rad': {'procesados': 0, 'errores': 0},
        'informes_fot': {'procesados': 0, 'errores': 0}
    }
    
    # Primero procesamos los Informes-Z
    z_proc, z_err = procesar_informes_z(directorio_path)
    resultados['informes_z']['procesados'] = z_proc
    resultados['informes_z']['errores'] = z_err
    
    # Luego procesamos los Informes-Radiante
    rad_proc, rad_err = procesar_informes_radiante(directorio_path)
    resultados['informes_rad']['procesados'] = rad_proc
    resultados['informes_rad']['errores'] = rad_err
    
    # Finalmente procesamos los Informes-fotometria
    fot_proc, fot_err = procesar_informes_fotometria(directorio_path)
    resultados['informes_fot']['procesados'] = fot_proc
    resultados['informes_fot']['errores'] = fot_err
    
    return resultados

def procesar_informes_z(directorio_path):
    """
    Procesa los archivos Informe-Z en los subdirectorios Trayectoria-*
    """
    procesados = 0
    errores = 0
    informes_encontrados = []
    
    try:
        ruta = Path(directorio_path)
        
        subdirectorios_trayectoria = [d for d in ruta.iterdir() 
                                     if d.is_dir() and d.name.startswith("Trayectoria-")]
        
        if not subdirectorios_trayectoria:
            return 0, 0
        
        for subdir_trayectoria in subdirectorios_trayectoria:
            for archivo in subdir_trayectoria.iterdir():
                if archivo.is_file() and archivo.name.startswith("Informe-Z") and not archivo.name.endswith(".kml"):
                    informes_encontrados.append(archivo)
        
        if not informes_encontrados:
            return 0, 0
        
        logger.debug(f"Encontrados {len(informes_encontrados)} archivo(s) Informe-Z en {directorio_path}")
        
        for informe_path in informes_encontrados:
            try:
                script_dir = Path(__file__).parent
                carga_script = script_dir / "CargaInformesZ.py"
                
                resultado = subprocess.run(
                    [sys.executable, str(carga_script), str(informe_path.parent)],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if resultado.returncode == 0:
                    procesados += 1
                else:
                    errores += 1
                    logger.debug(f"Error procesando {informe_path.name}: {resultado.stderr[:100] if resultado.stderr else ''}")
                    
            except subprocess.TimeoutExpired:
                errores += 1
                logger.warning(f"Timeout procesando {informe_path.name}")
            except Exception as e:
                errores += 1
                logger.error(f"Error inesperado procesando {informe_path.name}: {e}")
        
    except Exception as e:
        logger.error(f"Error al procesar directorio {directorio_path}: {e}")
        return 0, 1
    
    return procesados, errores

def procesar_informes_radiante(directorio_path):
    """
    Procesa los archivos Informe-Radiante que terminan en .inf
    """
    procesados = 0
    errores = 0
    informes_encontrados = []
    
    try:
        ruta = Path(directorio_path)
        
        for archivo in ruta.rglob("Informe-Radiante*.inf"):
            informes_encontrados.append(archivo)
        
        if not informes_encontrados:
            return 0, 0
        
        logger.debug(f"Encontrados {len(informes_encontrados)} archivo(s) Informe-Radiante en {directorio_path}")
        
        for informe_path in informes_encontrados:
            try:
                script_dir = Path(__file__).parent
                carga_script = script_dir / "CargaInformesRad.py"
                
                resultado = subprocess.run(
                    [sys.executable, str(carga_script), str(informe_path.parent)],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if resultado.returncode == 0:
                    procesados += 1
                else:
                    errores += 1
                    logger.debug(f"Error procesando {informe_path.name}: {resultado.stderr[:100] if resultado.stderr else ''}")
                    
            except subprocess.TimeoutExpired:
                errores += 1
                logger.warning(f"Timeout procesando {informe_path.name}")
            except Exception as e:
                errores += 1
                logger.error(f"Error inesperado procesando {informe_path.name}: {e}")
        
    except Exception as e:
        logger.error(f"Error al procesar directorio {directorio_path}: {e}")
        return 0, 1
    
    return procesados, errores

def procesar_informes_fotometria(directorio_path):
    """
    Procesa los archivos Informe-fotometria
    """
    procesados = 0
    errores = 0
    informes_encontrados = []
    
    try:
        ruta = Path(directorio_path)
        
        for archivo in ruta.rglob("Informe-fotometria*"):
            if archivo.is_file():
                informes_encontrados.append(archivo)
        
        if not informes_encontrados:
            return 0, 0
        
        logger.debug(f"Encontrados {len(informes_encontrados)} archivo(s) Informe-fotometria en {directorio_path}")
        
        for informe_path in informes_encontrados:
            try:
                script_dir = Path(__file__).parent
                carga_script = script_dir / "CargaInformesFot_MySQL.py"
                
                resultado = subprocess.run(
                    [sys.executable, str(carga_script), str(informe_path.parent)],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if resultado.returncode == 0:
                    procesados += 1
                else:
                    errores += 1
                    logger.debug(f"Error procesando {informe_path.name}: {resultado.stderr[:100] if resultado.stderr else ''}")
                    
            except subprocess.TimeoutExpired:
                errores += 1
                logger.warning(f"Timeout procesando {informe_path.name}")
            except Exception as e:
                errores += 1
                logger.error(f"Error inesperado procesando {informe_path.name}: {e}")
        
    except Exception as e:
        logger.error(f"Error al procesar directorio {directorio_path}: {e}")
        return 0, 1
    
    return procesados, errores

def main():
    """
    Función principal del script automatizado
    """
    logger.info("="*60)
    logger.info("INICIO DE PROCESAMIENTO AUTOMATIZADO DE METEOROS")
    logger.info(f"Fecha y hora de ejecución: {dt.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60)
    
    # Conectar a MySQL
    conexion = conectar_mysql()
    
    if not conexion:
        logger.error("No se pudo establecer conexión con MySQL")
        sys.exit(1)
    
    try:
        # Obtener última fecha y hora
        fecha, hora = obtener_ultima_fecha_hora(conexion)
        
        if fecha is None or hora is None:
            logger.warning("No se encontraron registros en la tabla Meteoro")
            sys.exit(0)
        
        logger.info(f"Último registro encontrado: {fecha} {hora}")
        
        # Separar fecha
        mes, dia, año = separar_fecha(fecha)
        
        if año is None or mes is None or dia is None:
            logger.error("Error al procesar fecha del último registro")
            sys.exit(1)
        
        # Obtener directorios pendientes
        directorios_pendientes, info = obtener_directorios_pendientes(año, mes, dia, hora)
        
        logger.info(f"Análisis de directorios completado:")
        logger.info(f"  - Total de directorios: {info['total_directorios']}")
        logger.info(f"  - Directorios procesados: {info['procesados']}")
        logger.info(f"  - Directorios pendientes: {info['pendientes']}")
        
        if not directorios_pendientes:
            logger.info("No hay directorios pendientes de procesar")
            sys.exit(0)
        
        logger.info(f"Iniciando procesamiento de {len(directorios_pendientes)} directorio(s) pendiente(s)")
        
        # Contadores globales
        totales = {
            'informes_z': {'procesados': 0, 'errores': 0},
            'informes_rad': {'procesados': 0, 'errores': 0},
            'informes_fot': {'procesados': 0, 'errores': 0}
        }
        
        # Procesar todos los directorios pendientes
        for i, directorio in enumerate(directorios_pendientes, 1):
            logger.info(f"[{i}/{len(directorios_pendientes)}] Procesando: {directorio['fecha_formato']} {directorio['hora_formato']}")
            
            resultados = procesar_todos_los_informes(directorio['ruta'])
            
            # Acumular resultados
            for tipo in totales:
                totales[tipo]['procesados'] += resultados[tipo]['procesados']
                totales[tipo]['errores'] += resultados[tipo]['errores']
            
            # Log de progreso cada 10 directorios
            if i % 10 == 0:
                porcentaje = (i / len(directorios_pendientes)) * 100
                logger.info(f"Progreso: {porcentaje:.1f}%")
        
        # Resumen final
        logger.info("="*60)
        logger.info("RESUMEN DEL PROCESAMIENTO")
        logger.info("="*60)
        
        total_procesados = sum(t['procesados'] for t in totales.values())
        total_errores = sum(t['errores'] for t in totales.values())
        
        logger.info(f"Informes-Z: Procesados={totales['informes_z']['procesados']}, Errores={totales['informes_z']['errores']}")
        logger.info(f"Informes-Radiante: Procesados={totales['informes_rad']['procesados']}, Errores={totales['informes_rad']['errores']}")
        logger.info(f"Informes-fotometria: Procesados={totales['informes_fot']['procesados']}, Errores={totales['informes_fot']['errores']}")
        logger.info(f"TOTALES: Procesados={total_procesados}, Errores={total_errores}")
        logger.info(f"Directorios procesados: {len(directorios_pendientes)}")
        
        if total_errores > 0:
            logger.warning("Procesamiento completado con errores")
            sys.exit(1)
        else:
            logger.info("Procesamiento completado exitosamente")
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Error durante el procesamiento: {e}")
        sys.exit(1)
    finally:
        if conexion.is_connected():
            conexion.close()
            logger.info("Conexión a MySQL cerrada")

if __name__ == "__main__":
    main()