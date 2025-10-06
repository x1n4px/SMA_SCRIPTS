#!/usr/bin/env python3
"""
Script para conectar a MySQL, leer la tabla meteoros,
obtener la última fecha y hora, mostrarlas separadas,
y verificar directorios pendientes de procesar.
"""

import mysql.connector
from mysql.connector import Error
from datetime import datetime, time
import os
from pathlib import Path
import subprocess
import sys
import argparse
import time as time_module  # Para los delays de reintentos

# Importar configuración de base de datos
try:
    from config_db import DB_CONFIG, CONNECTION_CONFIG, TABLES, validate_config, get_connection_string
except ImportError:
    print("❌ Error: No se pudo importar config_db.py")
    print("🔧 Asegúrate de que el archivo config_db.py existe en el directorio actual")
    sys.exit(1)

def conectar_mysql():
    """
    Establece conexión con la base de datos MySQL usando configuración centralizada
    """
    # Validar configuración antes de intentar conectar
    config_valida, mensaje = validate_config()
    if not config_valida:
        print(f"❌ Error en configuración: {mensaje}")
        return None
    
    # Obtener configuración de reintentos
    max_intentos = CONNECTION_CONFIG.get('retry_attempts', 3)
    delay_reintentos = CONNECTION_CONFIG.get('retry_delay', 5)
    
    for intento in range(1, max_intentos + 1):
        try:
            print(f"🔄 Intento {intento}/{max_intentos} - Conectando a {get_connection_string()}")
            
            # Usar configuración centralizada
            conexion = mysql.connector.connect(**DB_CONFIG)
            
            if conexion.is_connected():
                print("✅ Conexión exitosa a MySQL")
                db_info = conexion.get_server_info()
                print(f"📊 Versión del servidor MySQL: {db_info}")
                return conexion
                
        except Error as e:
            print(f"❌ Error en intento {intento}: {e}")
            
            if intento < max_intentos:
                print(f"⏳ Esperando {delay_reintentos} segundos antes de reintentar...")
                time_module.sleep(delay_reintentos)
            else:
                print(f"❌ No se pudo conectar después de {max_intentos} intentos")
    
    return None

def obtener_ultima_fecha_hora(conexion):
    """
    Obtiene la última fecha y hora de la tabla meteoros
    """
    try:
        cursor = conexion.cursor()
        
        # Usar nombre de tabla desde configuración
        tabla_meteoro = TABLES.get('meteoro', 'Meteoro')
        
        # Query para obtener la última fecha y hora
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
            print("No se encontraron registros en la tabla meteoros")
            return None, None
            
    except Error as e:
        print(f"Error al ejecutar consulta: {e}")
        return None, None
    finally:
        if cursor:
            cursor.close()

def separar_fecha(fecha):
    """
    Separa la fecha en mes, día y año
    """
    if fecha:
        # Si fecha es un objeto datetime.date
        if hasattr(fecha, 'month'):
            mes = fecha.month
            dia = fecha.day
            año = fecha.year
        # Si fecha es una cadena de texto
        else:
            fecha_str = str(fecha)
            # Asumiendo formato YYYY-MM-DD
            partes = fecha_str.split('-')
            if len(partes) == 3:
                año = int(partes[0])
                mes = int(partes[1])
                dia = int(partes[2])
            else:
                print(f"Formato de fecha no reconocido: {fecha_str}")
                return None, None, None
        
        return mes, dia, año
    return None, None, None

def separar_hora(hora):
    """
    Separa la hora en segundos, minutos y horas
    Maneja formato HH:MM:SS.fraccion (ej: 05:18:48.0364)
    """
    if hora:
        # Si hora es un objeto timedelta o time
        if hasattr(hora, 'seconds'):
            # Si es timedelta
            total_segundos = int(hora.total_seconds())
            horas = total_segundos // 3600
            minutos = (total_segundos % 3600) // 60
            segundos = total_segundos % 60
            milisegundos = 0
        elif hasattr(hora, 'hour'):
            # Si es time
            horas = hora.hour
            minutos = hora.minute
            segundos = hora.second
            milisegundos = hora.microsecond // 1000 if hasattr(hora, 'microsecond') else 0
        else:
            # Si es una cadena de texto
            hora_str = str(hora)
            # Verificar si tiene fracciones de segundo
            if '.' in hora_str:
                # Separar parte de tiempo y fracción
                parte_tiempo, fraccion = hora_str.split('.')
                # Obtener milisegundos de la fracción
                milisegundos = int(fraccion[:3].ljust(3, '0'))  # Tomar los primeros 3 dígitos
            else:
                parte_tiempo = hora_str
                milisegundos = 0
            
            # Asumiendo formato HH:MM:SS
            partes = parte_tiempo.split(':')
            if len(partes) == 3:
                horas = int(partes[0])
                minutos = int(partes[1])
                segundos = int(float(partes[2]))  # float por si queda algo decimal
            elif len(partes) == 2:
                # Formato HH:MM
                horas = int(partes[0])
                minutos = int(partes[1])
                segundos = 0
            else:
                print(f"Formato de hora no reconocido: {hora_str}")
                return None, None, None, None
        
        return segundos, minutos, horas, milisegundos
    return None, None, None, None

def obtener_directorios_pendientes(año_ultimo, mes_ultimo, dia_ultimo, hora_completa, ruta_base=None):
    """
    Obtiene la lista de directorios pendientes de procesar desde la última fecha/hora procesada
    Retorna una tupla: (directorios_pendientes, info_procesamiento)
    
    Args:
        año_ultimo: Año de la última fecha procesada
        mes_ultimo: Mes de la última fecha procesada
        dia_ultimo: Día de la última fecha procesada
        hora_completa: Hora completa de la última procesada
        ruta_base: Ruta base para buscar directorios (opcional)
    """
    # Construir ruta base
    if ruta_base is None:
        script_dir = Path(__file__).parent
        ruta_base = script_dir / "Carpeta-meteoro-procesado/home/sma/Meteoros/Detecciones"
    else:
        ruta_base = Path(ruta_base)
    
    directorios_pendientes = []
    info_global = {
        'total_directorios': 0,
        'procesados': 0,
        'pendientes': 0,
        'fechas_analizadas': [],
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
    
    # Buscar en todos los años disponibles
    try:
        años_disponibles = sorted([d for d in ruta_base.iterdir() if d.is_dir() and d.name.isdigit()])
        
        for ruta_año in años_disponibles:
            año_actual = int(ruta_año.name)
            
            # Obtener todas las fechas disponibles en este año
            fechas_disponibles = sorted([d for d in ruta_año.iterdir() if d.is_dir() and len(d.name) == 8 and d.name.isdigit()])
            
            for ruta_fecha in fechas_disponibles:
                fecha_str = ruta_fecha.name
                fecha_num = int(fecha_str)
                fecha_limite = int(f"{año_ultimo:04d}{mes_ultimo:02d}{dia_ultimo:02d}")
                
                # Obtener directorios de hora en esta fecha
                try:
                    directorios_hora = sorted([d for d in ruta_fecha.iterdir() if d.is_dir() and len(d.name) == 6 and d.name.isdigit()])
                    info_global['total_directorios'] += len(directorios_hora)
                    
                    if fecha_num > fecha_limite:
                        # Fecha posterior a la última procesada - todos los directorios son pendientes
                        for directorio in directorios_hora:
                            directorios_pendientes.append({
                                'nombre': directorio.name,
                                'ruta': str(directorio),
                                'fecha': fecha_str,
                                'año': año_actual,
                                'fecha_formato': f"{fecha_str[:4]}-{fecha_str[4:6]}-{fecha_str[6:8]}",
                                'hora_formato': f"{directorio.name[:2]}:{directorio.name[2:4]}:{directorio.name[4:6]}"
                            })
                        info_global['fechas_analizadas'].append(f"{fecha_str} ({len(directorios_hora)} dirs - TODOS PENDIENTES)")
                        
                    elif fecha_num == fecha_limite:
                        # Misma fecha - solo los directorios con hora posterior son pendientes
                        dirs_pendientes_fecha = 0
                        dirs_procesados_fecha = 0
                        
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
                                dirs_pendientes_fecha += 1
                            else:
                                dirs_procesados_fecha += 1
                        
                        info_global['procesados'] += dirs_procesados_fecha
                        info_global['fechas_analizadas'].append(f"{fecha_str} ({dirs_procesados_fecha} procesados, {dirs_pendientes_fecha} pendientes)")
                        
                    else:
                        # Fecha anterior - todos procesados
                        info_global['procesados'] += len(directorios_hora)
                        info_global['fechas_analizadas'].append(f"{fecha_str} ({len(directorios_hora)} dirs - TODOS PROCESADOS)")
                        
                except Exception as e:
                    print(f"⚠️  Error procesando fecha {fecha_str}: {e}")
                    continue
                    
    except Exception as e:
        print(f"\n❌ Error al analizar directorios: {e}")
        return [], {}
    
    info_global['pendientes'] = len(directorios_pendientes)
    
    return directorios_pendientes, info_global

def mostrar_directorios_pendientes(directorios_pendientes, info):
    """
    Muestra la información de los directorios pendientes
    """
    print("\n" + "=" * 60)
    print("ANÁLISIS COMPLETO DE DIRECTORIOS")
    print("=" * 60)
    print(f"📊 Total de directorios analizados: {info['total_directorios']}")
    print(f"✅ Directorios ya procesados: {info['procesados']}")
    print(f"⏳ Directorios pendientes de procesar: {info['pendientes']}")
    
    # Mostrar información de la última fecha/hora procesada
    ultima_fecha = info['ultima_fecha_procesada']
    ultima_hora = info['ultima_hora_procesada']
    fecha_formato = f"{ultima_fecha[:4]}-{ultima_fecha[4:6]}-{ultima_fecha[6:8]}"
    hora_formato = f"{ultima_hora[:2]}:{ultima_hora[2:4]}:{ultima_hora[4:6]}"
    print(f"📋 Última fecha/hora procesada: {fecha_formato} {hora_formato}")
    
    # Mostrar resumen de fechas analizadas
    if info['fechas_analizadas']:
        print(f"\n📅 Resumen de fechas analizadas:")
        for fecha_info in info['fechas_analizadas'][-10:]:  # Mostrar últimas 10
            print(f"   • {fecha_info}")
        if len(info['fechas_analizadas']) > 10:
            print(f"   ... y {len(info['fechas_analizadas']) - 10} fechas más")
    
    if directorios_pendientes:
        print(f"\n📂 DIRECTORIOS PENDIENTES DE PROCESAR:")
        print("-" * 60)
        
        # Agrupar por fecha para mejor visualización
        fechas_agrupadas = {}
        for directorio in directorios_pendientes:
            fecha = directorio['fecha']
            if fecha not in fechas_agrupadas:
                fechas_agrupadas[fecha] = []
            fechas_agrupadas[fecha].append(directorio)
        
        contador = 1
        for fecha in sorted(fechas_agrupadas.keys()):
            directorios_fecha = fechas_agrupadas[fecha]
            fecha_formato = f"{fecha[:4]}-{fecha[4:6]}-{fecha[6:8]}"
            print(f"\n   📅 {fecha_formato} ({len(directorios_fecha)} directorios):")
            
            for directorio in directorios_fecha:
                print(f"      {contador:3}. {directorio['nombre']} ({directorio['hora_formato']})")
                contador += 1
        
        return True
    else:
        print("\n✅ ¡Todos los directorios han sido procesados!")
        return False

def mostrar_menu():
    """
    Muestra el menú de opciones al usuario
    """
    print("\n" + "=" * 50)
    print("OPCIONES DE PROCESAMIENTO")
    print("=" * 50)
    print("1. Seleccionar manualmente directorios a procesar")
    print("2. Procesar todos los directorios pendientes")
    print("3. Salir")
    print("=" * 50)

def procesar_todos_los_informes(directorio_path):
    """
    Procesa secuencialmente todos los tipos de informes en el directorio:
    1. Primero Informe-Z (para crear la referencia del meteoro)
    2. Luego Informe-Radiante 
    3. Finalmente Informe-fotometria
    
    Args:
        directorio_path: Ruta al directorio a procesar
    
    Returns:
        Dict con contadores de cada tipo de informe procesado
    """
    resultados = {
        'informes_z': {'procesados': 0, 'errores': 0},
        'informes_rad': {'procesados': 0, 'errores': 0},
        'informes_fot': {'procesados': 0, 'errores': 0}
    }
    
    # Primero procesamos los Informes-Z
    print(f"\n   📄 Procesando Informes-Z...")
    z_proc, z_err = procesar_informes_z(directorio_path)
    resultados['informes_z']['procesados'] = z_proc
    resultados['informes_z']['errores'] = z_err
    
    # Luego procesamos los Informes-Radiante
    print(f"\n   🌟 Procesando Informes-Radiante...")
    rad_proc, rad_err = procesar_informes_radiante(directorio_path)
    resultados['informes_rad']['procesados'] = rad_proc
    resultados['informes_rad']['errores'] = rad_err
    
    # Finalmente procesamos los Informes-fotometria
    print(f"\n   📷 Procesando Informes-fotometria...")
    fot_proc, fot_err = procesar_informes_fotometria(directorio_path)
    resultados['informes_fot']['procesados'] = fot_proc
    resultados['informes_fot']['errores'] = fot_err
    
    return resultados

def procesar_informes_z(directorio_path):
    """
    Procesa los archivos Informe-Z en los subdirectorios Trayectoria-*
    
    Args:
        directorio_path: Ruta al directorio a procesar
    
    Returns:
        Tupla (procesados, errores) con contadores
    """
    procesados = 0
    errores = 0
    informes_encontrados = []
    
    try:
        ruta = Path(directorio_path)
        
        # Buscar todos los subdirectorios que empiecen con "Trayectoria-"
        subdirectorios_trayectoria = [d for d in ruta.iterdir() 
                                     if d.is_dir() and d.name.startswith("Trayectoria-")]
        
        if not subdirectorios_trayectoria:
            print(f"   ⚠️  No se encontraron subdirectorios 'Trayectoria-*' en {directorio_path}")
            return 0, 0
        
        for subdir_trayectoria in subdirectorios_trayectoria:
            # Buscar archivos que empiecen con "Informe-Z" y no terminen en ".kml"
            for archivo in subdir_trayectoria.iterdir():
                if archivo.is_file() and archivo.name.startswith("Informe-Z") and not archivo.name.endswith(".kml"):
                    informes_encontrados.append(archivo)
        
        if not informes_encontrados:
            print(f"   ⚠️  No se encontraron archivos Informe-Z válidos en {directorio_path}")
            return 0, 0
        
        print(f"   📄 Encontrados {len(informes_encontrados)} archivo(s) Informe-Z")
        
        # Procesar cada informe encontrado
        for informe_path in informes_encontrados:
            try:
                print(f"      • Procesando: {informe_path.parent.name}/{informe_path.name}")
                
                # Obtener la ruta del script CargaInformesZ.py
                script_dir = Path(__file__).parent
                carga_script = script_dir / "CargaInformesZ.py"
                
                # Ejecutar CargaInformesZ.py con el directorio padre del informe como argumento
                resultado = subprocess.run(
                    [sys.executable, str(carga_script), str(informe_path.parent)],
                    capture_output=True,
                    text=True,
                    timeout=30  # Timeout de 30 segundos por informe
                )
                
                if resultado.returncode == 0:
                    procesados += 1
                    print(f"        ✅ Procesado correctamente")
                    if resultado.stdout:
                        print(f"           Salida: {resultado.stdout[:100]}")
                else:
                    errores += 1
                    error_msg = resultado.stderr if resultado.stderr else resultado.stdout if resultado.stdout else 'Sin mensaje de error'
                    print(f"        ❌ Error al procesar (código {resultado.returncode}): {error_msg[:300]}")
                    
            except subprocess.TimeoutExpired:
                errores += 1
                print(f"        ❌ Tiempo de espera agotado procesando {informe_path.name}")
            except Exception as e:
                errores += 1
                print(f"        ❌ Error inesperado: {e}")
        
    except Exception as e:
        print(f"   ❌ Error al procesar directorio {directorio_path}: {e}")
        return 0, 1
    
    return procesados, errores

def procesar_informes_radiante(directorio_path):
    """
    Procesa los archivos Informe-Radiante que terminan en .inf
    
    Args:
        directorio_path: Ruta al directorio a procesar
    
    Returns:
        Tupla (procesados, errores) con contadores
    """
    procesados = 0
    errores = 0
    informes_encontrados = []
    
    try:
        ruta = Path(directorio_path)
        
        # Buscar archivos que empiecen con "Informe-Radiante" y terminen en ".inf"
        for archivo in ruta.rglob("Informe-Radiante*.inf"):
            informes_encontrados.append(archivo)
        
        if not informes_encontrados:
            print(f"      ⚠️  No se encontraron archivos Informe-Radiante en {directorio_path}")
            return 0, 0
        
        print(f"      🌟 Encontrados {len(informes_encontrados)} archivo(s) Informe-Radiante")
        
        # Procesar cada informe encontrado
        for informe_path in informes_encontrados:
            try:
                print(f"         • Procesando: {informe_path.name}")
                
                # Obtener la ruta del script CargaInformesRad.py
                script_dir = Path(__file__).parent
                carga_script = script_dir / "CargaInformesRad.py"
                
                # Ejecutar CargaInformesRad.py con el directorio del informe como argumento
                resultado = subprocess.run(
                    [sys.executable, str(carga_script), str(informe_path.parent)],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if resultado.returncode == 0:
                    procesados += 1
                    print(f"           ✅ Procesado correctamente")
                else:
                    errores += 1
                    error_msg = resultado.stderr if resultado.stderr else resultado.stdout if resultado.stdout else 'Sin mensaje de error'
                    print(f"           ❌ Error al procesar (código {resultado.returncode}): {error_msg[:200]}")
                    
            except subprocess.TimeoutExpired:
                errores += 1
                print(f"           ❌ Tiempo de espera agotado procesando {informe_path.name}")
            except Exception as e:
                errores += 1
                print(f"           ❌ Error inesperado: {e}")
        
    except Exception as e:
        print(f"      ❌ Error al procesar directorio {directorio_path}: {e}")
        return 0, 1
    
    return procesados, errores

def procesar_informes_fotometria(directorio_path):
    """
    Procesa los archivos Informe-fotometria
    
    Args:
        directorio_path: Ruta al directorio a procesar
    
    Returns:
        Tupla (procesados, errores) con contadores
    """
    procesados = 0
    errores = 0
    informes_encontrados = []
    
    try:
        ruta = Path(directorio_path)
        
        # Buscar archivos que empiecen con "Informe-fotometria"
        for archivo in ruta.rglob("Informe-fotometria*"):
            if archivo.is_file():
                informes_encontrados.append(archivo)
        
        if not informes_encontrados:
            print(f"      ⚠️  No se encontraron archivos Informe-fotometria en {directorio_path}")
            return 0, 0
        
        print(f"      📷 Encontrados {len(informes_encontrados)} archivo(s) Informe-fotometria")
        
        # Procesar cada informe encontrado
        for informe_path in informes_encontrados:
            try:
                print(f"         • Procesando: {informe_path.name}")
                
                # Obtener la ruta del script CargaInformesFot_MySQL.py
                script_dir = Path(__file__).parent
                carga_script = script_dir / "CargaInformesFot_MySQL.py"
                
                # Ejecutar CargaInformesFot_MySQL.py con el directorio del informe como argumento
                resultado = subprocess.run(
                    [sys.executable, str(carga_script), str(informe_path.parent)],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if resultado.returncode == 0:
                    procesados += 1
                    print(f"           ✅ Procesado correctamente")
                else:
                    errores += 1
                    error_msg = resultado.stderr if resultado.stderr else resultado.stdout if resultado.stdout else 'Sin mensaje de error'
                    print(f"           ❌ Error al procesar (código {resultado.returncode}): {error_msg[:200]}")
                    
            except subprocess.TimeoutExpired:
                errores += 1
                print(f"           ❌ Tiempo de espera agotado procesando {informe_path.name}")
            except Exception as e:
                errores += 1
                print(f"           ❌ Error inesperado: {e}")
        
    except Exception as e:
        print(f"      ❌ Error al procesar directorio {directorio_path}: {e}")
        return 0, 1
    
    return procesados, errores

def seleccion_manual(directorios_pendientes):
    """
    Permite al usuario seleccionar manualmente qué directorios procesar
    """
    print("\n📋 SELECCIÓN MANUAL DE DIRECTORIOS")
    print("-" * 50)
    print("Introduce los números de los directorios que deseas procesar.")
    print("Puedes usar:")
    print("  - Números individuales: 1,3,5")
    print("  - Rangos: 1-5")
    print("  - Combinación: 1,3,5-8,10")
    print("  - 'todos' para seleccionar todos")
    print()
    
    while True:
        try:
            seleccion = input("Tu selección: ").strip()
            
            if seleccion.lower() == 'todos':
                seleccionados = list(range(len(directorios_pendientes)))
                break
            
            # Procesar la selección
            seleccionados = set()
            partes = seleccion.split(',')
            
            for parte in partes:
                parte = parte.strip()
                if '-' in parte:
                    # Es un rango
                    inicio, fin = map(int, parte.split('-'))
                    if 1 <= inicio <= len(directorios_pendientes) and 1 <= fin <= len(directorios_pendientes):
                        seleccionados.update(range(inicio-1, fin))
                    else:
                        raise ValueError(f"Rango fuera de límites: {parte}")
                else:
                    # Es un número individual
                    num = int(parte)
                    if 1 <= num <= len(directorios_pendientes):
                        seleccionados.add(num-1)
                    else:
                        raise ValueError(f"Número fuera de rango: {num}")
            
            seleccionados = sorted(list(seleccionados))
            break
            
        except ValueError as e:
            print(f"❌ Error en la selección: {e}")
            print("Por favor, intenta de nuevo.")
    
    # Mostrar directorios seleccionados
    print(f"\n✅ Has seleccionado {len(seleccionados)} directorio(s):")
    directorios_a_procesar = []
    
    for i in seleccionados:
        directorio = directorios_pendientes[i]
        print(f"   • {directorio['nombre']} ({directorio['hora_formato']})")
        directorios_a_procesar.append(directorio)
    
    # Procesar los directorios seleccionados
    print(f"\n🔄 Procesando {len(directorios_a_procesar)} directorio(s)...")
    print("-" * 50)
    
    # Contadores globales
    totales = {
        'informes_z': {'procesados': 0, 'errores': 0},
        'informes_rad': {'procesados': 0, 'errores': 0},
        'informes_fot': {'procesados': 0, 'errores': 0}
    }
    
    for directorio in directorios_a_procesar:
        print(f"\n📂 Procesando directorio: {directorio['fecha_formato']} {directorio['hora_formato']}")
        print(f"   Ruta: {directorio['ruta']}")
        
        resultados = procesar_todos_los_informes(directorio['ruta'])
        
        # Acumular resultados
        for tipo in totales:
            totales[tipo]['procesados'] += resultados[tipo]['procesados']
            totales[tipo]['errores'] += resultados[tipo]['errores']
    
    # Mostrar resumen detallado
    print("\n" + "=" * 50)
    print("RESUMEN DEL PROCESAMIENTO")
    print("=" * 50)
    
    total_procesados = sum(t['procesados'] for t in totales.values())
    total_errores = sum(t['errores'] for t in totales.values())
    
    print(f"\n📄 Informes-Z:")
    print(f"   ✅ Procesados: {totales['informes_z']['procesados']}")
    if totales['informes_z']['errores'] > 0:
        print(f"   ❌ Con errores: {totales['informes_z']['errores']}")
    
    print(f"\n🌟 Informes-Radiante:")
    print(f"   ✅ Procesados: {totales['informes_rad']['procesados']}")
    if totales['informes_rad']['errores'] > 0:
        print(f"   ❌ Con errores: {totales['informes_rad']['errores']}")
    
    print(f"\n📷 Informes-fotometria:")
    print(f"   ✅ Procesados: {totales['informes_fot']['procesados']}")
    if totales['informes_fot']['errores'] > 0:
        print(f"   ❌ Con errores: {totales['informes_fot']['errores']}")
    
    print(f"\n📊 TOTALES:")
    print(f"   ✅ Total procesados: {total_procesados}")
    if total_errores > 0:
        print(f"   ❌ Total con errores: {total_errores}")
    print(f"   📂 Directorios procesados: {len(directorios_a_procesar)}")

def procesar_todos(directorios_pendientes):
    """
    Procesa todos los directorios pendientes
    """
    print(f"\n🔄 PROCESANDO TODOS LOS DIRECTORIOS")
    print("-" * 50)
    print(f"Total de directorios a procesar: {len(directorios_pendientes)}")
    
    # Confirmación antes de procesar todos
    print(f"\n⚠️  Esto procesará {len(directorios_pendientes)} directorio(s).")
    confirmacion = input("¿Deseas continuar? (s/n): ").strip().lower()
    
    if confirmacion != 's':
        print("\n❌ Procesamiento cancelado por el usuario.")
        return
    
    print("\n" + "=" * 50)
    
    # Contadores globales
    totales = {
        'informes_z': {'procesados': 0, 'errores': 0},
        'informes_rad': {'procesados': 0, 'errores': 0},
        'informes_fot': {'procesados': 0, 'errores': 0}
    }
    
    for i, directorio in enumerate(directorios_pendientes, 1):
        print(f"\n📂 [{i}/{len(directorios_pendientes)}] Procesando: {directorio['fecha_formato']} {directorio['hora_formato']}")
        print(f"   Ruta: {directorio['ruta']}")
        
        resultados = procesar_todos_los_informes(directorio['ruta'])
        
        # Acumular resultados
        for tipo in totales:
            totales[tipo]['procesados'] += resultados[tipo]['procesados']
            totales[tipo]['errores'] += resultados[tipo]['errores']
        
        # Mostrar progreso
        porcentaje = (i / len(directorios_pendientes)) * 100
        print(f"\n   Progreso total: {porcentaje:.1f}%")
    
    # Mostrar resumen detallado
    print("\n" + "=" * 50)
    print("RESUMEN DEL PROCESAMIENTO")
    print("=" * 50)
    
    total_procesados = sum(t['procesados'] for t in totales.values())
    total_errores = sum(t['errores'] for t in totales.values())
    
    print(f"\n📄 Informes-Z:")
    print(f"   ✅ Procesados: {totales['informes_z']['procesados']}")
    if totales['informes_z']['errores'] > 0:
        print(f"   ❌ Con errores: {totales['informes_z']['errores']}")
    
    print(f"\n🌟 Informes-Radiante:")
    print(f"   ✅ Procesados: {totales['informes_rad']['procesados']}")
    if totales['informes_rad']['errores'] > 0:
        print(f"   ❌ Con errores: {totales['informes_rad']['errores']}")
    
    print(f"\n📷 Informes-fotometria:")
    print(f"   ✅ Procesados: {totales['informes_fot']['procesados']}")
    if totales['informes_fot']['errores'] > 0:
        print(f"   ❌ Con errores: {totales['informes_fot']['errores']}")
    
    print(f"\n📊 TOTALES:")
    print(f"   ✅ Total procesados: {total_procesados}")
    if total_errores > 0:
        print(f"   ❌ Total con errores: {total_errores}")
    print(f"   📂 Directorios procesados: {len(directorios_pendientes)}")
    print(f"\n✅ Procesamiento completado.")

def main():
    """
    Función principal del script
    """
    # Configurar parser de argumentos
    parser = argparse.ArgumentParser(
        description='Script para procesar detecciones de meteoros desde la base de datos MySQL',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        'ruta_base',
        nargs='?',
        help='Ruta base para buscar los directorios de detecciones.\n'
             'Ejemplo: /Carpeta-meteoro-procesado/home/sma/Meteoros/Detecciones\n'
             'Si no se proporciona, se usa la ruta predeterminada relativa al script.'
    )
    
    args = parser.parse_args()
    
    print("=" * 50)
    print("SCRIPT DE LECTURA DE TABLA METEOROS")
    print("=" * 50)
    
    # Mostrar información de configuración
    print(f"\n🔧 Usando configuración desde: config_db.py")
    print(f"   Base de datos: {DB_CONFIG.get('database')}")
    print(f"   Servidor: {DB_CONFIG.get('host')}:{DB_CONFIG.get('port', 3306)}")
    print(f"   Usuario: {DB_CONFIG.get('user')}")
    
    # Mostrar la ruta que se va a usar
    if args.ruta_base:
        ruta_absoluta = Path(args.ruta_base).resolve()
        print(f"\n📂 Usando ruta base proporcionada:")
        print(f"   {ruta_absoluta}")
        
        # Verificar que la ruta existe
        if not ruta_absoluta.exists():
            print(f"\n❌ Error: La ruta proporcionada no existe: {ruta_absoluta}")
            sys.exit(1)
        if not ruta_absoluta.is_dir():
            print(f"\n❌ Error: La ruta proporcionada no es un directorio: {ruta_absoluta}")
            sys.exit(1)
    else:
        print(f"\n📂 Usando ruta base predeterminada")
    
    # Conectar a MySQL
    conexion = conectar_mysql()
    
    if conexion:
        try:
            # Obtener última fecha y hora
            fecha, hora = obtener_ultima_fecha_hora(conexion)
            
            if fecha is not None and hora is not None:
                print(f"\nÚltimo registro encontrado:")
                print(f"Fecha original: {fecha}")
                print(f"Hora original: {hora}")
                
                # Separar fecha
                mes, dia, año = separar_fecha(fecha)
                
                # Separar hora
                resultado_hora = separar_hora(hora)
                if resultado_hora and len(resultado_hora) == 4:
                    segundos, minutos, horas, milisegundos = resultado_hora
                else:
                    segundos, minutos, horas, milisegundos = None, None, None, None
                
                # Mostrar resultados por consola
                print("\n" + "=" * 50)
                print("FECHA SEPARADA:")
                print("=" * 50)
                print(f"Mes (mm): {mes:02d}" if mes is not None else "Mes: Error al procesar")
                print(f"Día (dd): {dia:02d}" if dia is not None else "Día: Error al procesar")
                print(f"Año (aaaa): {año:04d}" if año is not None else "Año: Error al procesar")
                
                print("\n" + "=" * 50)
                print("HORA SEPARADA:")
                print("=" * 50)
                print(f"Segundos (ss): {segundos:02d}" if segundos is not None else "Segundos: Error al procesar")
                print(f"Minutos (mm): {minutos:02d}" if minutos is not None else "Minutos: Error al procesar")
                print(f"Horas (hh): {horas:02d}" if horas is not None else "Horas: Error al procesar")
                if milisegundos is not None:
                    print(f"Milisegundos: {milisegundos:03d}")
                
                # Obtener directorios pendientes de procesar
                if año is not None and mes is not None and dia is not None:
                    directorios_pendientes, info = obtener_directorios_pendientes(
                        año, mes, dia, hora,
                        ruta_base=args.ruta_base if args.ruta_base else None
                    )
                    
                    if info and mostrar_directorios_pendientes(directorios_pendientes, info):
                        # Solo mostrar menú si hay directorios pendientes
                        while True:
                            mostrar_menu()
                            
                            try:
                                opcion = input("\nSelecciona una opción (1-3): ").strip()
                                
                                if opcion == '1':
                                    seleccion_manual(directorios_pendientes)
                                    break
                                elif opcion == '2':
                                    procesar_todos(directorios_pendientes)
                                    break
                                elif opcion == '3':
                                    print("\n👋 Saliendo del programa...")
                                    break
                                else:
                                    print("❌ Opción no válida. Por favor, selecciona 1, 2 o 3.")
                                    
                            except KeyboardInterrupt:
                                print("\n\n👋 Programa interrumpido por el usuario.")
                                break
                            except Exception as e:
                                print(f"❌ Error inesperado: {e}")
                                break
                
        finally:
            # Cerrar conexión
            if conexion.is_connected():
                conexion.close()
                print("\n" + "=" * 50)
                print("Conexión a MySQL cerrada")
    else:
        print("No se pudo establecer conexión con MySQL")

if __name__ == "__main__":
    main()