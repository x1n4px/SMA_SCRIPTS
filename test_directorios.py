#!/usr/bin/env python3
"""
Script de prueba para verificar la funcionalidad de verificación
de directorios pendientes sin necesidad de MySQL
"""

from pathlib import Path
import os

def verificar_directorios_pendientes(año, mes, dia, hora_completa):
    """
    Verifica cuántos directorios faltan por procesar después de la última fecha/hora procesada
    """
    # Para prueba, usar una ruta de ejemplo en tu directorio actual
    # Cambiar a la ruta real cuando esté disponible
    # ruta_base = Path("/home/sma/Meteoros/Detecciones")
    
    # Para pruebas locales, usando estructura de ejemplo:
    ruta_base = Path("./test_meteoros/Detecciones")
    
    # Construir ruta al año
    ruta_año = ruta_base / str(año)
    
    if not ruta_año.exists():
        print(f"\n⚠️  La ruta {ruta_año} no existe")
        print(f"Intentando crear estructura de ejemplo en directorio local...")
        
        # Crear estructura de ejemplo para pruebas
        test_base = Path("./test_meteoros/Detecciones")
        test_año = test_base / str(año)
        fecha_carpeta = f"{año:04d}{mes:02d}{dia:02d}"
        test_fecha = test_año / fecha_carpeta
        
        # Crear estructura de directorios de ejemplo
        test_fecha.mkdir(parents=True, exist_ok=True)
        
        # Crear algunos directorios de hora de ejemplo
        horas_ejemplo = ["020000", "023000", "030000", "033000", "040000", 
                        "043000", "050000", "051848", "053000", "060000"]
        
        for hora_dir in horas_ejemplo:
            (test_fecha / hora_dir).mkdir(exist_ok=True)
        
        print(f"✅ Estructura de ejemplo creada en: {test_base}")
        print(f"   Puedes probar el script con la ruta de ejemplo")
        return
    
    # Construir ruta a la fecha (yyyymmdd)
    fecha_carpeta = f"{año:04d}{mes:02d}{dia:02d}"
    ruta_fecha = ruta_año / fecha_carpeta
    
    if not ruta_fecha.exists():
        print(f"\n⚠️  La carpeta de fecha {ruta_fecha} no existe")
        # Mostrar fechas disponibles en ese año
        fechas_disponibles = sorted([d for d in ruta_año.iterdir() if d.is_dir()])
        if fechas_disponibles:
            print(f"\nFechas disponibles en {año}:")
            for fecha_dir in fechas_disponibles[:10]:
                print(f"  - {fecha_dir.name}")
            if len(fechas_disponibles) > 10:
                print(f"  ... y {len(fechas_disponibles) - 10} más")
        return
    
    print(f"\n📁 Navegando a: {ruta_fecha}")
    
    # Listar todos los directorios de hora en esa fecha
    try:
        directorios_hora = sorted([d for d in ruta_fecha.iterdir() if d.is_dir()])
        
        if not directorios_hora:
            print("No se encontraron directorios de hora en esta fecha")
            return
        
        # Convertir la hora completa procesada a formato comparable
        if hora_completa:
            # Extraer horas, minutos, segundos de la hora completa
            if isinstance(hora_completa, str):
                partes_hora = hora_completa.split('.')[0].split(':')
                if len(partes_hora) >= 3:
                    hora_proc = int(partes_hora[0])
                    min_proc = int(partes_hora[1])
                    seg_proc = int(partes_hora[2])
                    # Formato de comparación: hhmmss
                    hora_procesada_str = f"{hora_proc:02d}{min_proc:02d}{seg_proc:02d}"
                else:
                    hora_procesada_str = "000000"
            else:
                hora_procesada_str = "000000"
        else:
            hora_procesada_str = "000000"
        
        # Contar directorios pendientes
        directorios_pendientes = []
        directorios_procesados = []
        
        for directorio in directorios_hora:
            nombre_dir = directorio.name
            # Comparar con la hora procesada
            if nombre_dir > hora_procesada_str:
                directorios_pendientes.append(nombre_dir)
            else:
                directorios_procesados.append(nombre_dir)
        
        # Mostrar estadísticas
        print("\n" + "=" * 50)
        print("ANÁLISIS DE DIRECTORIOS")
        print("=" * 50)
        print(f"📊 Total de directorios en {fecha_carpeta}: {len(directorios_hora)}")
        print(f"✅ Directorios ya procesados: {len(directorios_procesados)}")
        print(f"⏳ Directorios pendientes de procesar: {len(directorios_pendientes)}")
        
        if directorios_pendientes:
            print(f"\n📋 Última hora procesada: {hora_procesada_str[:2]}:{hora_procesada_str[2:4]}:{hora_procesada_str[4:6]}")
            print(f"\n📂 Próximos directorios a procesar:")
            # Mostrar los primeros 10 directorios pendientes
            for i, dir_pendiente in enumerate(directorios_pendientes[:10], 1):
                hora_formato = f"{dir_pendiente[:2]}:{dir_pendiente[2:4]}:{dir_pendiente[4:6]}"
                print(f"   {i:2}. {dir_pendiente} ({hora_formato})")
            
            if len(directorios_pendientes) > 10:
                print(f"\n   ... y {len(directorios_pendientes) - 10} directorios más pendientes")
        else:
            print("\n✅ ¡Todos los directorios han sido procesados!")
        
        # Mostrar rango de tiempo cubierto
        if directorios_hora:
            primer_dir = directorios_hora[0].name
            ultimo_dir = directorios_hora[-1].name
            print(f"\n⏰ Rango de tiempo disponible:")
            print(f"   Desde: {primer_dir[:2]}:{primer_dir[2:4]}:{primer_dir[4:6]}")
            print(f"   Hasta: {ultimo_dir[:2]}:{ultimo_dir[2:4]}:{ultimo_dir[4:6]}")
        
    except Exception as e:
        print(f"\n❌ Error al analizar directorios: {e}")

# Datos de prueba basados en tu ejemplo
año = 2023
mes = 1
dia = 2
hora_completa = "05:18:48.0364"

print("=" * 50)
print("PRUEBA DE VERIFICACIÓN DE DIRECTORIOS")
print("=" * 50)
print(f"\nDatos de entrada:")
print(f"  Año: {año}")
print(f"  Mes: {mes:02d}")
print(f"  Día: {dia:02d}")
print(f"  Última hora procesada: {hora_completa}")

# Ejecutar verificación
verificar_directorios_pendientes(año, mes, dia, hora_completa)