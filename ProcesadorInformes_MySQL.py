#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script unificado para procesar informes de meteoros
Permite procesar todos los informes o filtrar por fechas/tipos
"""

import sys
import os
import math
import pathlib
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error
from typing import List, Tuple, Optional

# =============================================================================
# IMPORTAR CONFIGURACIÓN CENTRALIZADA
# =============================================================================
try:
    from config_db import DB_CONFIG, CONNECTION_CONFIG, PATHS_CONFIG, TABLES, validate_config, get_connection_string
except ImportError:
    print("❌ Error: No se pudo importar config_db.py")
    print("🔧 Asegúrate de que el archivo config_db.py existe en el directorio actual")
    sys.exit(1)


def resolve_base_path(user_path: Optional[str]) -> Path:
    """Resuelve la carpeta base de detecciones usando config_db por defecto."""
    if user_path:
        return Path(user_path).expanduser().resolve()

    default_relative = Path(
        PATHS_CONFIG.get("meteor_detections_base", "Carpeta-meteoro-procesado/home/sma/Detecciones")
    ).expanduser()
    if default_relative.is_absolute():
        return default_relative.resolve()
    return (Path(__file__).resolve().parent / default_relative).resolve()

# =============================================================================
# CLASE PRINCIPAL DEL PROCESADOR
# =============================================================================
class ProcesadorInformes:
    def __init__(self):
        """Inicializa el procesador de informes"""
        self.cnxn = None
        self.cursor = None
        self.estadisticas = {
            'fotometria': {'procesados': 0, 'omitidos': 0, 'errores': 0},
            'radiante': {'procesados': 0, 'omitidos': 0, 'errores': 0},
            'z': {'procesados': 0, 'omitidos': 0, 'errores': 0}
        }
        
    def conectar_db(self) -> bool:
        """Establece conexión con la base de datos MySQL usando configuración centralizada"""
        # Validar configuración
        config_valida, mensaje = validate_config()
        if not config_valida:
            print(f"❌ Error en configuración: {mensaje}")
            return False
        
        try:
            # Crear copia de configuración con timeout
            config_con_timeout = DB_CONFIG.copy()
            config_con_timeout['connection_timeout'] = CONNECTION_CONFIG.get('connection_timeout', 30)
            
            print(f"🔄 Conectando a {get_connection_string()}")
            self.cnxn = mysql.connector.connect(**config_con_timeout)
            self.cursor = self.cnxn.cursor()
            print("✅ Conexión establecida con la base de datos")
            return True
        except mysql.connector.Error as e:
            print(f"❌ Error de conexión MySQL: {e}")
            return False
    
    def cerrar_db(self):
        """Cierra la conexión con la base de datos"""
        if self.cursor:
            self.cursor.close()
        if self.cnxn:
            self.cnxn.close()
        print("✓ Conexión cerrada")

    def verificar_informe_existe(self, tipo_informe: str, fecha: str, hora: str) -> bool:
        """
        Verifica si un informe ya existe en la base de datos
        
        Args:
            tipo_informe: 'fotometria', 'radiante' o 'z'
            fecha: Fecha en formato YYYY-MM-DD
            hora: Hora en formato HH:MM:SS.mmm
        
        Returns:
            True si el informe existe, False en caso contrario
        """
        tablas = {
            'fotometria': 'Informe_Fotometria',
            'radiante': 'Informe_Radiante',
            'z': 'Informe_Z'
        }
        
        if tipo_informe not in tablas:
            return False
        
        tabla = tablas[tipo_informe]
        id_campo = 'Identificador' if tipo_informe != 'z' else 'IdInforme'
        
        query = f"SELECT {id_campo} FROM {tabla} WHERE Fecha = %s AND Hora = %s"
        self.cursor.execute(query, (fecha, hora))
        resultado = self.cursor.fetchone()
        
        return resultado is not None
    
    def obtener_fecha_hora_de_ruta(self, ruta: str) -> Tuple[str, str]:
        """
        Extrae fecha y hora de la estructura del directorio
        Formato: .../yyyymmdd/hhmmss/...
        
        Returns:
            Tupla (fecha, hora) en formato MySQL
        """
        partes = ruta.split('/')
        
        # Buscar la fecha (yyyymmdd)
        fecha_str = None
        hora_str = None
        
        for i, parte in enumerate(partes):
            # Buscar fecha en formato yyyymmdd
            if len(parte) == 8 and parte.isdigit():
                fecha_str = parte
                # La hora debería estar en la siguiente carpeta
                if i + 1 < len(partes) and len(partes[i + 1]) >= 6:
                    hora_str = partes[i + 1]
                    break
        
        if not fecha_str or not hora_str:
            return None, None
        
        # Convertir fecha a formato MySQL
        fecha = f"{fecha_str[:4]}-{fecha_str[4:6]}-{fecha_str[6:8]}"
        
        # Convertir hora a formato MySQL
        if len(hora_str) >= 6:
            hora = f"{hora_str[:2]}:{hora_str[2:4]}:{hora_str[4:6]}"
            if len(hora_str) > 6:
                hora += f".{hora_str[6:]}"
        else:
            hora = "00:00:00"
        
        return fecha, hora
    
    def filtrar_por_fecha(self, fecha_str: str, fecha_inicio: datetime, fecha_fin: datetime) -> bool:
        """
        Verifica si una fecha está dentro del rango especificado
        """
        try:
            # fecha_str formato: yyyymmdd
            fecha = datetime.strptime(fecha_str, "%Y%m%d")
            return fecha_inicio <= fecha <= fecha_fin
        except:
            return False
    
    def buscar_informes(self, directorio_base: str, filtro_fecha: Optional[Tuple[datetime, datetime]] = None) -> List[Tuple[str, str]]:
        """
        Busca todos los informes en el directorio especificado
        
        Returns:
            Lista de tuplas (ruta_completa, tipo_informe)
        """
        informes = []
        
        # Patrones de búsqueda para cada tipo de informe
        patrones = {
            'fotometria': 'Informe-fotometria',
            'radiante': 'Informe-Radiante',
            'z': 'Informe-Z'
        }
        
        print(f"\n🔍 Buscando informes en: {directorio_base}")
        
        for root, dirs, files in os.walk(directorio_base):
            # Si hay filtro de fecha, verificar la carpeta
            if filtro_fecha:
                partes = root.split('/')
                fecha_carpeta = None
                for parte in partes:
                    if len(parte) == 8 and parte.isdigit():
                        fecha_carpeta = parte
                        break
                
                if fecha_carpeta and not self.filtrar_por_fecha(fecha_carpeta, filtro_fecha[0], filtro_fecha[1]):
                    continue
            
            for archivo in files:
                for tipo, patron in patrones.items():
                    if archivo.startswith(patron):
                        # Excluir archivos .kml para informes Z
                        if tipo == 'z' and archivo.endswith('.kml'):
                            continue
                        
                        ruta_completa = os.path.join(root, archivo)
                        informes.append((ruta_completa, tipo))
        
        print(f"✓ Encontrados {len(informes)} informes")
        return informes
    
    def procesar_informe_fotometria(self, ruta: str, archivo: str) -> bool:
        """Procesa un informe de fotometría"""
        try:
            fecha, hora = self.obtener_fecha_hora_de_ruta(ruta)
            if not fecha or not hora:
                print(f"  ⚠ No se pudo extraer fecha/hora de la ruta")
                return False
            
            # Verificar si ya existe
            if self.verificar_informe_existe('fotometria', fecha, hora):
                self.estadisticas['fotometria']['omitidos'] += 1
                return False
            
            # Aquí iría el código de procesamiento del informe de fotometría
            # (copiar la lógica del script CargaInformesFot_MySQL.py)
            
            self.estadisticas['fotometria']['procesados'] += 1
            print(f"  ✓ Procesado: {archivo}")
            return True
            
        except Exception as e:
            self.estadisticas['fotometria']['errores'] += 1
            print(f"  ✗ Error procesando {archivo}: {e}")
            return False
    
    def procesar_informe_radiante(self, ruta: str, archivo: str) -> bool:
        """Procesa un informe de radiante"""
        try:
            fecha, hora = self.obtener_fecha_hora_de_ruta(ruta)
            if not fecha or not hora:
                # Intentar extraer del nombre del archivo
                if len(archivo) > 29:
                    year = archivo[17:21]
                    mes = archivo[21:23]
                    dia = archivo[23:25]
                    hora_i = archivo[25:27]
                    min_i = archivo[27:29]
                    seg_i = archivo[29:36] if len(archivo) > 36 else archivo[29:31] + ".0000"
                    
                    fecha = f"{year}-{mes}-{dia}"
                    hora = f"{hora_i}:{min_i}:{seg_i}"
                else:
                    print(f"  ⚠ No se pudo extraer fecha/hora")
                    return False
            
            # Verificar si ya existe
            if self.verificar_informe_existe('radiante', fecha, hora):
                self.estadisticas['radiante']['omitidos'] += 1
                return False
            
            # Aquí iría el código de procesamiento del informe de radiante
            # (copiar la lógica del script CargaInformesRad_MySQL.py)
            
            self.estadisticas['radiante']['procesados'] += 1
            print(f"  ✓ Procesado: {archivo}")
            return True
            
        except Exception as e:
            self.estadisticas['radiante']['errores'] += 1
            print(f"  ✗ Error procesando {archivo}: {e}")
            return False
    
    def procesar_informe_z(self, ruta: str, archivo: str) -> bool:
        """Procesa un informe Z"""
        try:
            # Extraer fecha y hora del contenido del archivo
            with open(os.path.join(ruta, archivo), 'r', encoding='utf-8') as f:
                primera_linea = f.readline().strip()
            
            if len(primera_linea) >= 24:
                year = primera_linea[:4]
                mes = primera_linea[5:7]
                dia = primera_linea[8:10]
                hora_i = primera_linea[11:13]
                min_i = primera_linea[14:16]
                seg_i = primera_linea[17:24]
                
                fecha = f"{year}-{mes}-{dia}"
                hora = f"{hora_i}:{min_i}:{seg_i}"
            else:
                print(f"  ⚠ No se pudo extraer fecha/hora del archivo")
                return False
            
            # Verificar si ya existe
            if self.verificar_informe_existe('z', fecha, hora):
                self.estadisticas['z']['omitidos'] += 1
                return False
            
            # Aquí iría el código de procesamiento del informe Z
            # (copiar la lógica del script CargaInformesZ_MySQL.py)
            
            self.estadisticas['z']['procesados'] += 1
            print(f"  ✓ Procesado: {archivo}")
            return True
            
        except Exception as e:
            self.estadisticas['z']['errores'] += 1
            print(f"  ✗ Error procesando {archivo}: {e}")
            return False
    
    def procesar_informes(self, informes: List[Tuple[str, str]], tipos_permitidos: List[str] = None):
        """
        Procesa una lista de informes
        
        Args:
            informes: Lista de tuplas (ruta_completa, tipo_informe)
            tipos_permitidos: Lista de tipos a procesar (None = todos)
        """
        total = len(informes)
        procesados = 0
        
        print(f"\n📊 Procesando {total} informes...")
        print("=" * 60)
        
        for i, (ruta_completa, tipo) in enumerate(informes, 1):
            # Filtrar por tipo si se especifica
            if tipos_permitidos and tipo not in tipos_permitidos:
                continue
            
            directorio = os.path.dirname(ruta_completa)
            archivo = os.path.basename(ruta_completa)
            
            print(f"\n[{i}/{total}] Procesando {tipo}: {archivo}")
            print(f"  📁 {directorio}")
            
            # Procesar según el tipo
            if tipo == 'fotometria':
                if self.procesar_informe_fotometria(directorio, archivo):
                    procesados += 1
            elif tipo == 'radiante':
                if self.procesar_informe_radiante(directorio, archivo):
                    procesados += 1
            elif tipo == 'z':
                if self.procesar_informe_z(directorio, archivo):
                    procesados += 1
        
        print("\n" + "=" * 60)
        print(f"✓ Procesamiento completado: {procesados}/{total} informes procesados")
    
    def mostrar_estadisticas(self):
        """Muestra las estadísticas del procesamiento"""
        print("\n" + "=" * 60)
        print("📈 ESTADÍSTICAS DEL PROCESAMIENTO")
        print("=" * 60)
        
        for tipo, stats in self.estadisticas.items():
            total = stats['procesados'] + stats['omitidos'] + stats['errores']
            if total > 0:
                print(f"\n{tipo.upper()}:")
                print(f"  ✓ Procesados: {stats['procesados']}")
                print(f"  ⊘ Omitidos (ya existen): {stats['omitidos']}")
                print(f"  ✗ Errores: {stats['errores']}")
                print(f"  Σ Total: {total}")

# =============================================================================
# FUNCIONES DE INTERFAZ DE USUARIO
# =============================================================================
def mostrar_menu_principal():
    """Muestra el menú principal"""
    print("\n" + "=" * 60)
    print("🌟 PROCESADOR DE INFORMES DE METEOROS")
    print("=" * 60)
    print("\nOpciones de procesamiento:")
    print("  1. Procesar TODOS los informes")
    print("  2. Procesar por RANGO DE FECHAS")
    print("  3. Procesar por TIPO de informe")
    print("  4. Procesar FECHA ESPECÍFICA")
    print("  5. Configurar base de datos")
    print("  0. Salir")
    print("-" * 60)

def obtener_rango_fechas() -> Tuple[datetime, datetime]:
    """Solicita al usuario un rango de fechas"""
    while True:
        try:
            print("\n📅 Ingrese el rango de fechas:")
            fecha_inicio_str = input("  Fecha inicio (YYYY-MM-DD): ")
            fecha_fin_str = input("  Fecha fin (YYYY-MM-DD): ")
            
            fecha_inicio = datetime.strptime(fecha_inicio_str, "%Y-%m-%d")
            fecha_fin = datetime.strptime(fecha_fin_str, "%Y-%m-%d")
            
            if fecha_inicio > fecha_fin:
                print("  ⚠ La fecha de inicio debe ser anterior a la fecha fin")
                continue
            
            # Ajustar para incluir todo el día final
            fecha_fin = fecha_fin + timedelta(days=1, seconds=-1)
            
            return fecha_inicio, fecha_fin
            
        except ValueError:
            print("  ⚠ Formato de fecha inválido. Use YYYY-MM-DD")

def obtener_tipos_informe() -> List[str]:
    """Solicita al usuario qué tipos de informes procesar"""
    print("\n📋 Seleccione los tipos de informes a procesar:")
    print("  1. Fotometría")
    print("  2. Radiante")
    print("  3. Z")
    print("  4. Todos")
    
    opcion = input("\nOpción (puede elegir varios, ej: 1,2): ").strip()
    
    if '4' in opcion or opcion == '':
        return ['fotometria', 'radiante', 'z']
    
    tipos = []
    if '1' in opcion:
        tipos.append('fotometria')
    if '2' in opcion:
        tipos.append('radiante')
    if '3' in opcion:
        tipos.append('z')
    
    return tipos if tipos else ['fotometria', 'radiante', 'z']

def configurar_base_datos():
    """Permite al usuario configurar la conexión a la base de datos"""
    print("\n⚙️  CONFIGURACIÓN DE BASE DE DATOS")
    print("-" * 40)
    
    DB_CONFIG['host'] = input(f"Host [{DB_CONFIG['host']}]: ") or DB_CONFIG['host']
    port_str = input(f"Puerto [{DB_CONFIG['port']}]: ")
    if port_str:
        DB_CONFIG['port'] = int(port_str)
    DB_CONFIG['database'] = input(f"Base de datos [{DB_CONFIG['database']}]: ") or DB_CONFIG['database']
    DB_CONFIG['user'] = input(f"Usuario [{DB_CONFIG['user']}]: ") or DB_CONFIG['user']
    DB_CONFIG['password'] = input("Contraseña: ") or DB_CONFIG['password']
    
    print("✓ Configuración actualizada")

# =============================================================================
# FUNCIÓN PRINCIPAL
# =============================================================================
def main():
    """Función principal del programa"""
    procesador = ProcesadorInformes()

    directorio_base = resolve_base_path(sys.argv[1] if len(sys.argv) > 1 else None)
    if not directorio_base.exists():
        print(f"✗ El directorio base '{directorio_base}' no existe")
        return
    
    while True:
        mostrar_menu_principal()
        opcion = input("\nSeleccione una opción: ").strip()
        
        if opcion == '0':
            print("\n👋 ¡Hasta luego!")
            break
        
        elif opcion == '5':
            configurar_base_datos()
            continue
        
        elif opcion in ['1', '2', '3', '4']:
            # Conectar a la base de datos
            if not procesador.conectar_db():
                continue
            
            try:
                filtro_fecha = None
                tipos_permitidos = None
                
                if opcion == '1':
                    # Procesar todos
                    print("\n♾️  Procesando TODOS los informes...")
                    
                elif opcion == '2':
                    # Procesar por rango de fechas
                    filtro_fecha = obtener_rango_fechas()
                    print(f"\n📅 Procesando del {filtro_fecha[0].date()} al {filtro_fecha[1].date()}")
                    
                elif opcion == '3':
                    # Procesar por tipo
                    tipos_permitidos = obtener_tipos_informe()
                    print(f"\n📋 Procesando tipos: {', '.join(tipos_permitidos)}")
                    
                elif opcion == '4':
                    # Procesar fecha específica
                    fecha_str = input("\n📅 Fecha específica (YYYY-MM-DD): ")
                    fecha = datetime.strptime(fecha_str, "%Y-%m-%d")
                    filtro_fecha = (fecha, fecha + timedelta(days=1, seconds=-1))
                    print(f"\n📅 Procesando fecha: {fecha.date()}")
                
                # Buscar informes
                informes = procesador.buscar_informes(directorio_base, filtro_fecha)
                
                if not informes:
                    print("\n⚠ No se encontraron informes con los criterios especificados")
                else:
                    # Confirmar procesamiento
                    print(f"\n¿Procesar {len(informes)} informes? (S/N): ", end='')
                    if input().upper() == 'S':
                        procesador.procesar_informes(informes, tipos_permitidos)
                        procesador.mostrar_estadisticas()
                
            finally:
                procesador.cerrar_db()
        
        else:
            print("⚠ Opción no válida")
        
        input("\nPresione ENTER para continuar...")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠ Proceso interrumpido por el usuario")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Error inesperado: {e}")
        sys.exit(1)
