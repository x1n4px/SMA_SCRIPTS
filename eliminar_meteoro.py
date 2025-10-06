#!/usr/bin/env python3
"""
Script para eliminar registros de meteoros desde una fecha específica en adelante.
Elimina datos de la tabla Meteoro y todas las tablas relacionadas respetando
la integridad referencial.

Uso:
    python3 eliminar_meteoro.py YYYYMMDD
    
Ejemplo:
    python3 eliminar_meteoro.py 20250901  # Elimina todo desde el 1 de septiembre de 2025 en adelante
"""

import sys
import argparse
import mysql.connector
from mysql.connector import Error
from datetime import datetime
from pathlib import Path

# Importar configuración de base de datos
try:
    from config_db import DB_CONFIG, TABLES, validate_config, get_connection_string
except ImportError:
    print("❌ Error: No se pudo importar config_db.py")
    print("🔧 Asegúrate de que el archivo config_db.py existe en el directorio actual")
    sys.exit(1)

class EliminadorMeteoros:
    def __init__(self):
        """Inicializa el eliminador de meteoros"""
        self.conexion = None
        self.cursor = None
        self.fecha_limite = None
        self.fecha_formateada = None
        
    def conectar_mysql(self):
        """Establece conexión con la base de datos MySQL"""
        # Validar configuración
        config_valida, mensaje = validate_config()
        if not config_valida:
            print(f"❌ Error en configuración: {mensaje}")
            return False
        
        try:
            print(f"🔄 Conectando a {get_connection_string()}...")
            self.conexion = mysql.connector.connect(**DB_CONFIG)
            
            if self.conexion.is_connected():
                print("✅ Conexión exitosa a MySQL")
                self.cursor = self.conexion.cursor()
                return True
                
        except Error as e:
            print(f"❌ Error al conectar a MySQL: {e}")
            return False
            
        return False
    
    def validar_fecha(self, fecha_str):
        """
        Valida y procesa la fecha ingresada
        
        Args:
            fecha_str: Fecha en formato YYYYMMDD
            
        Returns:
            bool: True si la fecha es válida, False en caso contrario
        """
        # Verificar longitud
        if len(fecha_str) != 8:
            print(f"❌ Error: La fecha debe tener 8 dígitos (YYYYMMDD)")
            return False
        
        # Verificar que sean solo números
        if not fecha_str.isdigit():
            print(f"❌ Error: La fecha debe contener solo números")
            return False
        
        # Extraer componentes
        try:
            año = int(fecha_str[:4])
            mes = int(fecha_str[4:6])
            dia = int(fecha_str[6:8])
            
            # Validar rangos
            if año < 1900 or año > 2100:
                print(f"❌ Error: Año fuera de rango (1900-2100)")
                return False
            
            if mes < 1 or mes > 12:
                print(f"❌ Error: Mes inválido (debe estar entre 1 y 12)")
                return False
            
            if dia < 1 or dia > 31:
                print(f"❌ Error: Día inválido (debe estar entre 1 y 31)")
                return False
            
            # Crear fecha para validación adicional
            fecha_obj = datetime(año, mes, dia)
            self.fecha_limite = fecha_obj
            self.fecha_formateada = fecha_obj.strftime('%Y-%m-%d')
            
            print(f"📅 Fecha límite establecida: {self.fecha_formateada}")
            return True
            
        except ValueError as e:
            print(f"❌ Error: Fecha inválida - {e}")
            return False
    
    def obtener_estadisticas(self):
        """
        Obtiene estadísticas de los registros que serán eliminados
        
        Returns:
            dict: Diccionario con las estadísticas por tabla
        """
        estadisticas = {}
        
        try:
            # Estadísticas de tabla Meteoro
            tabla_meteoro = TABLES.get('meteoro', 'Meteoro')
            query_meteoro = f"""
                SELECT COUNT(*) 
                FROM {tabla_meteoro} 
                WHERE Fecha >= %s
            """
            self.cursor.execute(query_meteoro, (self.fecha_formateada,))
            estadisticas['meteoro'] = self.cursor.fetchone()[0]
            
            # Obtener rango de fechas de meteoros a eliminar
            query_rango = f"""
                SELECT MIN(Fecha), MAX(Fecha) 
                FROM {tabla_meteoro} 
                WHERE Fecha >= %s
            """
            self.cursor.execute(query_rango, (self.fecha_formateada,))
            resultado = self.cursor.fetchone()
            if resultado and resultado[0]:
                estadisticas['fecha_min'] = resultado[0]
                estadisticas['fecha_max'] = resultado[1]
            
            # Estadísticas de tabla Informe_Radiante
            tabla_radiante = TABLES.get('radiante', 'Informe_Radiante')
            query_radiante = f"""
                SELECT COUNT(*) 
                FROM {tabla_radiante} r
                INNER JOIN {tabla_meteoro} m ON r.Identificador = m.Identificador
                WHERE m.Fecha >= %s
            """
            self.cursor.execute(query_radiante, (self.fecha_formateada,))
            estadisticas['radiante'] = self.cursor.fetchone()[0]
            
            # Estadísticas de tabla Informe_Fotometria
            tabla_fotometria = TABLES.get('fotometria', 'Informe_Fotometria')
            query_fotometria = f"""
                SELECT COUNT(*) 
                FROM {tabla_fotometria} f
                INNER JOIN {tabla_meteoro} m ON f.Identificador = m.Identificador
                WHERE m.Fecha >= %s
            """
            self.cursor.execute(query_fotometria, (self.fecha_formateada,))
            estadisticas['fotometria'] = self.cursor.fetchone()[0]
            
            # Contar fechas únicas afectadas
            query_fechas = f"""
                SELECT COUNT(DISTINCT Fecha) 
                FROM {tabla_meteoro} 
                WHERE Fecha >= %s
            """
            self.cursor.execute(query_fechas, (self.fecha_formateada,))
            estadisticas['fechas_unicas'] = self.cursor.fetchone()[0]
            
        except Error as e:
            print(f"❌ Error al obtener estadísticas: {e}")
            return None
            
        return estadisticas
    
    def eliminar_registros(self):
        """
        Elimina los registros de las tablas relacionadas en el orden correcto
        para respetar la integridad referencial
        
        Returns:
            bool: True si la eliminación fue exitosa
        """
        try:
            # Obtener nombres de tablas principales
            tabla_meteoro = TABLES.get('meteoro', 'Meteoro')
            tabla_informe_z = TABLES.get('informe_z', 'Informe_Z')
            tabla_radiante = TABLES.get('radiante', 'Informe_Radiante')
            tabla_fotometria = TABLES.get('fotometria', 'Informe_Fotometria')
            
            print("\n🗑️  Iniciando eliminación de registros...")
            print("   Orden de eliminación respetando integridad referencial")
            
            contadores = {}
            paso = 1
            
            # ===== NIVEL 1: Tablas que dependen de Informe_Z =====
            print(f"\n{paso}️⃣  Eliminando tablas dependientes de Informe_Z...")
            
            # Elementos_Orbitales
            query = f"""
                DELETE eo FROM Elementos_Orbitales eo
                INNER JOIN {tabla_informe_z} iz ON eo.Informe_Z_IdInforme = iz.IdInforme
                INNER JOIN {tabla_meteoro} m ON iz.Meteoro_Identificador = m.Identificador
                WHERE m.Fecha >= %s
            """
            self.cursor.execute(query, (self.fecha_formateada,))
            contadores['Elementos_Orbitales'] = self.cursor.rowcount
            print(f"   ✅ Elementos_Orbitales: {contadores['Elementos_Orbitales']} registros")
            
            # Lluvia_activa
            query = f"""
                DELETE la FROM Lluvia_activa la
                INNER JOIN {tabla_informe_z} iz ON la.Informe_Z_IdInforme = iz.IdInforme
                INNER JOIN {tabla_meteoro} m ON iz.Meteoro_Identificador = m.Identificador
                WHERE m.Fecha >= %s
            """
            self.cursor.execute(query, (self.fecha_formateada,))
            contadores['Lluvia_activa'] = self.cursor.rowcount
            print(f"   ✅ Lluvia_activa: {contadores['Lluvia_activa']} registros")
            
            # Puntos_ZWO
            query = f"""
                DELETE pz FROM Puntos_ZWO pz
                INNER JOIN {tabla_informe_z} iz ON pz.Informe_Z_IdInforme = iz.IdInforme
                INNER JOIN {tabla_meteoro} m ON iz.Meteoro_Identificador = m.Identificador
                WHERE m.Fecha >= %s
            """
            self.cursor.execute(query, (self.fecha_formateada,))
            contadores['Puntos_ZWO'] = self.cursor.rowcount
            print(f"   ✅ Puntos_ZWO: {contadores['Puntos_ZWO']} registros")
            
            # Trayectoria_medida
            query = f"""
                DELETE tm FROM Trayectoria_medida tm
                INNER JOIN {tabla_informe_z} iz ON tm.Informe_Z_IdInforme = iz.IdInforme
                INNER JOIN {tabla_meteoro} m ON iz.Meteoro_Identificador = m.Identificador
                WHERE m.Fecha >= %s
            """
            self.cursor.execute(query, (self.fecha_formateada,))
            contadores['Trayectoria_medida'] = self.cursor.rowcount
            print(f"   ✅ Trayectoria_medida: {contadores['Trayectoria_medida']} registros")
            
            # Trayectoria_por_regresion
            query = f"""
                DELETE tpr FROM Trayectoria_por_regresion tpr
                INNER JOIN {tabla_informe_z} iz ON tpr.Informe_Z_IdInforme = iz.IdInforme
                INNER JOIN {tabla_meteoro} m ON iz.Meteoro_Identificador = m.Identificador
                WHERE m.Fecha >= %s
            """
            self.cursor.execute(query, (self.fecha_formateada,))
            contadores['Trayectoria_por_regresion'] = self.cursor.rowcount
            print(f"   ✅ Trayectoria_por_regresion: {contadores['Trayectoria_por_regresion']} registros")
            paso += 1
            
            # ===== NIVEL 2: Tablas que dependen de Informe_Fotometria =====
            print(f"\n{paso}️⃣  Eliminando tablas dependientes de Informe_Fotometria...")
            
            # Datos_meteoro_fotometria
            query = f"""
                DELETE dmf FROM Datos_meteoro_fotometria dmf
                INNER JOIN {tabla_fotometria} ifot ON dmf.Informe_Fotometria_Identificador = ifot.Identificador
                INNER JOIN {tabla_meteoro} m ON ifot.Meteoro_Identificador = m.Identificador
                WHERE m.Fecha >= %s
            """
            self.cursor.execute(query, (self.fecha_formateada,))
            contadores['Datos_meteoro_fotometria'] = self.cursor.rowcount
            print(f"   ✅ Datos_meteoro_fotometria: {contadores['Datos_meteoro_fotometria']} registros")
            
            # Estrellas_usadas_para_regresión
            query = f"""
                DELETE eur FROM Estrellas_usadas_para_regresión eur
                INNER JOIN {tabla_fotometria} ifot ON eur.Informe_Fotometria_Identificador = ifot.Identificador
                INNER JOIN {tabla_meteoro} m ON ifot.Meteoro_Identificador = m.Identificador
                WHERE m.Fecha >= %s
            """
            self.cursor.execute(query, (self.fecha_formateada,))
            contadores['Estrellas_usadas_para_regresión'] = self.cursor.rowcount
            print(f"   ✅ Estrellas_usadas_para_regresión: {contadores['Estrellas_usadas_para_regresión']} registros")
            
            # Puntos_del_ajuste
            query = f"""
                DELETE pa FROM Puntos_del_ajuste pa
                INNER JOIN {tabla_fotometria} ifot ON pa.Informe_Fotometria_Identificador = ifot.Identificador
                INNER JOIN {tabla_meteoro} m ON ifot.Meteoro_Identificador = m.Identificador
                WHERE m.Fecha >= %s
            """
            self.cursor.execute(query, (self.fecha_formateada,))
            contadores['Puntos_del_ajuste'] = self.cursor.rowcount
            print(f"   ✅ Puntos_del_ajuste: {contadores['Puntos_del_ajuste']} registros")
            paso += 1
            
            # ===== NIVEL 3: Tablas que dependen de Informe_Radiante =====
            print(f"\n{paso}️⃣  Eliminando tablas dependientes de Informe_Radiante...")
            
            # Lluvia_Activa_InfRad
            query = f"""
                DELETE lair FROM Lluvia_Activa_InfRad lair
                INNER JOIN {tabla_radiante} ir ON lair.Informe_Radiante_Identificador = ir.Identificador
                INNER JOIN {tabla_meteoro} m ON ir.Meteoro_Identificador = m.Identificador
                WHERE m.Fecha >= %s
            """
            self.cursor.execute(query, (self.fecha_formateada,))
            contadores['Lluvia_Activa_InfRad'] = self.cursor.rowcount
            print(f"   ✅ Lluvia_Activa_InfRad: {contadores['Lluvia_Activa_InfRad']} registros")
            
            # Trayectoria_estimada
            query = f"""
                DELETE te FROM Trayectoria_estimada te
                INNER JOIN {tabla_radiante} ir ON te.Informe_Radiante_Identificador = ir.Identificador
                INNER JOIN {tabla_meteoro} m ON ir.Meteoro_Identificador = m.Identificador
                WHERE m.Fecha >= %s
            """
            self.cursor.execute(query, (self.fecha_formateada,))
            contadores['Trayectoria_estimada'] = self.cursor.rowcount
            print(f"   ✅ Trayectoria_estimada: {contadores['Trayectoria_estimada']} registros")
            
            # Velociades_Angulares
            query = f"""
                DELETE va FROM Velociades_Angulares va
                INNER JOIN {tabla_radiante} ir ON va.Informe_Radiante_Identificador = ir.Identificador
                INNER JOIN {tabla_meteoro} m ON ir.Meteoro_Identificador = m.Identificador
                WHERE m.Fecha >= %s
            """
            self.cursor.execute(query, (self.fecha_formateada,))
            contadores['Velociades_Angulares'] = self.cursor.rowcount
            print(f"   ✅ Velociades_Angulares: {contadores['Velociades_Angulares']} registros")
            paso += 1
            
            # ===== NIVEL 4: Tablas de informes principales =====
            print(f"\n{paso}️⃣  Eliminando tablas de informes principales...")
            
            # Informe_Z
            query = f"""
                DELETE iz FROM {tabla_informe_z} iz
                INNER JOIN {tabla_meteoro} m ON iz.Meteoro_Identificador = m.Identificador
                WHERE m.Fecha >= %s
            """
            self.cursor.execute(query, (self.fecha_formateada,))
            contadores['Informe_Z'] = self.cursor.rowcount
            print(f"   ✅ {tabla_informe_z}: {contadores['Informe_Z']} registros")
            
            # Informe_Fotometria
            query = f"""
                DELETE ifot FROM {tabla_fotometria} ifot
                INNER JOIN {tabla_meteoro} m ON ifot.Meteoro_Identificador = m.Identificador
                WHERE m.Fecha >= %s
            """
            self.cursor.execute(query, (self.fecha_formateada,))
            contadores['Informe_Fotometria'] = self.cursor.rowcount
            print(f"   ✅ {tabla_fotometria}: {contadores['Informe_Fotometria']} registros")
            
            # Informe_Radiante
            query = f"""
                DELETE ir FROM {tabla_radiante} ir
                INNER JOIN {tabla_meteoro} m ON ir.Meteoro_Identificador = m.Identificador
                WHERE m.Fecha >= %s
            """
            self.cursor.execute(query, (self.fecha_formateada,))
            contadores['Informe_Radiante'] = self.cursor.rowcount
            print(f"   ✅ {tabla_radiante}: {contadores['Informe_Radiante']} registros")
            paso += 1
            
            # ===== NIVEL 5: Tabla principal Meteoro =====
            print(f"\n{paso}️⃣  Eliminando tabla principal Meteoro...")
            query = f"""
                DELETE FROM {tabla_meteoro}
                WHERE Fecha >= %s
            """
            self.cursor.execute(query, (self.fecha_formateada,))
            contadores['Meteoro'] = self.cursor.rowcount
            print(f"   ✅ {tabla_meteoro}: {contadores['Meteoro']} registros")
            
            # Confirmar transacción
            self.conexion.commit()
            
            # Resumen final
            print("\n" + "=" * 60)
            print("RESUMEN DE ELIMINACIÓN")
            print("=" * 60)
            
            total = sum(contadores.values())
            print(f"📊 Total de registros eliminados: {total:,}")
            print("\nDetalle por tabla:")
            for tabla, count in contadores.items():
                if count > 0:
                    print(f"   • {tabla}: {count:,}")
            
            return True
            
        except Error as e:
            print(f"\n❌ Error al eliminar registros: {e}")
            print("🔄 Revirtiendo cambios...")
            self.conexion.rollback()
            return False
    
    def cerrar_conexion(self):
        """Cierra la conexión con la base de datos"""
        if self.cursor:
            self.cursor.close()
        if self.conexion and self.conexion.is_connected():
            self.conexion.close()
            print("\n🔌 Conexión a MySQL cerrada")

def main():
    """Función principal del script"""
    # Configurar parser de argumentos
    parser = argparse.ArgumentParser(
        description='Elimina registros de meteoros desde una fecha específica en adelante',
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
Ejemplos de uso:
  python3 eliminar_meteoro.py 20250901   # Elimina desde septiembre 2025
  python3 eliminar_meteoro.py 20240101   # Elimina desde enero 2024
  python3 eliminar_meteoro.py 20230815   # Elimina desde 15 de agosto 2023

ADVERTENCIA: Esta operación NO se puede deshacer.
Asegúrate de tener un respaldo de la base de datos antes de ejecutar este script.
        """
    )
    
    parser.add_argument(
        'fecha',
        help='Fecha desde la cual eliminar (formato: YYYYMMDD)'
    )
    
    parser.add_argument(
        '--sin-confirmacion',
        action='store_true',
        help='Ejecutar sin pedir confirmación (¡PELIGROSO!)'
    )
    
    args = parser.parse_args()
    
    # Banner inicial
    print("=" * 60)
    print("ELIMINADOR DE REGISTROS DE METEOROS")
    print("=" * 60)
    print("⚠️  ADVERTENCIA: Esta operación eliminará datos permanentemente")
    print("⚠️  Se recomienda hacer un respaldo antes de continuar")
    print()
    
    # Crear instancia del eliminador
    eliminador = EliminadorMeteoros()
    
    # Validar fecha
    if not eliminador.validar_fecha(args.fecha):
        sys.exit(1)
    
    # Conectar a la base de datos
    if not eliminador.conectar_mysql():
        sys.exit(1)
    
    try:
        # Obtener estadísticas
        print("\n📊 Obteniendo estadísticas de registros a eliminar...")
        estadisticas = eliminador.obtener_estadisticas()
        
        if not estadisticas:
            print("❌ No se pudieron obtener las estadísticas")
            sys.exit(1)
        
        # Mostrar estadísticas
        print("\n" + "=" * 60)
        print("REGISTROS QUE SERÁN ELIMINADOS")
        print("=" * 60)
        print(f"📅 Fecha límite: {eliminador.fecha_formateada} (inclusive)")
        
        if estadisticas.get('fecha_min'):
            print(f"📅 Rango de fechas afectadas: {estadisticas['fecha_min']} a {estadisticas['fecha_max']}")
            print(f"📅 Fechas únicas afectadas: {estadisticas['fechas_unicas']}")
        
        print(f"\n📊 Registros por tabla:")
        print(f"   • Meteoro: {estadisticas['meteoro']:,}")
        print(f"   • Radiante: {estadisticas['radiante']:,}")
        print(f"   • Fotometria: {estadisticas['fotometria']:,}")
        total = estadisticas['meteoro'] + estadisticas['radiante'] + estadisticas['fotometria']
        print(f"   • TOTAL: {total:,}")
        
        if total == 0:
            print("\n✅ No hay registros para eliminar con la fecha especificada")
            sys.exit(0)
        
        # Solicitar confirmación
        if not args.sin_confirmacion:
            print("\n" + "=" * 60)
            print("⚠️  CONFIRMACIÓN REQUERIDA")
            print("=" * 60)
            print(f"Estás a punto de eliminar {total:,} registros de la base de datos.")
            print("Esta operación NO se puede deshacer.")
            print()
            
            confirmacion = input("¿Estás seguro que deseas continuar? (escribe 'SI' para confirmar): ")
            
            if confirmacion.upper() != 'SI':
                print("\n❌ Operación cancelada por el usuario")
                sys.exit(0)
        
        # Ejecutar eliminación
        if eliminador.eliminar_registros():
            print("\n✅ Eliminación completada exitosamente")
        else:
            print("\n❌ La eliminación falló")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Operación interrumpida por el usuario")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")
        sys.exit(1)
        
    finally:
        # Cerrar conexión
        eliminador.cerrar_conexion()

if __name__ == "__main__":
    main()