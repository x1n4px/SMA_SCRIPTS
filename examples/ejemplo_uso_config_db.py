#!/usr/bin/env python3
"""
Ejemplo de cómo usar la configuración centralizada de base de datos en otros scripts.
Este archivo sirve como plantilla para actualizar otros scripts del sistema.
"""

import mysql.connector
from mysql.connector import Error
import sys

# Importar la configuración centralizada
try:
    from config_db import DB_CONFIG, CONNECTION_CONFIG, TABLES, validate_config, get_connection_string
except ImportError:
    print("❌ Error: No se pudo importar config_db.py")
    print("🔧 Asegúrate de que el archivo config_db.py existe en el directorio actual")
    sys.exit(1)


def conectar_db():
    """
    Ejemplo de función para conectar a la base de datos usando la configuración centralizada.
    """
    # Validar configuración
    config_valida, mensaje = validate_config()
    if not config_valida:
        print(f"❌ Error en configuración: {mensaje}")
        return None
    
    try:
        print(f"🔄 Conectando a {get_connection_string()}")
        
        # Conectar usando la configuración centralizada
        conexion = mysql.connector.connect(**DB_CONFIG)
        
        if conexion.is_connected():
            print("✅ Conexión exitosa")
            return conexion
            
    except Error as e:
        print(f"❌ Error al conectar: {e}")
        return None


def ejemplo_consulta(conexion):
    """
    Ejemplo de cómo usar los nombres de tabla desde la configuración.
    """
    cursor = conexion.cursor()
    
    # Usar nombres de tabla desde la configuración
    tabla_meteoro = TABLES.get('meteoro', 'Meteoro')
    tabla_observatorio = TABLES.get('observatorio', 'Observatorio')
    
    # Ejemplo de consulta
    query = f"""
    SELECT COUNT(*) 
    FROM {tabla_meteoro}
    """
    
    try:
        cursor.execute(query)
        resultado = cursor.fetchone()
        print(f"📊 Total de registros en {tabla_meteoro}: {resultado[0]}")
        
    except Error as e:
        print(f"❌ Error en consulta: {e}")
    finally:
        cursor.close()


def main():
    """
    Función principal de ejemplo.
    """
    print("=" * 50)
    print("EJEMPLO DE USO DE CONFIGURACIÓN CENTRALIZADA")
    print("=" * 50)
    
    # Mostrar información de configuración
    print(f"\n🔧 Configuración de Base de Datos:")
    print(f"   Base de datos: {DB_CONFIG.get('database')}")
    print(f"   Servidor: {DB_CONFIG.get('host')}:{DB_CONFIG.get('port', 3306)}")
    print(f"   Usuario: {DB_CONFIG.get('user')}")
    print(f"   Charset: {DB_CONFIG.get('charset')}")
    
    print(f"\n🔧 Configuración de Conexión:")
    print(f"   Timeout: {CONNECTION_CONFIG.get('connection_timeout')} segundos")
    print(f"   Reintentos: {CONNECTION_CONFIG.get('retry_attempts')}")
    print(f"   Delay entre reintentos: {CONNECTION_CONFIG.get('retry_delay')} segundos")
    
    # Conectar a la base de datos
    conexion = conectar_db()
    
    if conexion:
        try:
            # Ejecutar consulta de ejemplo
            ejemplo_consulta(conexion)
            
        finally:
            if conexion.is_connected():
                conexion.close()
                print("\n✅ Conexión cerrada")
    else:
        print("\n❌ No se pudo establecer conexión")


# ===========================
# PLANTILLA PARA ACTUALIZAR OTROS SCRIPTS
# ===========================

"""
INSTRUCCIONES PARA ACTUALIZAR OTROS SCRIPTS:

1. Agregar el import al inicio del archivo:
   
   from config_db import DB_CONFIG, CONNECTION_CONFIG, TABLES, validate_config

2. Reemplazar las credenciales hardcodeadas:
   
   ANTES:
   conexion = mysql.connector.connect(
       host='localhost',
       database='astro',
       user='in4p',
       password='0000'
   )
   
   DESPUÉS:
   conexion = mysql.connector.connect(**DB_CONFIG)

3. Usar nombres de tabla desde la configuración:
   
   ANTES:
   query = "SELECT * FROM Meteoro"
   
   DESPUÉS:
   tabla_meteoro = TABLES.get('meteoro', 'Meteoro')
   query = f"SELECT * FROM {tabla_meteoro}"

4. Validar la configuración antes de conectar:
   
   config_valida, mensaje = validate_config()
   if not config_valida:
       print(f"Error: {mensaje}")
       return None

5. Usar los parámetros de reintentos si es necesario:
   
   max_intentos = CONNECTION_CONFIG.get('retry_attempts', 3)
   delay = CONNECTION_CONFIG.get('retry_delay', 5)
"""

if __name__ == "__main__":
    main()