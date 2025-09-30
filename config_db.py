#!/usr/bin/env python3
"""
Archivo de configuración centralizado para la conexión a la base de datos MySQL.
Este archivo contiene todos los parámetros necesarios para conectarse a la base de datos
y es utilizado por todos los scripts del sistema.
"""

# ===========================
# CONFIGURACIÓN DE BASE DE DATOS
# ===========================

DB_CONFIG = {
    'host': 'localhost',        # Servidor de base de datos
    'database': 'astro',         # Nombre de la base de datos
    'user': 'in4p',              # Usuario de MySQL
    'password': '0000',          # Contraseña de MySQL
    'port': 3306,                # Puerto de MySQL (por defecto 3306)
    'charset': 'utf8mb4',        # Conjunto de caracteres
    'use_unicode': True,         # Usar Unicode
    'autocommit': False,         # Autocommit deshabilitado por defecto
    'raise_on_warnings': True,   # Lanzar excepciones en warnings
}

# ===========================
# CONFIGURACIÓN DE CONEXIÓN
# ===========================

CONNECTION_CONFIG = {
    'connection_timeout': 30,    # Timeout de conexión en segundos
    'retry_attempts': 3,         # Número de intentos de reconexión
    'retry_delay': 5,           # Segundos entre intentos de reconexión
}

# ===========================
# CONFIGURACIÓN DE LOGGING
# ===========================

LOGGING_CONFIG = {
    'log_queries': False,        # Registrar consultas SQL
    'log_file': 'database.log',  # Archivo de log
    'log_level': 'INFO',        # Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
}

# ===========================
# TABLAS DE LA BASE DE DATOS
# ===========================

TABLES = {
    'meteoro': 'Meteoro',
    'observatorio': 'Observatorio',
    'lluvia': 'Lluvia',
    'radiante': 'Radiante',
    'fotometria': 'Fotometria',
    # Agregar más tablas según sea necesario
}

# ===========================
# FUNCIONES AUXILIARES
# ===========================

def get_db_config():
    """
    Retorna la configuración completa de la base de datos.
    
    Returns:
        dict: Diccionario con la configuración de la base de datos
    """
    return DB_CONFIG.copy()

def get_connection_string():
    """
    Genera una cadena de conexión para mostrar (sin la contraseña).
    
    Returns:
        str: Cadena de conexión formateada
    """
    return f"mysql://{DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

def validate_config():
    """
    Valida que todos los campos requeridos estén presentes en la configuración.
    
    Returns:
        tuple: (bool, str) - (True si es válido, mensaje de error si hay problemas)
    """
    required_fields = ['host', 'database', 'user', 'password']
    missing_fields = []
    
    for field in required_fields:
        if field not in DB_CONFIG or DB_CONFIG[field] is None:
            missing_fields.append(field)
    
    if missing_fields:
        return False, f"Campos faltantes en la configuración: {', '.join(missing_fields)}"
    
    return True, "Configuración válida"

# ===========================
# CONFIGURACIÓN DE DESARROLLO/PRODUCCIÓN
# ===========================

# Puedes descomentar y modificar según el entorno
# import os
# ENVIRONMENT = os.environ.get('ENV', 'development')
# 
# if ENVIRONMENT == 'production':
#     DB_CONFIG['host'] = 'servidor-produccion.ejemplo.com'
#     DB_CONFIG['password'] = os.environ.get('DB_PASSWORD', '')
# elif ENVIRONMENT == 'development':
#     # Configuración actual es para desarrollo
#     pass