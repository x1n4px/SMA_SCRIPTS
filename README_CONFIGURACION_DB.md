# 📋 Configuración Centralizada de Base de Datos

## ✅ Resumen de Cambios Realizados

Se ha implementado un sistema de configuración centralizada para la conexión a la base de datos MySQL. Todos los scripts ahora leen la configuración desde un único archivo `config_db.py`.

## 📁 Archivos Modificados

### 1. **Archivo de Configuración Central**
- ✅ `config_db.py` - **NUEVO** - Contiene toda la configuración de base de datos

### 2. **Scripts Actualizados**
- ✅ `leer_meteoros.py` - Script principal de lectura
- ✅ `CargaInformesZ.py` - Carga de informes Z
- ✅ `CargaInformesRad.py` - Carga de informes de radiante
- ✅ `CargaInformesFot_MySQL.py` - Carga de informes de fotometría
- ✅ `CargaInicial_MySQL.py` - Carga inicial de datos
- ✅ `ProcesadorInformes_MySQL.py` - Procesador unificado de informes
- ✅ `leer_meteoros_todos.py` - Script automatizado para cron

### 3. **Archivos de Ayuda**
- ✅ `ejemplo_uso_config_db.py` - Ejemplo de uso de la configuración
- ✅ `README_CONFIGURACION_DB.md` - Este archivo de documentación

## 🔧 Configuración

### Archivo `config_db.py`

El archivo contiene las siguientes secciones:

```python
# Configuración de Base de Datos
DB_CONFIG = {
    'host': 'localhost',        # Servidor MySQL
    'database': 'astro',         # Nombre de la base de datos
    'user': 'in4p',              # Usuario
    'password': '0000',          # Contraseña
    'port': 3306,                # Puerto
    'charset': 'utf8mb4',        # Conjunto de caracteres
    'use_unicode': True,         # Usar Unicode
    'autocommit': False,         # Autocommit
    'raise_on_warnings': True    # Lanzar excepciones en warnings
}

# Configuración de Conexión
CONNECTION_CONFIG = {
    'connection_timeout': 30,    # Timeout en segundos
    'retry_attempts': 3,         # Número de reintentos
    'retry_delay': 5            # Segundos entre reintentos
}

# Tablas de la Base de Datos
TABLES = {
    'meteoro': 'Meteoro',
    'observatorio': 'Observatorio',
    'lluvia': 'Lluvia',
    'radiante': 'Radiante',
    'fotometria': 'Fotometria'
}
```

## 🚀 Ventajas de la Configuración Centralizada

1. **🔐 Seguridad**: Credenciales en un solo lugar
2. **🔧 Mantenimiento**: Cambios en un solo archivo afectan a todos los scripts
3. **🔄 Reintentos automáticos**: Manejo robusto de conexiones
4. **✅ Validación**: Verifica configuración antes de conectar
5. **📊 Consistencia**: Todos los scripts usan la misma configuración
6. **🌍 Entornos múltiples**: Fácil cambio entre desarrollo/producción

## 📝 Cómo Usar

### 1. Para Ejecutar Scripts

Los scripts funcionan igual que antes, pero ahora usan la configuración centralizada:

```bash
# Ejecutar script principal
python3 leer_meteoros.py

# Con ruta personalizada
python3 leer_meteoros.py /ruta/a/detecciones

# Script automatizado
python3 leer_meteoros_todos.py
```

### 2. Para Cambiar Credenciales

Editar únicamente el archivo `config_db.py`:

```python
DB_CONFIG = {
    'host': 'nuevo_servidor',
    'database': 'nueva_bd',
    'user': 'nuevo_usuario',
    'password': 'nueva_contraseña',
    ...
}
```

### 3. Para Ambientes de Producción

Puedes usar variables de entorno editando `config_db.py`:

```python
import os

# Para producción
if os.environ.get('ENV') == 'production':
    DB_CONFIG['host'] = os.environ.get('DB_HOST')
    DB_CONFIG['password'] = os.environ.get('DB_PASSWORD')
```

## 🔍 Características Implementadas

### Reintentos Automáticos
- Los scripts intentan conectarse hasta 3 veces
- Espera 5 segundos entre intentos
- Configurable en `CONNECTION_CONFIG`

### Validación de Configuración
- Verifica que todos los campos requeridos estén presentes
- Muestra mensajes de error descriptivos

### Logging Mejorado
- Muestra información de conexión
- Indica versión del servidor MySQL
- Registra intentos de conexión

## ⚠️ Consideraciones de Seguridad

### Para Producción:

1. **Excluir del control de versiones**:
   ```bash
   echo "config_db.py" >> .gitignore
   ```

2. **Crear archivo de ejemplo**:
   ```bash
   cp config_db.py config_db.example.py
   # Editar config_db.example.py y poner valores de ejemplo
   ```

3. **Usar variables de entorno**:
   ```bash
   export DB_PASSWORD="contraseña_segura"
   ```

4. **Permisos de archivo**:
   ```bash
   chmod 600 config_db.py  # Solo lectura/escritura para el propietario
   ```

## 📊 Verificación de Funcionamiento

Para verificar que todo funciona correctamente:

```bash
# Probar el ejemplo
python3 ejemplo_uso_config_db.py

# Verificar conexión con el script principal
python3 leer_meteoros.py
```

## 🐛 Solución de Problemas

### Error: "No se pudo importar config_db.py"
- Verificar que `config_db.py` existe en el directorio de scripts
- Verificar permisos de lectura del archivo

### Error: "Error en configuración"
- Revisar que todos los campos en `DB_CONFIG` estén completos
- Verificar credenciales de MySQL

### Error de conexión después de reintentos
- Verificar que MySQL esté ejecutándose
- Verificar credenciales
- Verificar firewall/red

## 📚 Documentación de Funciones Auxiliares

### `validate_config()`
Valida que la configuración tenga todos los campos requeridos.

### `get_connection_string()`
Genera una cadena de conexión para logging (sin contraseña).

### `get_db_config()`
Retorna una copia de la configuración de base de datos.

## 🎯 Próximos Pasos Recomendados

1. **Hacer backup de la configuración actual**
2. **Probar en entorno de desarrollo**
3. **Configurar variables de entorno para producción**
4. **Actualizar documentación del proyecto**
5. **Capacitar al equipo en el nuevo sistema**

## 📞 Soporte

Si encuentras algún problema con la configuración centralizada:
1. Revisa este README
2. Consulta el archivo `ejemplo_uso_config_db.py`
3. Verifica los logs de error de los scripts

---

**Última actualización**: 2025-09-30
**Versión**: 1.0.0