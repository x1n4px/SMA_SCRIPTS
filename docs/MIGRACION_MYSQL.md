# Guía de Migración de Scripts de SQL Server a MySQL

## Cambios Realizados

### 1. Biblioteca de Conexión
- **Antes (SQL Server):** `import pyodbc`
- **Después (MySQL):** `import mysql.connector`

### 2. Cadena de Conexión

**SQL Server:**
```python
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password+';Connection Timeout =15')
```

**MySQL:**
```python
cnxn = mysql.connector.connect(
    host=server,
    port=port,
    database=database,
    user=username,
    password=password,
    connection_timeout=15
)
```

### 3. Sintaxis SQL Específicas

#### 3.1 Formato de Fechas
- **SQL Server:** `SET dateformat dmy` 
- **MySQL:** No es necesario, MySQL acepta formato 'YYYY-MM-DD' directamente
- **Solución:** Convertir fechas al formato ISO: `f"{year}-{month}-{day}"`

#### 3.2 IF EXISTS ... ELSE INSERT
- **SQL Server:** 
```sql
IF EXISTS (SELECT * FROM tabla WHERE condicion)
    UPDATE tabla SET ...
ELSE
    INSERT INTO tabla ...
```

- **MySQL - Opción 1:** Usar `REPLACE INTO`
```sql
REPLACE INTO tabla (columnas) VALUES (valores)
```

- **MySQL - Opción 2:** Usar `INSERT ... ON DUPLICATE KEY UPDATE`
```sql
INSERT INTO tabla (columnas) VALUES (valores)
ON DUPLICATE KEY UPDATE columna1=valor1, columna2=valor2
```

#### 3.3 Manejo de NULL
- **SQL Server:** Puede usar 'NULL' como string en concatenación
- **MySQL:** Usar `None` en Python y placeholders `%s`

### 4. Manejo de Cursores y Resultados

**SQL Server:**
```python
cursor.execute("SELECT * FROM tabla")
for row in cursor:
    # procesar row
```

**MySQL:**
```python
cursor.execute("SELECT * FROM tabla")
resultado = cursor.fetchall()  # o fetchone() para un solo registro
for row in resultado:
    # procesar row
```

### 5. Consultas Parametrizadas

**SQL Server:**
```python
sentencia = "INSERT INTO tabla VALUES (" + valor1 + "," + valor2 + ")"
cursor.execute(sentencia)
```

**MySQL (Más seguro):**
```python
sentencia = "INSERT INTO tabla VALUES (%s, %s)"
valores = (valor1, valor2)
cursor.execute(sentencia, valores)
```

### 6. Tipos de Datos
- Los valores numéricos deben convertirse explícitamente con `int()`, `float()`
- Los valores NULL deben ser `None` en Python, no strings 'NULL'

## Scripts Migrados

1. **CargaInicial_MySQL.py**
   - Carga observatorios y lluvias de meteoros
   - Usa REPLACE INTO para actualizar o insertar

2. **CargaInformesFot_MySQL.py**
   - Procesa informes fotométricos
   - Maneja fechas en formato ISO
   - Usa consultas parametrizadas

3. **CargaInformesRad_MySQL.py**
   - Procesa informes de radiante
   - Similar estructura a CargaInformesFot

4. **CargaInformesZ_MySQL.py**
   - Procesa informes Z
   - El más complejo de los scripts

## Requisitos

### Instalación de la biblioteca MySQL:
```bash
pip install mysql-connector-python
```

### Configuración de la Base de Datos
Actualizar las variables de conexión en cada script:
```python
server = 'localhost'  # o IP del servidor MySQL
database = 'nombre_bd'
username = 'usuario'
password = 'contraseña'
port = 3306  # Puerto por defecto de MySQL
```

## Notas Importantes

1. **Claves Primarias:** Asegurarse de que las tablas en MySQL tengan definidas las claves primarias correctas para que REPLACE INTO funcione adecuadamente.

2. **Codificación:** MySQL debe estar configurado con UTF-8 para manejar caracteres especiales correctamente.

3. **Transacciones:** Los scripts usan `commit()` después de cada inserción. Para mejor rendimiento, considerar hacer commit por lotes.

4. **Índices:** Crear índices apropiados en MySQL para mejorar el rendimiento de las consultas SELECT.

5. **Validación:** Siempre probar los scripts con datos de prueba antes de ejecutar en producción.

## Ejemplo de Creación de Tabla en MySQL

```sql
CREATE TABLE Observatorio (
    Número INT PRIMARY KEY,
    Nombre_Camara VARCHAR(255),
    Descripción TEXT,
    Longitud_Sexagesimal VARCHAR(100),
    Latitud_Sexagesimal VARCHAR(100),
    Longitud_Radianes DECIMAL(10,6),
    Latitud_Radianes DECIMAL(10,6),
    Altitud DECIMAL(10,2),
    Directorio_Local VARCHAR(255),
    Directorio_Nube VARCHAR(255),
    Tamaño_Chip DECIMAL(10,2),
    Orientación_Chip DECIMAL(10,2),
    Máscara VARCHAR(255),
    Créditos TEXT,
    Nombre_Observatorio VARCHAR(255),
    Activo TINYINT(1)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

## Solución de Problemas Comunes

1. **Error de conexión:** Verificar que MySQL esté ejecutándose y los datos de conexión sean correctos.

2. **Error de codificación:** Asegurar que la base de datos y las tablas usen UTF-8.

3. **Error de tipos de datos:** Verificar que los valores NULL se manejen como `None` en Python.

4. **Error de claves duplicadas:** Revisar que las claves primarias estén correctamente definidas.