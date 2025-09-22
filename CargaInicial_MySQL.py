# -*- coding: utf-8 -*-
import sys
import os
import pathlib

##########Conexion a la BD##############
import mysql.connector

# Configuración de conexión MySQL
server = 'localhost'  # o la IP del servidor MySQL
database = 'database'
username = 'usuario' 
password = 'password'
port = 3306  # Puerto por defecto de MySQL

try:
    # Conexión a MySQL
    cnxn = mysql.connector.connect(
        host=server,
        port=port,
        database=database,
        user=username,
        password=password,
        connection_timeout=15
    )
    cursor = cnxn.cursor()
    ##########Conexion a la BD##############

    def cargarObservatorio(ruta):
        obsAstrometria = []
        datosAstrometria = []
        # Nombre
        # Longitud 1
        # Longitud 2
        # Longitud 3
        # Latitud 1
        # Latitud 2
        # Latitud 3
        # Chip
        # Orientación


        obsMeteoros = []
        datosMeteoros = []
        # Número
        # Latitud Radianes
        # Longitud Radianes
        # Altitud


        obsDescripcion = []
        datosDescripcion = []
        # Número
        # Nombre
        # Descripción
        # Directorio local
        # Directiorio nube
        # Máscara
        # Créditos

        observatorios = []
        # Número
        # Nombre
        # Descripción
        # Longitud
        # Latitud
        # Altitud
        # Directorio Local
        # Directorio Nube
        # Tamaño Chip
        # Orientación Chip
        # Máscara
        # Créditos

        ########## Datos OBSERVATORIOS-ASTROMETRIA ##########
        with open(ruta + "/" + "observatorios-astrometria", encoding="utf-8") as fname:
            for lineas in fname:
                obsAstrometria.append(lineas.strip('\n'))

        for frase in obsAstrometria:
            datosAstrometria.append((frase[:10].strip() +" "+ frase[10:]).split(" "))
            
        cont = 1
        for frase in datosAstrometria:
            inicial = [cont, frase[0], "", "", "", "", "", "", frase[7], frase[8], "", "", frase[1]+" "+frase[2]+" "+frase[3], frase[4]+" "+frase[5]+" "+frase[6], "", ""]
            cont += 1
            observatorios.append(inicial)


        ########## Datos OBSERVATORIOS-METEOROS ##########
        with open(ruta + "/" + "observatorios-meteoros", encoding="utf-8") as fname:
            for lineas in fname:
                obsMeteoros.append(lineas.strip('\n'))

        for frase in obsMeteoros:
            datosMeteoros.append(frase.split(":"))

        for frase in datosMeteoros:
            observatorios[int(frase[0])-1][3] = frase[1]
            observatorios[int(frase[0])-1][4] = frase[2]
            observatorios[int(frase[0])-1][5] = frase[3]
            #Update a los observatorios indicados con los datos

        ########## Datos DESCRIPCIÓN-OBSERVATORIOS ##########
        with open(ruta + "/" + "descripcion-observatorios.csv", encoding="utf-8") as fname:
            for lineas in fname:
                obsDescripcion.append(lineas.strip('\n'))

        for frase in obsDescripcion:
            datosDescripcion.append(frase.split(":"))

        datosDescripcion.pop(0)
        for frase in datosDescripcion:
            observatorios[int(frase[0])-1][2] = frase[2]
            observatorios[int(frase[0])-1][6] = frase[3]
            observatorios[int(frase[0])-1][7] = frase[4]
            observatorios[int(frase[0])-1][10] = frase[5]
            observatorios[int(frase[0])-1][11] = frase[6]
            observatorios[int(frase[0])-1][14] = frase[7]
            observatorios[int(frase[0])-1][15] = frase[8]




        #Añadimos los observatorios a la BD
        for observatorio in observatorios:

            #Comprobamos los datos que pueden ser NULL para evitar problemas
            numero = observatorio[0]
            nombreCamara = observatorio[1]
            if nombreCamara == "-1":
                nombreCamara = "No tiene"
            descripcion = observatorio[2]
            if descripcion == "-1":
                descripcion = "No tiene"
            longitudsexagesimal = observatorio[12]
            latitudsexagesimal = observatorio[13]
            longitudrads = observatorio[3]
            if longitudrads == "":
                longitudrads = None
            else:
                longitudrads = float(longitudrads)
            latitudrads = observatorio[4]
            if latitudrads == "":
                latitudrads = None
            else:
                latitudrads = float(latitudrads)
            altitud = observatorio[5]
            if altitud == "":
                altitud = None
            else:
                altitud = float(altitud)
            dlocal = observatorio[6]
            if dlocal == "-1":
                dlocal = "No tiene"
            dnube = observatorio[7]
            if dnube == "-1":
                dnube = "No tiene"
            tchip = float(observatorio[8])
            ochip = float(observatorio[9])
            mascara = observatorio[10]
            if mascara == "-1":
                mascara = "No tiene"
            creditos = observatorio[11]
            if creditos == "-1":
                creditos = "No tiene"
            nombreObsv = observatorio[14]
            if nombreObsv == "-1":
                nombreObsv = "No tiene"
            activo = int(observatorio[15])

            # Usando REPLACE INTO para MySQL (equivalente a IF EXISTS UPDATE ELSE INSERT)
            sentencia = """
                REPLACE INTO Observatorio 
                (Número, Nombre_Camara, Descripción, Longitud_Sexagesimal, 
                 Latitud_Sexagesimal, Longitud_Radianes, Latitud_Radianes, 
                 Altitud, Directorio_Local, Directorio_Nube, Tamaño_Chip, 
                 Orientación_Chip, Máscara, Créditos, Nombre_Observatorio, Activo) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            valores = (numero, nombreCamara, descripcion, longitudsexagesimal, 
                      latitudsexagesimal, longitudrads, latitudrads, altitud, 
                      dlocal, dnube, tchip, ochip, mascara, creditos, 
                      nombreObsv, activo)
            
            cursor.execute(sentencia, valores)
            cnxn.commit()


    def cargarLluvia(archivo, ruta):
        year = archivo[3:]
        cont = 0
        datos = []
        frasesbien = []
        separador = ":"
        with open(ruta + "/" + archivo, encoding="utf-8") as fname:
            for lineas in fname:
                datos.append(lineas.strip('\n'))

        for frase in datos:
            frasesbien.append(frase.split(separador))

        for dato in frasesbien:
            if dato[0] == "*":
                mesI = dato[1]
                diaI = dato[2]
                mesF = dato[3]
                diaF = dato[4]
                abreviatura = dato[5]
                nombre = dato[6]
                if len(dato) == 9:
                    velocidad = dato[8]
                    if velocidad == "":
                        velocidad = None
                    else:
                        velocidad = float(velocidad)
                else:
                    velocidad = None

                # Formato de fecha para MySQL: YYYY-MM-DD
                fecha_inicio = f"{year}-{mesI}-{diaI}"
                fecha_fin = f"{year}-{mesF}-{diaF}"

                # Usando REPLACE INTO para MySQL
                sentencia = """
                    REPLACE INTO Lluvia 
                    (Identificador, Año, Nombre, Fecha_Inicio, Fecha_Fin, Velocidad) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                valores = (abreviatura, year, nombre, fecha_inicio, fecha_fin, velocidad)
                
                cursor.execute(sentencia, valores)
                cnxn.commit()

            elif len(dato) == 1:
                lluvia = dato[0]
            
            else:
                cont += 1
                mes = dato[0]
                dia = dato[1]
                ascension = dato[2]
                declinacion = dato[3]

                # Formato de fecha para MySQL: YYYY-MM-DD
                fecha = f"{year}-{mes}-{dia}"

                # Usando REPLACE INTO para MySQL
                sentencia = """
                    REPLACE INTO Seccion 
                    (Identificador, Fecha, Ascensión_recta_del_radiante, 
                     Declinación_del_radiante, Lluvia_Identificador, Lluvia_Año) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                valores = (str(cont), fecha, ascension, declinacion, lluvia, year)
                
                cursor.execute(sentencia, valores)
                cnxn.commit()

    directorio = input("Directorio donde se encuentran los archivos de los observatorios (No escribir ruta si no se quieren cargar): ")
    if directorio != "":
        cargarObservatorio(directorio)

    directorio = input("Directorio donde se encuentran los calendarios (No escribir ruta si no se quieren cargar): ")
    if directorio != "":
        with os.scandir(directorio) as itr:
            for entrada in itr:
                if entrada.name[:3] == "cal":
                    print("Procesando: " + entrada.name)
                    cargarLluvia(entrada.name, directorio)

    cursor.close()
    cnxn.close()

except mysql.connector.Error as e:
    print('Error de conexión MySQL:', e)