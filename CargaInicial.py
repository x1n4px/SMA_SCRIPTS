# -*- coding: utf-8 -*-
import sys
import os
import pathlib

##########Conexion a la BD##############
import pyodbc as pyodbc
server = 'server' 
database = 'database'
username = 'usuario' 
password = 'password' 

try:
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password+';Connection Timeout =15')
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
                longitudrads = "NULL"
            latitudrads = observatorio[4]
            if latitudrads == "":
                latitudrads = "NULL"
            altitud = observatorio[5]
            if altitud == "":
                altitud = "NULL"
            dlocal = observatorio[6]
            if dlocal == "-1":
                dlocal = "No tiene"
            dnube = observatorio[7]
            if dnube == "-1":
                dnube = "No tiene"
            tchip = observatorio[8]
            ochip = observatorio[9]
            mascara = observatorio[10]
            if mascara == "-1":
                mascara = "No tiene"
            creditos = observatorio[11]
            if creditos == "-1":
                creditos = "No tiene"
            nombreObsv = observatorio[14]
            if nombreObsv == "-1":
                nombreObsv = "No tiene"
            activo = observatorio[15]



            #Creamos la sentencia que comprueba si existe lo que se va a añadir, para actualizarlo. Si no existe, lo crea  
            exists="IF EXISTS (SELECT * FROM Observatorio WHERE Número = "+str(numero)+")"
            update="\n\tUPDATE Observatorio"
            set="\n\tSET Nombre_Camara = '"+nombreCamara+"', Descripción = '"+descripcion+"', Longitud_Sexagesimal = '"+longitudsexagesimal+"', Latitud_Sexagesimal = '"+latitudsexagesimal+"', Longitud_Radianes = "+longitudrads+", Latitud_Radianes = "+latitudrads+", Altitud = "+altitud+", Directorio_Local = '"+dlocal+"', Directorio_Nube = '"+dnube+"', Tamaño_Chip = "+tchip+", Orientación_Chip = "+ochip+", Máscara = '"+mascara+"', Créditos = '"+creditos+"', Nombre_Observatorio = '"+nombreObsv+"', Activo = "+activo+""
            where="\n\tWHERE Número = "+str(numero)
            insert ="\nELSE\n\tINSERT INTO Observatorio (Número, Nombre_Camara, Descripción, Longitud_Sexagesimal, Latitud_Sexagesimal, Longitud_Radianes, Latitud_Radianes, Altitud, Directorio_Local, Directorio_Nube, Tamaño_Chip, Orientación_Chip, Máscara, Créditos, Nombre_Observatorio, Activo) values ("+str(numero)+",'"+nombreCamara+"','"+descripcion+"','"+longitudsexagesimal+"','"+latitudsexagesimal+"',"+longitudrads+","+latitudrads+","+altitud+",'"+dlocal+"','"+dnube+"',"+tchip+","+ochip+",'"+mascara+"','"+creditos+"','"+nombreObsv+"',"+activo+")"
            sentencia = exists+update+set+where+insert
            cursor.execute(sentencia)
            cursor.commit()


    def cargarLluvia(archivo,ruta):

        # Ya que vamos a añadir fechas, nos aseguramos de que se suben en formato correspondiente dd/mm/yyyy
        cursor.execute("SET dateformat dmy")

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
                        velocidad = "NULL"
                else:
                    velocidad = "NULL"

                #Creamos la sentencia que comprueba si existe lo que se va a añadir, para actualizarlo. Si no existe, lo crea  
                exists="IF EXISTS (SELECT * FROM Lluvia WHERE Identificador = '"+abreviatura+"' and Año = '"+year+"')"
                update="\n\tUPDATE Lluvia"
                set="\n\tSET Nombre ='"+nombre+"', Fecha_Inicio ='"+diaI+"-"+mesI+"-"+year+"', Fecha_Fin = '"+diaF+"-"+mesF+"-"+year+"', Velocidad = "+velocidad
                where="\n\tWHERE Identificador = '"+abreviatura+"' and Año = '"+year+"'"
                insert ="\nELSE\n\tINSERT INTO Lluvia (Identificador, Año, Nombre, Fecha_Inicio, Fecha_Fin, Velocidad) values (""'"+abreviatura+"',"+year+",'"+nombre+"','"+diaI+"-"+mesI+"-"+year+"','"+diaF+"-"+mesF+"-"+year+"',"+velocidad+")" 
                sentencia=exists+update+set+where+insert

                cursor.execute(sentencia)
                cursor.commit()

            elif len(dato) == 1:
                lluvia = dato[0]
            
            else:
                cont += 1
                mes = dato[0]
                dia = dato[1]
                ascension = dato[2]
                declinacion = dato[3]

                #Creamos la sentencia que comprueba si existe lo que se va a añadir, para actualizarlo. Si no existe, lo crea  
                exists="IF EXISTS (SELECT * FROM Seccion WHERE Identificador = '"+str(cont)+"' and Lluvia_Identificador = '"+lluvia+"' and Lluvia_Año = "+year+")"
                update="\n\tUPDATE Seccion"
                set="\n\tSET Fecha = '"+dia+"-"+mes+"-"+year+"', Ascensión_recta_del_radiante = '"+ascension+"', Declinación_del_radiante = '"+declinacion+"'"
                where="\n\tWHERE Identificador = '"+str(cont)+"' and Lluvia_Identificador = '"+lluvia+"' and Lluvia_Año = "+year+""
                insert ="\nELSE\n\tINSERT INTO Seccion (Identificador, Fecha, Ascensión_recta_del_radiante, Declinación_del_radiante, Lluvia_Identificador, Lluvia_Año) values (""'"+str(cont)+"','"+dia+"-"+mes+"-"+year+"','"+ascension+"','"+declinacion+"','"+lluvia+"',"+year+")"
                sentencia=exists+update+set+where+insert

                cursor.execute(sentencia)
                cursor.commit()

    directorio = input("Directorio donde se encuentran los archivos de los observatorios (No escribir ruta si no se quieren cargar): ")
    if directorio != "":
        cargarObservatorio(directorio)

    directorio = input("Directorio donde se encuentran los calendarios (No escribir ruta si no se quieren cargar): ")
    if directorio != "":
        with os.scandir(directorio) as itr:
            for entrada in itr:
                if entrada.name[:3] == "cal":
                    print("Procesando: " + entrada.name)
                    cargarLluvia(entrada.name,directorio)

    cursor.close()

except pyodbc.OperationalError as e:
    print('Error de conexión:', e)