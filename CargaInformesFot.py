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

    def recorrerSubdirectorio(ruta):
        with os.scandir(ruta) as itr:
            for entrada in itr:
                if entrada.is_dir():
                    recorrerSubdirectorio(ruta + "/" + entrada.name)
                elif entrada.name[:18] == "Informe-fotometria":
                    procesaInforme(ruta, entrada.name)

    def procesaInforme(ruta, informe):
        lineasarchivo = []
        with open((ruta + "/" + informe), encoding="utf-8") as fname:
            for lineas in fname:
                lineasarchivo.append(lineas.strip('\n'))

        # Apuntamos la línea por la que vamos, para evitar problemas
        actual = 0

        # Ya que vamos a añadir fechas, nos aseguramos de que se suben en formato correspondiente dd/mm/yyyy
        cursor.execute("SET dateformat dmy")

        # Sacamos la fecha y hora a través de la ruta del informe
        aux = ruta.split('/')
        fechaF = aux[len(aux)-3]
        if len(fechaF) > 8:
            fechaF = fechaF[-8:]
        horaF = aux[len(aux)-2]

        yearInforme = fechaF[:4]
        mesInforme = fechaF[4:][:2]
        diaInforme = fechaF[6:][:2]

        horaInforme = horaF[:2]
        minutoInforme = horaF[2:][:2]
        segundoInforme = horaF[4:][:2]

        fecha = diaInforme + "-" + mesInforme + "-" + yearInforme
        hora = horaInforme + ":" + minutoInforme + ":" + segundoInforme

        # Comprobamos si existe ya un informe en esta hora y dia
        infNuevo = True
        fechaComparar = yearInforme+"-"+mesInforme+"-"+diaInforme
        cursor.execute("SELECT Identificador, Fecha, Hora FROM Informe_Fotometria")
        for i in cursor:
            if str(i[1]) == fechaComparar and str(i[2]) == hora:
                infNuevo = False
                idInf = i[0]

        if infNuevo:
            cursor.execute("SELECT COUNT(*) FROM Informe_Fotometria")
            for i in cursor:
                idInf = i[0] + 1

        if infNuevo:
            if len(sys.argv) == 1:
                print(ruta + "/" + informe)

            # Comprobamos si existe un meteoro en la base de datos a esta fecha y hora, en caso contrario, lo añadimos
            cursor.execute("SELECT * FROM Meteoro")
            insertar = True
            for i in cursor:
                fechaBien =  str(i[1])[8:][:2] + "-" + str(i[1])[5:][:2] + "-" + str(i[1])[:4]
                if fecha == fechaBien:
                    if hora[:5] ==  i[2][:5]:
                        if float(i[2][6:]) < float(hora[6:]) and float(i[2][6:])+2 > float(hora[6:]):
                            idM = i[0]
                            insertar = False
                        elif float(i[2][6:]) == float(hora[6:]):
                            idM = i[0]
                            insertar = False
                        elif float(i[2][6:]) > float(hora[6:]) and float(i[2][6:])-2 < float(hora[6:]):
                            idM = i[0]
                            insertar = False

            if insertar:
                cursor.execute("SELECT * FROM Meteoro")
                idM = 1
                for i in cursor:
                    idM = i[0] + 1
                insert ="INSERT INTO Meteoro (Identificador, Fecha, Hora) values ("+str(idM)+",'"+fecha+"','"+hora+"')" 
                print(insert)
                cursor.execute(insert)
                cursor.commit()


            # Sacamos las estrellas visibles del catálogo Hipparcos
            aux = lineasarchivo[actual].split(':')
            estrellasVisibles = aux[1][1:]
                # Actualizamos actual
            actual = actual + 5

            # Sacamos las estrellas usadas para regresión
            estrellasRegresion = []    
            while(lineasarchivo[actual][:3] != "Núm"):
                estrella = []
                aux = lineasarchivo[actual].split(' ')
                for i in aux:
                    if i != "" and i != "|":
                        estrella.append(i)
                estrellasRegresion.append(estrella)
                actual = actual + 1
            
            # Seguidamente sacamos el número de estrellas usadas en la regresión
            aux = lineasarchivo[actual].split(':')
            numEstrellas = aux[1][1:]
                # Actualizamos actual
            actual = actual + 3

            # Sacamos el coef ext de la recta de Bouger
            aux = lineasarchivo[actual].split(' ')
            coefExtBoug = aux[len(aux)-1]
                # Actualizamos actual
            actual = actual + 1

            # Sacamos el punto cero de la recta de Bouger
            aux = lineasarchivo[actual].split(' ')
            puntoCeroBoug = aux[len(aux)-1]
                # Actualizamos actual
            actual = actual + 2

            # Sacamos el error típico de regresión
            aux = lineasarchivo[actual].split(' ')
            errorRegresion = aux[len(aux)-1]
                # Actualizamos actual
            actual = actual + 1

            # Sacamos el error típico del punto cero
            aux = lineasarchivo[actual].split(' ')
            errorPuntoCero = aux[len(aux)-1]
                # Actualizamos actual
            actual = actual + 1

            # Sacamos el error típico del coef ext
            aux = lineasarchivo[actual].split(' ')
            errorCoefExt = aux[len(aux)-1]
                # Actualizamos actual
            actual = actual + 6
            
            # Sacamos los datos introducidos del meteoro
            datosMeteoro = []
            aux = lineasarchivo[actual].split(' ')
            for i in aux:
                if i != "" and i != "|":
                    datosMeteoro.append(i)
                # Actualizamos actual
            actual = actual + 4

            # Sacamos los datos del ajuste de la trayectoria de la parábola
            aux = lineasarchivo[actual].split('=')
            a = aux[1]
            aux = lineasarchivo[actual+1].split('=')
            b = aux[1]
            aux = lineasarchivo[actual+2].split('=')
            c  = aux[1]
            coefParabola = a + " " + b + " " + c
                # Actualizamos actual
            actual = actual + 7

            # Sacamos los puntos del ajuste
            ajusteTrayectoria = []
            while(lineasarchivo[actual] != ""):
                aux = lineasarchivo[actual].split(' ')
                punto = []
                for i in aux:
                    if i != "" and i != "|":
                        punto.append(i)
                ajusteTrayectoria.append(punto)
                actual = actual + 1
                # Actualizamos actual
            actual = actual + 1

            # Sacamos MagMax y MagMin
            aux = lineasarchivo[actual].split(' ')
            magMax = aux[1]
            magMin = aux[len(aux)-1]
                # Actualizamos actual
            actual = actual + 2

            # Sacamos la masa fotométrica
            aux = lineasarchivo[actual].split(':')
            masaFotometrica = aux[len(aux)-1][1:]

            cursor.execute("SELECT COUNT(*) FROM Informe_Fotometria")
            for i in cursor:
                idInf = i[0] + 1
            insert ="INSERT INTO Informe_Fotometria (Identificador, Fecha, Hora, Estrellas_visibles, Estrellas_usadas_para_regresion, Coeficiente_externo_Recta_de_Bouger, Punto_cero_Recta_de_Bouger, Error_tipico_regresion, Error_tipico_punto_cero, Error_tipico_coeficiente_externo, Coeficientes_parabola_trayectoria, MagMax, MagMin, Masa_fotometrica, Meteoro_Identificador) values("+str(idInf)+",'"+fecha+"','"+hora+"',"+str(estrellasVisibles)+","+str(numEstrellas)+","+coefExtBoug+","+puntoCeroBoug+","+errorRegresion+","+errorPuntoCero+","+errorCoefExt+",'"+coefParabola+"',"+magMax+","+magMin+","+masaFotometrica+","+str(idM)+")" 
            cursor.execute(insert)
            cursor.commit()

            for i in estrellasRegresion:
                cursor.execute("SELECT COUNT(*) FROM Estrellas_usadas_para_regresión")
                for a in cursor:
                    idStar = a[0] + 1
                idEstrella = str(i[0]) + " " + str(i[1])
                insert = "INSERT INTO Estrellas_usadas_para_regresión (Identificador, Id_estrella, Masa_de_aire, Magnitud_de_catalogo, Magnitud_instrumental, Informe_Fotometria_Identificador) values ("+str(idStar)+",'"+idEstrella+"',"+i[2]+","+i[3]+","+i[4]+","+str(idInf)+")"
                cursor.execute(insert)
                cursor.commit()

            for i in ajusteTrayectoria:
                insert = "INSERT INTO Puntos_del_ajuste (t, Dist, Mc, Ma, Informe_Fotometria_Identificador) values ("+i[0]+","+i[1]+","+i[2]+","+i[3]+","+str(idInf)+")"
                cursor.execute(insert)
                cursor.commit()
            
            insert = "INSERT INTO Datos_meteoro_fotometria (X_Inicio, Y_Inicio, Maire_Inicio, distInicio, X_Final, Y_Final, Maire_Final, dist_Final, Informe_Fotometria_Identificador) values ("+datosMeteoro[0]+","+datosMeteoro[1]+","+datosMeteoro[2]+","+datosMeteoro[3]+","+datosMeteoro[4]+","+datosMeteoro[5]+","+datosMeteoro[6]+","+datosMeteoro[7]+","+str(idInf)+")"
            cursor.execute(insert)
            cursor.commit()


    if len(sys.argv) > 1:
        directorio = sys.argv[1]
        with os.scandir(directorio) as itr:
            for entrada in itr:
                if entrada.is_dir():
                    recorrerSubdirectorio(directorio + "/" + entrada.name)
                elif entrada.name[:18] == "Informe-fotometria":
                    procesaInforme(directorio, entrada.name)
    else:
        directorio = input("Directorio de los informes a cargar: ")
        print("Se cargarán los informes contenidos en el directorio ("+directorio+") y sus subdirectorios")
        sn = input("¿Continuar? (S/N): ")
        if sn == "S":
            with os.scandir(directorio) as itr:
                for entrada in itr:
                    if entrada.is_dir():
                        recorrerSubdirectorio(directorio + "/" + entrada.name)
                    elif entrada.name[:18] == "Informe-fotometria":
                        procesaInforme(directorio, entrada.name)

        cursor.close()
except pyodbc.OperationalError as e:
    print('Error de conexión:', e)