# -*- coding: utf-8 -*-
import sys
import os
import pathlib
from decimal import Decimal, getcontext

# Establecer precisión alta para manejar números muy pequeños
getcontext().prec = 50

##########Conexion a la BD##############
import mysql.connector
from mysql.connector import Error

# Importar configuración centralizada
try:
    from config_db import DB_CONFIG, CONNECTION_CONFIG, TABLES, validate_config, get_connection_string
except ImportError:
    print("❌ Error: No se pudo importar config_db.py")
    print("🔧 Asegúrate de que el archivo config_db.py existe en el directorio actual")
    sys.exit(1)

cnxn = None
cursor = None

try:
    # Validar configuración
    config_valida, mensaje = validate_config()
    if not config_valida:
        print(f"❌ Error en configuración: {mensaje}")
        sys.exit(1)
    
    # Crear copia de configuración con autocommit y timeout
    config_con_autocommit = DB_CONFIG.copy()
    config_con_autocommit['autocommit'] = True
    config_con_autocommit['connection_timeout'] = CONNECTION_CONFIG.get('connection_timeout', 30)
    
    # Conexión a MySQL
    cnxn = mysql.connector.connect(**config_con_autocommit)
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

        # Formato de fecha para MySQL: YYYY-MM-DD
        fecha = f"{yearInforme}-{mesInforme}-{diaInforme}"
        hora = f"{horaInforme}:{minutoInforme}:{segundoInforme}"

        # Comprobamos si existe ya un informe en esta hora y dia
        infNuevo = True
        cursor.execute("SELECT Identificador, Fecha, Hora FROM Informe_Fotometria WHERE Fecha = %s AND Hora = %s", (fecha, hora))
        resultado = cursor.fetchall()
        if resultado:
            infNuevo = False
            idInf = resultado[0][0]

        if infNuevo:
            cursor.execute("SELECT COUNT(*) FROM Informe_Fotometria")
            resultado = cursor.fetchone()
            idInf = resultado[0] + 1

        if infNuevo:
            if len(sys.argv) == 1:
                print(ruta + "/" + informe)

            # Comprobamos si existe un meteoro en la base de datos a esta fecha y hora, en caso contrario, lo añadimos
            cursor.execute("SELECT * FROM Meteoro WHERE Fecha = %s", (fecha,))
            meteoros = cursor.fetchall()
            insertar = True
            for meteoro in meteoros:
                if hora[:5] == meteoro[2][:5]:
                    hora_sec = Decimal(hora[6:])
                    meteoro_sec = Decimal(meteoro[2][6:])
                    if meteoro_sec < hora_sec and meteoro_sec + Decimal('2') > hora_sec:
                        idM = meteoro[0]
                        insertar = False
                    elif meteoro_sec == hora_sec:
                        idM = meteoro[0]
                        insertar = False
                    elif meteoro_sec > hora_sec and meteoro_sec - Decimal('2') < hora_sec:
                        idM = meteoro[0]
                        insertar = False

            if insertar:
                cursor.execute("SELECT MAX(Identificador) FROM Meteoro")
                resultado = cursor.fetchone()
                idM = 1 if resultado[0] is None else resultado[0] + 1
                insert = "INSERT INTO Meteoro (Identificador, Fecha, Hora) VALUES (%s, %s, %s)"
                cursor.execute(insert, (idM, fecha, hora))
                cnxn.commit()
                print(f"Meteoro insertado: ID={idM}, Fecha={fecha}, Hora={hora}")

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
            coefParabola = f"{a} {b} {c}"
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
            resultado = cursor.fetchone()
            idInf = resultado[0] + 1
            
            insert = """INSERT INTO Informe_Fotometria 
                       (Identificador, Fecha, Hora, Estrellas_visibles, 
                        Estrellas_usadas_para_regresion, Coeficiente_externo_Recta_de_Bouger, 
                        Punto_cero_Recta_de_Bouger, Error_tipico_regresion, 
                        Error_tipico_punto_cero, Error_tipico_coeficiente_externo, 
                        Coeficientes_parabola_trayectoria, MagMax, MagMin, 
                        Masa_fotometrica, Meteoro_Identificador) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            
            valores = (idInf, fecha, hora, int(estrellasVisibles), int(numEstrellas), 
                      Decimal(coefExtBoug), Decimal(puntoCeroBoug), Decimal(errorRegresion), 
                      Decimal(errorPuntoCero), Decimal(errorCoefExt), coefParabola, 
                      Decimal(magMax), Decimal(magMin), Decimal(masaFotometrica), idM)
            
            cursor.execute(insert, valores)
            cnxn.commit()

            for i in estrellasRegresion:
                cursor.execute("SELECT COUNT(*) FROM Estrellas_usadas_para_regresión")
                resultado = cursor.fetchone()
                idStar = resultado[0] + 1
                idEstrella = f"{i[0]} {i[1]}"
                insert = """INSERT INTO Estrellas_usadas_para_regresión 
                           (Identificador, Id_estrella, Masa_de_aire, Magnitud_de_catalogo, 
                            Magnitud_instrumental, Informe_Fotometria_Identificador) 
                           VALUES (%s, %s, %s, %s, %s, %s)"""
                valores = (idStar, idEstrella, Decimal(i[2]), Decimal(i[3]), Decimal(i[4]), idInf)
                cursor.execute(insert, valores)
                cnxn.commit()

            for i in ajusteTrayectoria:
                insert = """INSERT INTO Puntos_del_ajuste 
                           (t, Dist, Mc, Ma, Informe_Fotometria_Identificador) 
                           VALUES (%s, %s, %s, %s, %s)"""
                valores = (Decimal(i[0]), Decimal(i[1]), Decimal(i[2]), Decimal(i[3]), idInf)
                cursor.execute(insert, valores)
                cnxn.commit()
            
            insert = """INSERT INTO Datos_meteoro_fotometria 
                       (X_Inicio, Y_Inicio, Maire_Inicio, distInicio, X_Final, 
                        Y_Final, Maire_Final, dist_Final, Informe_Fotometria_Identificador) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            valores = (Decimal(datosMeteoro[0]), Decimal(datosMeteoro[1]), Decimal(datosMeteoro[2]), 
                      Decimal(datosMeteoro[3]), Decimal(datosMeteoro[4]), Decimal(datosMeteoro[5]), 
                      Decimal(datosMeteoro[6]), Decimal(datosMeteoro[7]), idInf)
            cursor.execute(insert, valores)
            cnxn.commit()


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
        print(f"Se cargarán los informes contenidos en el directorio ({directorio}) y sus subdirectorios")
        sn = input("¿Continuar? (S/N): ")
        if sn == "S":
            with os.scandir(directorio) as itr:
                for entrada in itr:
                    if entrada.is_dir():
                        recorrerSubdirectorio(directorio + "/" + entrada.name)
                    elif entrada.name[:18] == "Informe-fotometria":
                        procesaInforme(directorio, entrada.name)

    if cursor:
        cursor.close()
    if cnxn:
        cnxn.close()
    
except mysql.connector.Error as e:
    print('Error de conexión MySQL:', e)
    sys.exit(1)
except Exception as e:
    print('Error general:', e)
    sys.exit(1)
finally:
    try:
        if cnxn and cnxn.is_connected():
            if cursor:
                cursor.close()
            cnxn.close()
    except:
        pass
