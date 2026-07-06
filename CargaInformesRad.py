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


def normalize_trajectory_dir(ruta):
    ruta_path = pathlib.Path(ruta).expanduser().resolve()
    if ruta_path.name == "vm":
        return ruta_path.parent
    return ruta_path


def event_datetime_from_route(ruta):
    trajectory_dir = normalize_trajectory_dir(ruta)
    event_dir = trajectory_dir.parent
    fecha = f"{event_dir.parent.name[:4]}-{event_dir.parent.name[4:6]}-{event_dir.parent.name[6:8]}"
    hora = f"{event_dir.name[:2]}:{event_dir.name[2:4]}:{event_dir.name[4:6]}.0000"
    return fecha, hora


def time_to_seconds(hora):
    raw = str(hora).strip()
    if " " in raw:
        raw = raw.split(" ")[-1]
    if "." not in raw:
        raw = f"{raw}.0000"
    parts = raw.split(":")
    if len(parts) < 3:
        raise ValueError(f"Formato de hora inválido: {hora}")
    return Decimal(parts[0]) * Decimal("3600") + Decimal(parts[1]) * Decimal("60") + Decimal(parts[2])


cnxn = None
cursor = None

try:
    # Validar configuración
    config_valida, mensaje = validate_config()
    if not config_valida:
        print(f"❌ Error en configuración: {mensaje}")
        sys.exit(1)
    
    # Crear copia de configuración con autocommit
    config_con_autocommit = DB_CONFIG.copy()
    config_con_autocommit['autocommit'] = True  # Para que los INSERT se ejecuten automáticamente
    
    cnxn = mysql.connector.connect(**config_con_autocommit)
    cursor = cnxn.cursor()
    ##########Conexion a la BD##############

    def recorrerSubdirectorio(ruta):
        with os.scandir(ruta) as itr:
            for entrada in itr:
                if entrada.is_dir():
                    recorrerSubdirectorio(ruta + "/" + entrada.name)
                elif entrada.name[:16] == "Informe-Radiante" and entrada.name[-3:] == "inf":
                    procesaInforme(ruta, entrada.name)

    def procesaInforme(ruta, informe):
        lineasarchivo = []
        with open((ruta + "/" + informe), encoding="utf-8") as fname:
            for lineas in fname:
                lineasarchivo.append(lineas.strip('\n'))
        
        # Apuntamos la línea por la que vamos, para evitar problemas
        actual = 0

        # Sacamos la lluvia asociada
        idLluviaAsociada = informe[:-4][-4:]
        if idLluviaAsociada[0] == "-":
            idLluviaAsociada = idLluviaAsociada[-3:]
        if idLluviaAsociada == "SPO":
            idLluviaAsociada = "Ninguna"

        # Sacamos la fecha y hora del informe
        yearInforme = informe[17:][:4]
        mesInforme = informe[21:][:2]
        diaInforme = informe[23:][:2]
        horaInforme = informe[25:][:2]
        minutoInforme = informe[27:][:2]
        if(len(informe)>43):
            segundoInforme = informe[29:][:7]
        else:
            segundoInforme = informe[29:][:2] + ".0000"

        # Formato de fecha MySQL: YYYY-MM-DD
        fecha = yearInforme + "-" + mesInforme + "-" + diaInforme
        hora = horaInforme + ":" + minutoInforme + ":" + segundoInforme

        # Comprobamos si existe ya un informe en esta hora y dia
        infNuevo = True
        cursor.execute("SELECT Identificador, Fecha, Hora FROM Informe_Radiante")
        for i in cursor:
            if str(i[1]) == fecha and str(i[2]) == hora:
                infNuevo = False
                idInf = i[0]

        if infNuevo:
            cursor.execute("SELECT MAX(Identificador) FROM Informe_Radiante")
            resultado = cursor.fetchone()
            idInf = (resultado[0] + 1) if resultado[0] is not None else 1

        if infNuevo:
            if len(sys.argv) == 1:
                print(ruta + "/" + informe)
            # Comprobamos si existe un meteoro en la base de datos a esta fecha y hora, en caso contrario, lo añadimos
            event_fecha, event_hora = event_datetime_from_route(ruta)
            cursor.execute("SELECT Identificador, Fecha, Hora FROM Meteoro WHERE Fecha = %s", (event_fecha,))
            insertar = True
            target_seconds = time_to_seconds(event_hora)
            mejor_match = None
            for i in cursor:
                try:
                    delta = abs(time_to_seconds(i[2]) - target_seconds)
                except Exception:
                    continue
                if delta <= Decimal("3"):
                    if mejor_match is None or delta < mejor_match[0] or (delta == mejor_match[0] and i[0] < mejor_match[1]):
                        mejor_match = (delta, i[0])
            if mejor_match is not None:
                idM = mejor_match[1]
                insertar = False

            if insertar:
                cursor.execute("SELECT MAX(Identificador) FROM Meteoro")
                resultado = cursor.fetchone()
                idM = (resultado[0] + 1) if resultado[0] is not None else 1
                insert = "INSERT INTO Meteoro (Identificador, Fecha, Hora) VALUES (%s, %s, %s)"
                cursor.execute(insert, (idM, event_fecha, event_hora))

            # Sacamos el número del observatorio
            aux = lineasarchivo[actual].split(' ')
            obsv = aux[len(aux)-1][:2]
            if obsv[1] == ":":
                obsv = obsv[0]
                # Actualizamos actual
            actual = actual + 6

            # Sacamos las lluvias activas a la fecha
            lluvias = []
            while(lineasarchivo[actual] != ""):
                lluvias.append(lineasarchivo[actual])
                actual = actual + 1
                # Actualizamos actual
            actual = actual + 4

            # Sacamos las distancias minimas entre radiantes y trayectoria
            distMinLluvias = []
            while(lineasarchivo[actual] != ""):
                lluviaAct = []
                for i in lineasarchivo[actual].split(' '):
                    if i != "" and i != "|":
                        lluviaAct.append(i)
                distMinLluvias.append(lluviaAct)
                actual = actual+1
                # Actualizamos actual
            actual = actual + 1

            velocidadesAngulares = []
            trayectorias = []

            # Sacamos lluvia asociada al informe (Ya que alguna lluvia es mayor a una palabra, controlamos estas también)
            if lineasarchivo[actual][:2] == "No":
                lluviaAsociada = "Ninguna"

                cursor.execute("SELECT MAX(Identificador) FROM Informe_Radiante")
                resultado = cursor.fetchone()
                idInf = (resultado[0] + 1) if resultado[0] is not None else 1
                insert = "INSERT INTO Informe_Radiante (Identificador, Fecha, Hora, Velocidad_Lluvia_Asociada, Trayectorias_estimadas_para, Distancia_angular_radianes, Distancia_angular_grados, Velocidad_angular_grad_sec, Meteoro_Identificador, Observatorio_Número, Lluvia_Asociada) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(insert, (idInf, fecha, hora, None, 'No medido', None, None, None, idM, obsv, idLluviaAsociada))

                
            else:
                aux = lineasarchivo[actual].split(' ')
                lluviaAsociada = ""
                for i in range(0,len(aux)-11):
                    lluviaAsociada = lluviaAsociada + aux[5 + i] + " "
                # Una vez sacada la lluvia a la que se asocia, sacamos el identificador de la misma
                for i in range(len(lluvias)):
                    if lluvias[i] + " " == lluviaAsociada:
                        lluviaAsociada = distMinLluvias[i][0]
                    # Actualizamos actual
                velLluviaAsociada = aux[len(aux)-2]
                actual = actual + 3

                if lineasarchivo[actual].split(' ')[4] == "tiempo":
                    # Como tenemos lluvia asociada, sacamos las trayectorias estimadas
                    aux = lineasarchivo[actual].split(' ')
                    tiempoTrayectorias = "Un tiempo de " + aux[len(aux)-2]
                        # Actualizamos actual
                    actual = actual + 4

                    # Sacamos las trayectorias
                    while(lineasarchivo[actual] != ""):
                        trayec = []
                        for i in lineasarchivo[actual].split(' '):
                            if i != "" and i != "|":
                                trayec.append(i)
                        trayectorias.append(trayec)
                        actual = actual + 1
                        # Actualizamos actual
                    actual = actual + 1

                    # Sacamos la distancia angular
                    distAngular = []
                    aux = lineasarchivo[actual][62:].split(' ')
                    for i in aux:
                        if i != "" and i !="/":
                            distAngular.append(i)
                        # Actualizamos actual
                    actual = actual + 1

                    # Sacamos la velocidad angular
                    aux = lineasarchivo[actual].split(' ')
                    velAngular = aux[len(aux)-1]
                        # Actualizamos actual
                    actual = actual + 7

                    # Sacamos las velocidades angulares previstas
                    velocidadesAngulares = []
                    while(actual<len(lineasarchivo)):
                        vAng = []
                        aux = lineasarchivo[actual].split(' ')
                        for i in aux:
                            if i != "" and i != "|":
                                vAng.append(i)
                        velocidadesAngulares.append(vAng)
                        actual = actual + 1

                    #Añadimos los datos del informe
                    cursor.execute("SELECT MAX(Identificador) FROM Informe_Radiante")
                    resultado = cursor.fetchone()
                    idInf = (resultado[0] + 1) if resultado[0] is not None else 1
                    insert = "INSERT INTO Informe_Radiante (Identificador, Fecha, Hora, Velocidad_Lluvia_Asociada, Trayectorias_estimadas_para, Distancia_angular_radianes, Distancia_angular_grados, Velocidad_angular_grad_sec, Meteoro_Identificador, Observatorio_Número, Lluvia_Asociada) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(insert, (idInf, fecha, hora, str(velLluviaAsociada), str(tiempoTrayectorias), Decimal(distAngular[0]), Decimal(distAngular[1]), Decimal(velAngular), idM, obsv, idLluviaAsociada))

                    for i in trayectorias:
                        insert = "INSERT INTO Trayectoria_estimada (Velocidad, Lon_Inicio, Lat_Inicio, Alt_Inicio, Dist_Inicio, Lon_Final, Lat_Final, Alt_Final, Dist_Final, Recor, e, t, Informe_Radiante_Identificador) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        cursor.execute(insert, (Decimal(i[0]), i[1], i[2], Decimal(i[3]), Decimal(i[4]), i[5], i[6], Decimal(i[7]), Decimal(i[8]), Decimal(i[9]), None, None, idInf))
                
                else:
                    distAngular = "No medido"
                    velAngular = "No medido"
                    # Sacamos el rango de alturas
                    aux = lineasarchivo[actual].split(' ')
                    ranAlt = []
                    ranAlt.append(aux[len(aux)-2])
                    ranAlt.append(aux[len(aux)-1][:2])
                    tiempoTrayectorias = "Rango de alturas " + ranAlt[0] + " " + ranAlt[1]
                    # Actualizamos actual
                    actual = actual + 4


                    # Sacamos las trayectorias
                    while(lineasarchivo[actual] != ""):
                        trayec = []
                        for i in lineasarchivo[actual].split(' '):
                            if i != "" and i != "|":
                                trayec.append(i)
                        trayectorias.append(trayec)
                        actual = actual + 1
                        # Actualizamos actual
                    actual = actual + 4

                    # Sacamos las velocidades angulares
                    while(actual<len(lineasarchivo)):
                        vAng = []
                        aux = lineasarchivo[actual].split(' ')
                        for i in aux:
                            if i != "" and i != "|":
                                vAng.append(i)
                        velocidadesAngulares.append(vAng)
                        actual = actual + 1

                    #Añadimos los datos del informe
                    cursor.execute("SELECT MAX(Identificador) FROM Informe_Radiante")
                    resultado = cursor.fetchone()
                    idInf = (resultado[0] + 1) if resultado[0] is not None else 1
                    insert = "INSERT INTO Informe_Radiante (Identificador, Fecha, Hora, Velocidad_Lluvia_Asociada, Trayectorias_estimadas_para, Distancia_angular_radianes, Distancia_angular_grados, Velocidad_angular_grad_sec, Meteoro_Identificador, Observatorio_Número, Lluvia_Asociada) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(insert, (idInf, fecha, hora, str(velLluviaAsociada), str(tiempoTrayectorias), None, None, None, idM, obsv, idLluviaAsociada))

                    for i in trayectorias:
                        insert = "INSERT INTO Trayectoria_estimada (Velocidad, Lon_Inicio, Lat_Inicio, Alt_Inicio, Dist_Inicio, Lon_Final, Lat_Final, Alt_Final, Dist_Final, Recor, e, t, Informe_Radiante_Identificador) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        cursor.execute(insert, (None, i[0], i[1], Decimal(i[2]), Decimal(i[3]), i[4], i[5], Decimal(i[6]), Decimal(i[7]), None, Decimal(i[8]), Decimal(i[9]), idInf))

            for i in distMinLluvias:
                insert = "INSERT INTO Lluvia_Activa_InfRad (Ar_de_la_fecha, De_de_la_fecha, Ar_más_cercano, De_más_cercano, Distancia, Informe_Radiante_Identificador, Lluvia_Identificador, Lluvia_Año) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(insert, (Decimal(i[1]), Decimal(i[2]), Decimal(i[3]), Decimal(i[4]), Decimal(i[5]), idInf, i[0], int(yearInforme)))

            for i in velocidadesAngulares:
                insert = "INSERT INTO Velocidades_Angulares (hi, Lluvia, Meteoro, Informe_Radiante_Identificador) VALUES (%s, %s, %s, %s)"
                cursor.execute(insert, (Decimal(i[0]), Decimal(i[1]), Decimal(i[2]), idInf))



    if len(sys.argv) > 1:
        directorio = sys.argv[1]
        with os.scandir(directorio) as itr:
            for entrada in itr:
                if entrada.is_dir():
                    recorrerSubdirectorio(directorio + "/" + entrada.name)
                elif entrada.name[:16] == "Informe-Radiante" and entrada.name[-3:] == "inf":
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
                    elif entrada.name[:16] == "Informe-Radiante" and entrada.name[-3:] == "inf":
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
