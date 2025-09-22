# -*- coding: utf-8 -*-
import sys
import os
import math
import pathlib

##########Conexion a la BD##############
import mysql.connector
from mysql.connector import Error

server = 'localhost'  # o tu servidor MySQL
database = 'database'
username = 'usuario' 
password = 'password'
port = 3306  # puerto por defecto de MySQL

try:
    cnxn = mysql.connector.connect(
        host=server,
        database=database,
        user=username,
        password=password,
        port=port,
        autocommit=True  # Para que los INSERT se ejecuten automáticamente
    )
    cursor = cnxn.cursor()
    ##########Conexion a la BD##############

    def recorrerSubdirectorio(ruta):
        with os.scandir(ruta) as itr:
            for entrada in itr:
                if entrada.is_dir():
                    recorrerSubdirectorio(ruta + "/" + entrada.name)
                elif entrada.name[:9] == "Informe-Z" and entrada.name[-3:] != "kml":
                    procesaInforme(ruta, entrada.name)

    def procesaInforme(ruta, informe):
        lineasarchivo = []
        with open((ruta + "/" + informe), encoding="utf-8") as fname:
            for lineas in fname:
                lineasarchivo.append(lineas.strip('\n'))	

        # Apuntamos la línea por la que vamos, para evitar problemas
        actual = 0

        # Sacamos el tamaño del informe para saber hasta donde avanzar (por precaución)
        tamInforme = len(lineasarchivo)

        # Sacamos si los Elementos Orbitales se han calculado con la velocidad inicial o la velocidad media
        aux = ruta.split("/")
        if aux[len(aux)-1] == "vm":
            calcCon = "vm"
        else:
            calcCon = "vi"

        if tamInforme > 7:
            # Sacamos la fecha del informe
            yearInforme = lineasarchivo[actual][:4]
            mesInforme = lineasarchivo[actual][5:][:2]
            diaInforme = lineasarchivo[actual][8:][:2]
            horaInforme = lineasarchivo[actual][11:][:2]
            minutoInforme = lineasarchivo[actual][14:][:2]
            segundoInforme = lineasarchivo[actual][17:][:7]
            # Formato de fecha MySQL: YYYY-MM-DD
            fecha = yearInforme + "-" + mesInforme + "-" + diaInforme
            hora = horaInforme + ":" + minutoInforme + ":" + segundoInforme
                # Actualizamos actual 
            actual = actual + 1

            # Comprobamos si existe ya un informe en esta hora y dia
            infNuevo = True
            cursor.execute("SELECT IdInforme, Fecha, Hora FROM Informe_Z")
            for i in cursor:
                if str(i[1]) == fecha and str(i[2]) == hora:
                    infNuevo = False
                    idInf = i[0]

            if infNuevo:
                if len(sys.argv) == 1:
                    print(ruta + "/" + informe)
                cursor.execute("SELECT COUNT(*) FROM Informe_Z")
                for i in cursor:
                    idInf = i[0] + 1

            # Sacamos valores de la estación 1
            aux = lineasarchivo[actual][40:].split(":")
            estacion1 = []
            estacion1.append(aux[0]) # Numero

            aux = (aux[1].split(' '))
            estacion1.append(aux[1]) # Longitud
            estacion1.append(aux[2]) # Latitud

            aux = lineasarchivo[actual+3].split(' ')
            estacion1.append(aux[1]) # xi
            estacion1.append(aux[3]) # eta
            estacion1.append(aux[5]) # zeta


            # Sacamos valores de la estación 2
            aux = lineasarchivo[actual+1][40:].split(":")
            estacion2 = []
            estacion2.append(aux[0]) # Numero

            aux = (aux[1].split(' '))
            estacion2.append(aux[1]) # Longitud
            estacion2.append(aux[2]) # Latitud

            aux = lineasarchivo[actual+5].split(' ')
            estacion2.append(aux[1]) # xi
            estacion2.append(aux[3]) # eta
            estacion2.append(aux[5]) # zeta
                # Actualizamos actual 
            actual = actual + 6
        else:
            infNuevo = False
            actual = tamInforme+1

        # Sacamos el error cuadrático de la estacion 1
        if actual < tamInforme:
            errorCuadEst1 = ""
            while lineasarchivo[actual] != "":
                errorCuadEst1 = lineasarchivo[actual][70:]
                    # Actualizamos actual 
                actual = actual + 1
        else:
            errorCuadEst1 = "No medido"

        ajustesZWO = []
        if actual + 3 < tamInforme:
            # Sacamos los ajustes ZWO
            actual = actual + 3
            while lineasarchivo[actual] != "" :
                if lineasarchivo[actual][25] != " ":
                    ajuste = lineasarchivo[actual][25:].split(' ') # Añadimos X/Y y Ar/De del ajuste
                else:
                    ajuste = lineasarchivo[actual][26:].split(' ') # Añadimos X/Y y Ar/De del ajuste
                ajuste.append(lineasarchivo[actual][11:][:2]) # Añadimos la hora
                ajuste.append(lineasarchivo[actual][14:][:2]) # Añadimos el minuto
                ajuste.append(lineasarchivo[actual][17:][:7]) # Añadimos el segundo
                ajustesZWO.append(ajuste)
                actual = actual+1
                # Actualizamos actual 
            actual = actual + 1
        fotogramas = str(len(ajustesZWO))

        if actual+1 < tamInforme:
            if lineasarchivo[actual][:9] != "No se han":
                erroresCuadEst2 = []
                actual = actual + 1
                while(lineasarchivo[actual] != ""):
                    # Sacamos el error cuadrático de 
                    erroresCuadEst2.append(lineasarchivo[actual][70:])
                            # Actualizamos actual 
                    actual = actual + 1
                erCuadEst2 = ""
                for i in erroresCuadEst2:
                    erCuadEst2 = erCuadEst2 + i + " "
                actual = actual + 1
            else:
                erCuadEst2 = "No medido"
                    # Actualizamos actual 
                actual = actual + 2
        else: 
            actual = actual + 2
            erCuadEst2 = "No medido"
            
        if actual < tamInforme:
            # Sacamos el ajuste de la estación 2
            ajusteEstacion2 = []
            ajusteEstacion2.append(lineasarchivo[actual+2][8:].split(' '))
            ajusteEstacion2.append(lineasarchivo[actual+3][8:].split(' '))
            ajEst2Inicio = ajusteEstacion2[0][0] + " " +ajusteEstacion2[0][1] + " " +ajusteEstacion2[0][2] + " " +ajusteEstacion2[0][3] + " " +ajusteEstacion2[0][4]+ " " +ajusteEstacion2[0][5]
            ajEst2Final = ajusteEstacion2[1][0] + " " +ajusteEstacion2[1][1] + " " +ajusteEstacion2[1][2] + " " +ajusteEstacion2[1][3] + " " +ajusteEstacion2[1][4]+ " " +ajusteEstacion2[1][5]
                # Actualizamos actual 
            actual = actual + 5
        else:
            ajEst2Inicio = "No medido"
            ajEst2Final = "No medido"

        if actual < tamInforme:
            # Sacamos los siguientes datos
            anguloDiedro = lineasarchivo[actual][40:]
            pesoEstadistico = lineasarchivo[actual+1][18:]
                # Actualizamos actual 
            actual = actual + 3
        else:
            anguloDiedro = "NULL"
            pesoEstadistico = "NULL"

        if actual < tamInforme:
            # Añadimos lo errores AR/DE radiante
            erARDE = lineasarchivo[actual][24:].split(' ')
            errorARDE = erARDE[0] + " " + erARDE[1]
                # Actualizamos actual 
            actual = actual + 1
        else:
            errorARDE = "No medido"
        
        if actual < tamInforme:
            # Añadimos el radiante
            radiante = []
            aux = lineasarchivo[actual].split(' ')
            radiante.append(aux[1].strip(","))
            radiante.append(aux[2].strip(","))
            radiante.append(aux[3])
            radiante.append(aux[4])
            coordsRadiante = radiante[0] + " " + radiante[1] + " " + radiante[2] + " " + radiante[3]
                # Actualizamos actual 
            actual = actual + 1
        else:
            coordsRadiante = "No medido"

        if actual < tamInforme:
            # Añadimos J2000
            j2000 = []
            aux = lineasarchivo[actual].split(' ')
            j2000.append(aux[3].strip(","))
            j2000.append(aux[4].strip(","))
            j2000.append(aux[5])
            j2000.append(aux[6])
            coordsJ2000 = j2000[0] + " " + j2000[1] + " " + j2000[2] + " " + j2000[3]
                # Actualizamos actual 
            actual = actual + 1
        else: 
            coordsJ2000 = "No medido"

        if actual < tamInforme:
            # Añadimos Azimut y distancia cenital (si existe)
            if lineasarchivo[actual][:6] == "Azimut":
                aux = lineasarchivo[actual].split(' ')
                azimut = aux[9]
                distCenital = aux[10]
                    # Actualizamos actual 
                actual = actual + 3
            else:
                azimut = "NULL"
                distCenital = "NULL"
                    # Actualizamos actual 
                actual = actual + 2
        else: 
            azimut = "NULL"
            distCenital = "NULL"

        if actual < tamInforme:
            # Añadimos los datos de estacion 1:
            Est1 = [] 
            for i in range (0,2):
                aux = lineasarchivo[actual+i][11:].split(' ')
                aux[2] = aux[2][2:]
                aux[3] = aux[3][2:]
                Est1.append(aux)
            Est1Inicio = Est1[0][0] + " " + Est1[0][1] + " " + Est1[0][2] + " " + Est1[0][3]
            Est1Final = Est1[1][0] + " " + Est1[1][1] + " " + Est1[1][2] + " " + Est1[1][3]
                # Actualizamos actual 
            actual = actual + 4
        else:
            Est1Inicio = "No medido"
            Est1Final = "No medido"

        if actual < tamInforme:
            # Añadimos los datos de estacion 2:
            Est2 = []
            for i in range (0,2):
                aux = lineasarchivo[actual+i][11:].split(' ')
                aux[2] = aux[2][2:]
                aux[3] = aux[3][2:]
                Est2.append(aux)  
            Est2Inicio = Est2[0][0] + " " + Est2[0][1] + " " + Est2[0][2] + " " + Est2[0][3]
            Est2Final = Est2[1][0] + " " + Est2[1][1] + " " + Est2[1][2] + " " + Est2[1][3]
                # Actualizamos actual 
            actual = actual + 4
        else:
            Est2Inicio = "No medido"
            Est2Final = "No medido"

        if actual < tamInforme:
            # Sacamos el impacto previsible
            impactoPrev = lineasarchivo[actual][12:] # X e Y del impacto
                # Actualizamos actual 
            actual = actual + 3
        else:
            impactoPrev = "No medido"

        if actual < tamInforme:
            # Ecuación parametrica de la trayectoria
            aux = lineasarchivo[actual][4:].split(' ')
            ecParam = []
            ecParam.append(aux[1]) # Añadimos a
            ecParam.append(aux[3]) # Añadimos b
            ecParam.append(aux[5]) # Añadimos c

            aux = lineasarchivo[actual+1][4:].split(' ')
            ptsEst1ParamInicio = []
            ptsEst1ParamInicio.append(aux[1]) # Añadimos xi
            ptsEst1ParamInicio.append(aux[3]) # Añadimos eta
            ptsEst1ParamInicio.append(aux[5]) # Añadimos zeta

            aux = lineasarchivo[actual+2][4:].split(' ')
            ptsEst1ParamFinal = []
            ptsEst1ParamFinal.append(aux[1]) # Añadimos xi
            ptsEst1ParamFinal.append(aux[3]) # Añadimos eta
            ptsEst1ParamFinal.append(aux[5]) # Añadimos zeta

            aux = lineasarchivo[actual+3][4:].split(' ')
            ptsEst2ParamInicio = []
            ptsEst2ParamInicio.append(aux[1]) # Añadimos xi
            ptsEst2ParamInicio.append(aux[3]) # Añadimos eta
            ptsEst2ParamInicio.append(aux[5]) # Añadimos zeta

            aux = lineasarchivo[actual+4][4:].split(' ')
            ptsEst2ParamFinal = []
            ptsEst2ParamFinal.append(aux[1]) # Añadimos xi
            ptsEst2ParamFinal.append(aux[3]) # Añadimos eta
            ptsEst2ParamFinal.append(aux[5]) # Añadimos zeta
                # Actualizamos actual 
            actual = actual + 6

            if infNuevo:
                cursor.execute("SELECT COUNT(*) FROM Ecuacion_parametrica")
                for i in cursor:
                    idEc = i[0] + 1
                insert = "INSERT INTO Ecuacion_parametrica (IdEc, a, b, c, Inicio_Estacion_1, Fin_Estacion_1, Inicio_Estacion_2, Fin_Estacion_2) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(insert, (idEc, float(ecParam[0]), float(ecParam[1]), float(ecParam[2]), 
                                       ptsEst1ParamInicio[0] + " " + ptsEst1ParamInicio[1] + " " + ptsEst1ParamInicio[2],
                                       ptsEst1ParamFinal[0] + " " + ptsEst1ParamFinal[1] + " " + ptsEst1ParamFinal[2],
                                       ptsEst2ParamInicio[0] + " " + ptsEst2ParamInicio[1] + " " + ptsEst2ParamInicio[2],
                                       ptsEst2ParamFinal[0] + " " + ptsEst2ParamFinal[1] + " " + ptsEst2ParamFinal[2]))
        else:
            if infNuevo:
                cursor.execute("SELECT COUNT(*) FROM Ecuacion_parametrica")
                for i in cursor:
                    idEc = i[0] + 1
                insert = "INSERT INTO Ecuacion_parametrica (IdEc, a, b, c, Inicio_Estacion_1, Fin_Estacion_1, Inicio_Estacion_2, Fin_Estacion_2) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(insert, (idEc, None, None, None, None, None, None, None))
            

        if actual+3 < tamInforme:
            # Sacamos los siguientes datos
            aux = lineasarchivo[actual].split(' ')
            distRecorridaEst1 = aux[4]
            errorDistEst1 = aux[6]
            aux = lineasarchivo[actual+1].split(' ')
            errorAlturasEst1 = aux[4]

            aux = lineasarchivo[actual+2].split(' ')
            if aux[4] == ":":
                distRecorridaEst2 = aux[5]
                errorDistEst2 = aux[7]
            else:
                distRecorridaEst2 = aux[4]
                errorDistEst2 = aux[6]
            aux = lineasarchivo[actual+3].split(' ')
            if aux[4] == ":":
                errorAlturasEst2 = aux[5]
            else:
                errorAlturasEst2 = aux[4]
                
                # Actualizamos actual 
            actual = actual + 8
        else:
            distRecorridaEst1 = "NULL"
            errorDistEst1 = "NULL"
            errorAlturasEst1 = "NULL"

            distRecorridaEst2 = "NULL"
            errorDistEst2 = "NULL"
            errorAlturasEst2 = "NULL"

        # Trayectoria medida desde estacion 1
            # Primeramente añadimos la inicial por separado, ya que no tiene ciertos valores
        trayectoriaEst1 = []
        if actual < tamInforme:
            aux = lineasarchivo[actual][35:].split(' ')
            trayectoriaInicial = []
            trayectoriaInicial.append("0.000")
            trayectoriaInicial.append("NULL")
            trayectoriaInicial.append("NULL")
            for i in aux:
                if i != "":
                    trayectoriaInicial.append(i)
            trayectoriaInicial.append("NULL")
            trayectoriaInicial.append("NULL")
            trayectoriaInicial.append(lineasarchivo[actual][11:][:2]) # Añadimos la hora
            trayectoriaInicial.append(lineasarchivo[actual][14:][:2]) # Añadimos el minuto
            trayectoriaInicial.append(lineasarchivo[actual][17:][:7]) # Añadimos el segundo
                # Actualizamos actual 
            actual = actual + 1

                # Añadimos el resto de la trayectoria
            trayectoriaEst1.append(trayectoriaInicial)
            while(lineasarchivo[actual] != "" and actual+1 < tamInforme):
                aux = lineasarchivo[actual][25:].split(' ')
                auxTrayec = []
                for i in aux:
                    if i != "":
                        auxTrayec.append(i)
                auxTrayec.append(lineasarchivo[actual][11:][:2]) # Añadimos la hora
                auxTrayec.append(lineasarchivo[actual][14:][:2]) # Añadimos el minuto
                auxTrayec.append(lineasarchivo[actual][17:][:7]) # Añadimos el segundo
                trayectoriaEst1.append(auxTrayec)
                actual = actual + 1
        
        if actual+1 < tamInforme:
            # Añadimos Tiempo estación 1
            if lineasarchivo[actual+1][:6] == "Tiempo": 
                tiempoEst1 = lineasarchivo[actual+1][19:]
                    # Actualizamos actual 
                actual = actual + 3
            else:
                actual = actual+1
                tiempoEst1 = "NULL"
        else:
            tiempoEst1 = "NULL"

        if actual < tamInforme:
            # Añadimos velocidad media
            aux = lineasarchivo[actual].split(' ')
            if aux[2] != "media" and aux[2] != "":
                vMedia = aux[2]
            elif aux[3] != "":
                vMedia = aux[3]
            else:
                vMedia = aux[4]
                # Actualizamos actual 
            actual = actual + 3
        else:
            vMedia = "NULL"

        if actual < tamInforme:
            # Añadimos Tiempo trayectoria en estación 2
            aux = lineasarchivo[actual].split(' ')
            if aux[6] == "(segundos):":
                if aux[7] == " ":
                    tTrayecEsta2 = aux[8]
                else:
                    tTrayecEsta2 = aux[7]
            else:
                tTrayecEsta2 = aux[6]
                # Actualizamos actual 
            actual = actual + 1
        else:
            tTrayecEsta2 = "NULL"

        if actual < tamInforme:
            # Añadimos Distancia recorrida en estación 2
            aux = lineasarchivo[actual].split(' ')
            distRecorrEst2 = aux[6]
                # Actualizamos actual 
            actual = actual + 6

        trayecRegresionEst1 = []
        if actual < tamInforme:
        # Trayectoria calculada por regresión desde estacion 1
            while(lineasarchivo[actual] != ""):
                aux = lineasarchivo[actual][25:].split(' ')
                trayec = []
                for i in aux:
                    if i != "":
                        trayec.append(i)
                trayec.append(lineasarchivo[actual][11:][:2]) # Añadimos la hora
                trayec.append(lineasarchivo[actual][14:][:2]) # Añadimos el minuto
                trayec.append(lineasarchivo[actual][17:][:7]) # Añadimos el segundo
                trayecRegresionEst1.append(trayec)
                actual = actual + 1
                # Actualizamos actual 
            actual = actual + 3

        if actual < tamInforme:
            # Añadimos la ecuación del movimiento en Km/s
            ecMovKMS = []
            for i in range (actual,actual+3):
                aux = lineasarchivo[i].split(' ')
                ecMovKMS.append(aux[2])
                # Actualizamos actual 
            ecMovKMSentera = ecMovKMS[0] + " " + ecMovKMS[1] + " " + ecMovKMS[2]
            actual = actual + 6
        else:
            ecMovKMSentera = "No medido"

        if actual < tamInforme:
            # Añadimos la ecuación del movimiento en pix/s
            ecMovPIX = []
            for i in range (actual,actual+3):
                aux = lineasarchivo[i].split(' ')
                ecMovPIX.append(aux[2])
                # Actualizamos actual 
            ecMovPIXentera = ecMovPIX[0] + " " + ecMovPIX[1] + " " + ecMovPIX[2]
            actual = actual + 3
        else:
            ecMovPIXentera = "No medido"

        if actual < tamInforme:
            # Sacamos el error en la velocidad
            errorVelocidad = lineasarchivo[actual][17:]
                # Actualizamos actual 
            actual = actual + 2
        else:
            errorVelocidad="NULL"
    
        if actual < tamInforme:
            if lineasarchivo[actual][:17] == "Velocidad inicial":
                # Sacamos velocidad inicial en estación 2
                aux = lineasarchivo[actual].split(' ')
                if aux[4] != ":":
                    vIniEst2 = aux[4]
                else:
                    vIniEst2 = aux[5]
                    # Actualizamos actual 
                actual = actual + 2
            else:
                vIniEst2 = "NULL"
        else:
            vIniEst2 = "NULL"

        if actual < tamInforme:
            # Sacamos aceleración en Km/s^2
            aux = lineasarchivo[actual].split(' ')
            aceKMS = aux[len(aux)-1]
                # Actualizamos actual 
            actual = actual + 1
        else:
            aceKMS = "NULL"

        if actual < tamInforme:
            # Sacamos aceleración en g's
            aux = lineasarchivo[actual].split(' ')
            aceGS = aux[7]
                # Actualizamos actual 
            seguir = True
            while(lineasarchivo[actual][:10] != "Distancias" and seguir):
                actual = actual + 1
                if actual+1 >= tamInforme:
                    seguir = False
            actual = actual + 1
        else:
            aceGS = "NULL"


        lluviasActivas = []
        distMinRadTray = []
        if actual < tamInforme:
            # Sacamos las lluvias activas a la fecha y distancias entre radianes y trayectoria
            while(lineasarchivo[actual] != "" and actual+1<tamInforme):
                aux = lineasarchivo[actual].split(' ')
                distMinRadTray.append(aux[0])
                lluviasActivas.append(aux[1])
                actual = actual + 1
                # Actualizamos actual 
            actual = actual + 3

        if actual < tamInforme:
            # Sacamos los elementos orbitales
            velInf = lineasarchivo[actual][18:].split(' ')
            velGeo = lineasarchivo[actual+1][18:].split(' ')
                # Actualizamos actual 
            actual = actual + 3

        if actual < tamInforme:
            radianteGeocentrico = [] # Ar, De, i, p, a, e, q, T, Omega (Para cada valor: Max, Min)
            for i in range(actual,actual+10):
                radianteGeocentrico.append(lineasarchivo[i][18:].split(' '))
                # Actualizamos actual 
            actual = actual + 12
        
        hayOrb = False
        if actual < tamInforme:
            # Añadimos los elementos orbitales
            elemOrb = [] # Vi, Vg, ar, de, i, p, a, e, q, T, omega Omega (Para cada valor: Valor, error)
            for i in range(0,11):
                aux = lineasarchivo[actual+i].split(' ')
                elemO = []
                elemO.append(aux[1])
                elemO.append(aux[2])
                elemOrb.append(elemO)

            #Añadimnos Omega por separado ya que tiene una disposición distinta
            Omega = [] # grados, votos
            Omega.append(lineasarchivo[actual+12][7:].split(' '))
            Omega.append(lineasarchivo[actual+13][7:].split(' '))
                # Actualizamos actual 
            hayOrb = True
            actual = actual + 15

            #Por petición de Alberto Castellón, el valor p de los elementos orbitales se modifica de forma que queda p = p*pi/180
            p1 = (float(radianteGeocentrico[3][0]) * math.pi) / 180
            p2 = (float(radianteGeocentrico[3][1]) * math.pi) / 180
            p3 = (float(elemOrb[5][0]) * math.pi) / 180
            p4 = (float(elemOrb[5][1]) * math.pi) / 180

            radianteGeocentrico[3][0] = str(p1)
            radianteGeocentrico[3][1] = str(p2)
            elemOrb[5][0] = str(p3)
            elemOrb[5][1] = str(p4)
            

        if actual < tamInforme:
        # Añadimos el método utilizado
            metodo = lineasarchivo[actual][18:]
        else:
            metodo = "NULL"

        if infNuevo:
            cursor.execute("SELECT * FROM Meteoro")
            insertar = True
            update = False
            for i in cursor:
                fechaBien = str(i[1])  # MySQL ya devuelve el formato correcto YYYY-MM-DD
                if fecha == fechaBien:
                    if hora[:5] == str(i[2])[:5]:
                        if float(str(i[2])[6:]) < float(hora[6:]) and float(str(i[2])[6:])+2 > float(hora[6:]):
                            idM = i[0]
                            insertar = False
                        elif float(str(i[2])[6:]) == float(hora[6:]):
                            idM = i[0]
                            insertar = False
                        elif float(str(i[2])[6:]) > float(hora[6:]) and float(str(i[2])[6:])-2 < float(hora[6:]):
                            idM = i[0]
                            update_query = "UPDATE Meteoro SET Hora = %s, Fecha = %s WHERE Identificador = %s"
                            cursor.execute(update_query, (hora, fecha, i[0]))
                            update = True
                            insertar = False
            
            if insertar:
                cursor.execute("SELECT * FROM Meteoro")
                idM = 1
                for i in cursor:
                    idM = i[0] + 1
                insert = "INSERT INTO Meteoro (Identificador, Fecha, Hora) VALUES (%s, %s, %s)"
                cursor.execute(insert, (idM, fecha, hora))

        if infNuevo:
            # Preparar valores para NULL de manera segura
            angulo_val = None if anguloDiedro == "NULL" else float(anguloDiedro)
            peso_val = None if pesoEstadistico == "NULL" else float(pesoEstadistico)
            azimut_val = None if azimut == "NULL" else float(azimut)
            dist_cenital_val = None if distCenital == "NULL" else float(distCenital)
            dist_recorrida_est1_val = None if distRecorridaEst1 == "NULL" else float(distRecorridaEst1)
            error_dist_est1_val = None if errorDistEst1 == "NULL" else float(errorDistEst1)
            error_alturas_est1_val = None if errorAlturasEst1 == "NULL" else float(errorAlturasEst1)
            dist_recorrida_est2_val = None if distRecorridaEst2 == "NULL" else float(distRecorridaEst2)
            error_dist_est2_val = None if errorDistEst2 == "NULL" else float(errorDistEst2)
            error_alturas_est2_val = None if errorAlturasEst2 == "NULL" else float(errorAlturasEst2)
            tiempo_est1_val = None if tiempoEst1 == "NULL" else float(tiempoEst1)
            v_media_val = None if vMedia == "NULL" else float(vMedia)
            t_trayec_esta2_val = None if tTrayecEsta2 == "NULL" else float(tTrayecEsta2)
            error_velocidad_val = None if errorVelocidad == "NULL" else float(errorVelocidad)
            v_ini_est2_val = None if vIniEst2 == "NULL" else float(vIniEst2)
            ace_kms_val = None if aceKMS == "NULL" else float(aceKMS)
            ace_gs_val = None if aceGS == "NULL" else float(aceGS)
            metodo_val = None if metodo == "NULL" else metodo

            insert = """INSERT INTO Informe_Z (IdInforme, Observatorio_Número2, Observatorio_Número, Fecha, Hora, 
                       Error_cuadrático_de_ortogonalidad_en_la_esfera_celeste_1, 
                       Error_cuadrático_de_ortogonalidad_en_la_esfera_celeste_2, Fotogramas_usados,
                       Ajuste_estación_2_Inicio, Ajuste_estación_2_Final, 
                       Ángulo_diedro_entre_planos_trayectoria, Peso_estadístico, 
                       Errores_AR_DE_radiante, Coordenadas_astronómicas_del_radiante_Eclíptica_de_la_fecha, 
                       Coordenadas_astronómicas_del_radiante_J200, Azimut, Dist_Cenital, 
                       Inicio_de_la_trayectoria_Estacion_1, Fin_de_la_trayectoria_Estacion_1, 
                       Inicio_de_la_trayectoria_Estacion_2, Fin_de_la_trayectoria_Estacion_2, 
                       Impacto_previsible, Distancia_recorrida_Estacion_1, Error_distancia_Estacion_1, 
                       Error_alturas_Estacion_1, Distancia_recorrida_Estacion_2, Error_distancia_Estacion_2, 
                       Error_alturas_Estacion_2, Tiempo_Estacion_1, Velocidad_media, 
                       Tiempo_trayectoria_en_estacion_2, Ecuacion_del_movimiento_en_Kms, 
                       Ecuacion_del_movimiento_en_gs, Error_Velocidad, Velocidad_Inicial_Estacion_2, 
                       Aceleración_en_Kms, Aceleración_en_gs, Método_utilizado, Ruta_del_informe, 
                       Ecuacion_parametrica_IdEc, Meteoro_Identificador) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            
            cursor.execute(insert, (idInf, estacion2[0], estacion1[0], fecha, hora, errorCuadEst1, erCuadEst2, 
                                   int(fotogramas), ajEst2Inicio, ajEst2Final, angulo_val, peso_val, 
                                   errorARDE, coordsRadiante, coordsJ2000, azimut_val, dist_cenital_val, 
                                   Est1Inicio, Est1Final, Est2Inicio, Est2Final, impactoPrev, 
                                   dist_recorrida_est1_val, error_dist_est1_val, error_alturas_est1_val, 
                                   dist_recorrida_est2_val, error_dist_est2_val, error_alturas_est2_val, 
                                   tiempo_est1_val, v_media_val, t_trayec_esta2_val, ecMovKMSentera, 
                                   ecMovPIXentera, error_velocidad_val, v_ini_est2_val, ace_kms_val, 
                                   ace_gs_val, metodo_val, ruta, idEc, idM))

            for i in range(len(lluviasActivas)):
                insert = "INSERT INTO Lluvia_Activa (Distancia_mínima_entre_radianes_y_trayectoria, Lluvia_Identificador, Lluvia_Año, Informe_Z_IdInforme) VALUES (%s, %s, %s, %s)"
                cursor.execute(insert, (distMinRadTray[i], lluviasActivas[i], int(yearInforme), idInf))

            for i in range(len(trayecRegresionEst1)):
                hora_trayec = trayecRegresionEst1[i][4] + ":" + trayecRegresionEst1[i][5] + ":" + trayecRegresionEst1[i][6]
                insert = "INSERT INTO Trayectoria_por_regresion (Fecha, Hora, t, s, v_Kms, v_Pixs, Informe_Z_IdInforme) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(insert, (fecha, hora_trayec, float(trayecRegresionEst1[i][0]), float(trayecRegresionEst1[i][1]), float(trayecRegresionEst1[i][2]), float(trayecRegresionEst1[i][3]), idInf))
            
            for i in range(len(ajustesZWO)):
                hora_ajuste = ajustesZWO[i][6] + ":" + ajustesZWO[i][7] + ":" + ajustesZWO[i][8]
                insert = "INSERT INTO Puntos_ZWO (Fecha, Hora, X, Y, Ar_Grados, De_Grados, Ar_Sexagesimal, De_Sexagesimal, Informe_Z_IdInforme) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(insert, (fecha, hora_ajuste, float(ajustesZWO[i][0]), float(ajustesZWO[i][1]), float(ajustesZWO[i][2]), float(ajustesZWO[i][3]), ajustesZWO[i][4], ajustesZWO[i][5], idInf))
            

            for i in range(len(trayectoriaEst1)):
                hora_trayec = trayectoriaEst1[i][13] + ":" + trayectoriaEst1[i][14] + ":" + trayectoriaEst1[i][15]
                # Manejo de valores NULL
                s_val = None if trayectoriaEst1[i][0] == "0.000" else float(trayectoriaEst1[i][0])
                t_val = None if trayectoriaEst1[i][1] == "NULL" else float(trayectoriaEst1[i][1])
                v_val = None if trayectoriaEst1[i][2] == "NULL" else float(trayectoriaEst1[i][2])
                x_val = None if trayectoriaEst1[i][9] == "NULL" else float(trayectoriaEst1[i][9])
                y_val = None if trayectoriaEst1[i][10] == "NULL" else float(trayectoriaEst1[i][10])
                pix_val = None if trayectoriaEst1[i][11] == "NULL" else float(trayectoriaEst1[i][11])
                pix_seg_val = None if trayectoriaEst1[i][12] == "NULL" else float(trayectoriaEst1[i][12])
                
                insert = """INSERT INTO Trayectoria_medida (Fecha, Hora, s, t, v, lambda, phi, 
                           AR_Estacion_1, De_Estacion_1, Ar_Estacion_2, De_Estacion_2, X, Y, Pix, Pix_Seg, 
                           Informe_Z_IdInforme) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cursor.execute(insert, (fecha, hora_trayec, s_val, t_val, v_val, trayectoriaEst1[i][3], 
                                       trayectoriaEst1[i][4], trayectoriaEst1[i][5], trayectoriaEst1[i][6], 
                                       trayectoriaEst1[i][7], trayectoriaEst1[i][8], x_val, y_val, pix_val, 
                                       pix_seg_val, idInf))
        
        # Añadimos los elemenos orbitales a la base de datos si existen
        if hayOrb:
            addEO = True
            cursor.execute("SELECT Calculados_Con FROM Elementos_Orbitales WHERE Informe_Z_IdInforme = %s", (idInf,))
            for i in cursor:
                if str(i[0]) == calcCon:
                    addEO = False
            if addEO:
                insert = """INSERT INTO Elementos_Orbitales (Informe_Z_IdInforme, Calculados_con, Vel__Inf, 
                           Vel__Geo, Ar, De, i, p, a, e, q, T, omega, Omega_grados_votos_max_min) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cursor.execute(insert, (idInf, calcCon, 
                                       elemOrb[0][0] + " " + velInf[0] + " " + velInf[1] + " " + elemOrb[0][1],
                                       elemOrb[1][0] + " " + velGeo[0] + " " + velGeo[1] + " " + elemOrb[1][1],
                                       elemOrb[2][0] + " " + radianteGeocentrico[0][0] + " " + radianteGeocentrico[0][1] + " " + elemOrb[2][1],
                                       elemOrb[3][0] + " " + radianteGeocentrico[1][0] + " " + radianteGeocentrico[1][1] + " " + elemOrb[3][1],
                                       elemOrb[4][0] + " " + radianteGeocentrico[2][0] + " " + radianteGeocentrico[2][1] + " " + elemOrb[4][1],
                                       elemOrb[5][0] + " " + radianteGeocentrico[3][0] + " " + radianteGeocentrico[3][1] + " " + elemOrb[5][1],
                                       elemOrb[6][0] + " " + radianteGeocentrico[4][0] + " " + radianteGeocentrico[4][1] + " " + elemOrb[6][1],
                                       elemOrb[7][0] + " " + radianteGeocentrico[5][0] + " " + radianteGeocentrico[5][1] + " " + elemOrb[7][1],
                                       elemOrb[8][0] + " " + radianteGeocentrico[6][0] + " " + radianteGeocentrico[6][1] + " " + elemOrb[8][1],
                                       elemOrb[9][0] + " " + radianteGeocentrico[7][0] + " " + radianteGeocentrico[7][1] + " " + elemOrb[9][1],
                                       elemOrb[10][0] + " " + radianteGeocentrico[9][0] + " " + radianteGeocentrico[9][1] + " " + elemOrb[10][1],
                                       "(" + Omega[0][0] + " " + Omega[0][1] + "), (" + Omega[1][0] + " " + Omega[1][1] + ") " + radianteGeocentrico[8][0] + " " + radianteGeocentrico[8][1]))


    if len(sys.argv) > 1:
        directorio = sys.argv[1]
        with os.scandir(directorio) as itr:
                for entrada in itr:
                    if entrada.is_dir():
                        recorrerSubdirectorio(directorio + "/" + entrada.name)
                    elif entrada.name[:9] == "Informe-Z" and entrada.name[-3:] != "kml":
                        procesaInforme(directorio, entrada.name)
    else:
        directorio = input("Directorio raíz de los informes a cargar: ")
        print("Se cargarán los informes contenidos en el directorio ("+directorio+") y sus subdirectorios")
        sn = input("¿Continuar? (S/N): ")
        if sn == "S":
            with os.scandir(directorio) as itr:
                for entrada in itr:
                    if entrada.is_dir():
                        recorrerSubdirectorio(directorio + "/" + entrada.name)
                    elif entrada.name[:9] == "Informe-Z" and entrada.name[-3:] != "kml":
                        procesaInforme(directorio, entrada.name)

    cursor.close()
    cnxn.close()

except mysql.connector.Error as e:
    print('Error de conexión MySQL:', e)
except Exception as e:
    print('Error general:', e)
finally:
    if cnxn.is_connected():
        cursor.close()
        cnxn.close()