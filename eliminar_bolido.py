#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Elimina un unico bolido de la base de datos dada una fecha concreta.

El script busca los registros de Meteoro de una fecha, muestra las opciones si
hay mas de una y borra solo el bolido seleccionado junto con sus relaciones
propias. No borra tablas compartidas como Observatorio o Lluvia.

Uso:
    python3 eliminar_bolido.py 20250901
    python3 eliminar_bolido.py 2025-09-01 --identificador 123
"""

import argparse
import sys
from datetime import datetime

try:
    import mysql.connector
    from mysql.connector import Error
except ImportError:
    mysql = None
    Error = Exception

try:
    from config_db import DB_CONFIG, TABLES, get_connection_string, validate_config
except ImportError:
    print("Error: No se pudo importar config_db.py")
    print("Asegurate de ejecutar este script desde el directorio SMA_SCRIPTS")
    sys.exit(1)


class EliminadorBolido:
    def __init__(self):
        self.conexion = None
        self.cursor = None
        self.tabla_meteoro = TABLES.get("meteoro", "Meteoro")
        self.tabla_informe_z = TABLES.get("informe_z", "Informe_Z")
        self.tabla_radiante = TABLES.get("radiante", "Informe_Radiante")
        self.tabla_fotometria = TABLES.get("fotometria", "Informe_Fotometria")
        self.tablas_existentes = set()

    def conectar_mysql(self):
        if mysql is None:
            print("Error: No se pudo importar mysql.connector")
            print("Instala la dependencia con: pip install mysql-connector-python")
            return False

        config_valida, mensaje = validate_config()
        if not config_valida:
            print(f"Error en configuracion: {mensaje}")
            return False

        try:
            print(f"Conectando a {get_connection_string()}...")
            self.conexion = mysql.connector.connect(**DB_CONFIG)
            self.cursor = self.conexion.cursor(dictionary=True)
            self._cargar_tablas_existentes()
            print("Conexion exitosa a MySQL")
            return True
        except Error as exc:
            print(f"Error al conectar a MySQL: {exc}")
            return False

    def _cargar_tablas_existentes(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        self.tablas_existentes = {next(iter(row.values())) for row in rows}

    def tabla_existe(self, nombre_tabla):
        return nombre_tabla in self.tablas_existentes

    def cerrar_conexion(self):
        if self.cursor:
            self.cursor.close()
        if self.conexion and self.conexion.is_connected():
            self.conexion.close()
            print("Conexion a MySQL cerrada")

    def validar_fecha(self, fecha_str):
        formatos = ("%Y%m%d", "%Y-%m-%d")
        for formato in formatos:
            try:
                return datetime.strptime(fecha_str, formato).strftime("%Y-%m-%d")
            except ValueError:
                pass

        print("Error: La fecha debe estar en formato YYYYMMDD o YYYY-MM-DD")
        return None

    def obtener_bolidos_por_fecha(self, fecha):
        query = f"""
            SELECT Identificador, Fecha, Hora
            FROM `{self.tabla_meteoro}`
            WHERE Fecha = %s
            ORDER BY Hora, Identificador
        """
        self.cursor.execute(query, (fecha,))
        return self.cursor.fetchall()

    def obtener_bolido_por_id(self, identificador, fecha=None):
        params = [identificador]
        filtro_fecha = ""
        if fecha:
            filtro_fecha = "AND Fecha = %s"
            params.append(fecha)

        query = f"""
            SELECT Identificador, Fecha, Hora
            FROM `{self.tabla_meteoro}`
            WHERE Identificador = %s {filtro_fecha}
            LIMIT 1
        """
        self.cursor.execute(query, tuple(params))
        return self.cursor.fetchone()

    def elegir_bolido(self, fecha, identificador=None):
        if identificador is not None:
            bolido = self.obtener_bolido_por_id(identificador, fecha)
            if not bolido:
                print(f"No existe ningun bolido con Identificador={identificador} en {fecha}")
            return bolido

        bolidos = self.obtener_bolidos_por_fecha(fecha)
        if not bolidos:
            print(f"No hay bolidos guardados en la fecha {fecha}")
            return None

        if len(bolidos) == 1:
            bolido = bolidos[0]
            print(
                "Se encontro un unico bolido: "
                f"ID={bolido['Identificador']} Fecha={bolido['Fecha']} Hora={bolido['Hora']}"
            )
            return bolido

        print(f"Se encontraron {len(bolidos)} bolidos en {fecha}:")
        for indice, bolido in enumerate(bolidos, start=1):
            print(
                f"  {indice}. ID={bolido['Identificador']} "
                f"Hora={bolido['Hora']}"
            )

        while True:
            respuesta = input("Elige el numero del bolido a eliminar: ").strip()
            if not respuesta.isdigit():
                print("Introduce un numero de la lista.")
                continue

            indice = int(respuesta)
            if 1 <= indice <= len(bolidos):
                return bolidos[indice - 1]

            print("Opcion fuera de rango.")

    def _contar(self, query, params):
        self.cursor.execute(query, params)
        row = self.cursor.fetchone()
        return next(iter(row.values())) if row else 0

    def obtener_ids_informe_z(self, meteoro_id):
        query = f"""
            SELECT IdInforme, Ecuacion_parametrica_IdEc
            FROM `{self.tabla_informe_z}`
            WHERE Meteoro_Identificador = %s
        """
        self.cursor.execute(query, (meteoro_id,))
        return self.cursor.fetchall()

    def obtener_estadisticas(self, meteoro_id):
        estadisticas = {}

        informe_z_ids = self.obtener_ids_informe_z(meteoro_id)
        estadisticas["Informe_Z"] = len(informe_z_ids)
        estadisticas["Ecuacion_parametrica"] = self._contar_ecuaciones_huerfanas(informe_z_ids)

        informe_fot_query = f"""
            SELECT COUNT(*) AS total
            FROM `{self.tabla_fotometria}`
            WHERE Meteoro_Identificador = %s
        """
        estadisticas["Informe_Fotometria"] = self._contar(informe_fot_query, (meteoro_id,))

        informe_rad_query = f"""
            SELECT COUNT(*) AS total
            FROM `{self.tabla_radiante}`
            WHERE Meteoro_Identificador = %s
        """
        estadisticas["Informe_Radiante"] = self._contar(informe_rad_query, (meteoro_id,))

        relaciones = [
            (
                "Elementos_Orbitales",
                f"""
                SELECT COUNT(*) AS total
                FROM `Elementos_Orbitales` eo
                INNER JOIN `{self.tabla_informe_z}` iz
                    ON eo.Informe_Z_IdInforme = iz.IdInforme
                WHERE iz.Meteoro_Identificador = %s
                """,
            ),
            (
                "Lluvia_activa",
                f"""
                SELECT COUNT(*) AS total
                FROM `Lluvia_activa` la
                INNER JOIN `{self.tabla_informe_z}` iz
                    ON la.Informe_Z_IdInforme = iz.IdInforme
                WHERE iz.Meteoro_Identificador = %s
                """,
            ),
            (
                "Puntos_ZWO",
                f"""
                SELECT COUNT(*) AS total
                FROM `Puntos_ZWO` pz
                INNER JOIN `{self.tabla_informe_z}` iz
                    ON pz.Informe_Z_IdInforme = iz.IdInforme
                WHERE iz.Meteoro_Identificador = %s
                """,
            ),
            (
                "Trayectoria_medida",
                f"""
                SELECT COUNT(*) AS total
                FROM `Trayectoria_medida` tm
                INNER JOIN `{self.tabla_informe_z}` iz
                    ON tm.Informe_Z_IdInforme = iz.IdInforme
                WHERE iz.Meteoro_Identificador = %s
                """,
            ),
            (
                "Trayectoria_por_regresion",
                f"""
                SELECT COUNT(*) AS total
                FROM `Trayectoria_por_regresion` tpr
                INNER JOIN `{self.tabla_informe_z}` iz
                    ON tpr.Informe_Z_IdInforme = iz.IdInforme
                WHERE iz.Meteoro_Identificador = %s
                """,
            ),
            (
                "Datos_meteoro_fotometria",
                f"""
                SELECT COUNT(*) AS total
                FROM `Datos_meteoro_fotometria` dmf
                INNER JOIN `{self.tabla_fotometria}` ifot
                    ON dmf.Informe_Fotometria_Identificador = ifot.Identificador
                WHERE ifot.Meteoro_Identificador = %s
                """,
            ),
            (
                "Estrellas_usadas_para_regresion",
                f"""
                SELECT COUNT(*) AS total
                FROM `Estrellas_usadas_para_regresión` eur
                INNER JOIN `{self.tabla_fotometria}` ifot
                    ON eur.Informe_Fotometria_Identificador = ifot.Identificador
                WHERE ifot.Meteoro_Identificador = %s
                """,
            ),
            (
                "Puntos_del_ajuste",
                f"""
                SELECT COUNT(*) AS total
                FROM `Puntos_del_ajuste` pa
                INNER JOIN `{self.tabla_fotometria}` ifot
                    ON pa.Informe_Fotometria_Identificador = ifot.Identificador
                WHERE ifot.Meteoro_Identificador = %s
                """,
            ),
            (
                "Lluvia_Activa_InfRad",
                f"""
                SELECT COUNT(*) AS total
                FROM `Lluvia_Activa_InfRad` lair
                INNER JOIN `{self.tabla_radiante}` ir
                    ON lair.Informe_Radiante_Identificador = ir.Identificador
                WHERE ir.Meteoro_Identificador = %s
                """,
            ),
            (
                "Trayectoria_estimada",
                f"""
                SELECT COUNT(*) AS total
                FROM `Trayectoria_estimada` te
                INNER JOIN `{self.tabla_radiante}` ir
                    ON te.Informe_Radiante_Identificador = ir.Identificador
                WHERE ir.Meteoro_Identificador = %s
                """,
            ),
        ]

        for tabla, query in relaciones:
            if self.tabla_existe(tabla):
                estadisticas[tabla] = self._contar(query, (meteoro_id,))

        tabla_velocidades = self._tabla_velocidades_angulares()
        if tabla_velocidades:
            query = f"""
                SELECT COUNT(*) AS total
                FROM `{tabla_velocidades}` va
                INNER JOIN `{self.tabla_radiante}` ir
                    ON va.Informe_Radiante_Identificador = ir.Identificador
                WHERE ir.Meteoro_Identificador = %s
            """
            estadisticas[tabla_velocidades] = self._contar(query, (meteoro_id,))

        estadisticas["Meteoro"] = 1
        return estadisticas

    def _tabla_velocidades_angulares(self):
        for nombre in ("Velocidades_Angulares", "Velociades_Angulares"):
            if self.tabla_existe(nombre):
                return nombre
        return None

    def _contar_ecuaciones_huerfanas(self, informe_z_ids):
        ids_ec = [row["Ecuacion_parametrica_IdEc"] for row in informe_z_ids if row["Ecuacion_parametrica_IdEc"]]
        if not ids_ec or not self.tabla_existe("Ecuacion_parametrica"):
            return 0

        placeholders = ", ".join(["%s"] * len(ids_ec))
        query = f"""
            SELECT COUNT(*) AS total
            FROM `Ecuacion_parametrica` ec
            WHERE ec.IdEc IN ({placeholders})
              AND NOT EXISTS (
                  SELECT 1
                  FROM `{self.tabla_informe_z}` iz
                  WHERE iz.Ecuacion_parametrica_IdEc = ec.IdEc
                    AND iz.IdInforme NOT IN (
                        {", ".join(["%s"] * len(informe_z_ids))}
                    )
              )
        """
        params = ids_ec + [row["IdInforme"] for row in informe_z_ids]
        return self._contar(query, tuple(params))

    def _eliminar_con_join(self, tabla, alias, join, where, params):
        if not self.tabla_existe(tabla):
            return 0

        query = f"""
            DELETE {alias}
            FROM `{tabla}` {alias}
            {join}
            WHERE {where}
        """
        self.cursor.execute(query, params)
        return self.cursor.rowcount

    def eliminar_bolido(self, bolido):
        meteoro_id = bolido["Identificador"]
        contadores = {}

        try:
            if getattr(self.conexion, "in_transaction", False):
                self.conexion.commit()
            self.conexion.start_transaction()

            informe_z_ids = self.obtener_ids_informe_z(meteoro_id)
            ecuacion_ids = [
                row["Ecuacion_parametrica_IdEc"]
                for row in informe_z_ids
                if row["Ecuacion_parametrica_IdEc"]
            ]

            contadores["Elementos_Orbitales"] = self._eliminar_con_join(
                "Elementos_Orbitales",
                "eo",
                f"INNER JOIN `{self.tabla_informe_z}` iz ON eo.Informe_Z_IdInforme = iz.IdInforme",
                "iz.Meteoro_Identificador = %s",
                (meteoro_id,),
            )
            contadores["Lluvia_activa"] = self._eliminar_con_join(
                "Lluvia_activa",
                "la",
                f"INNER JOIN `{self.tabla_informe_z}` iz ON la.Informe_Z_IdInforme = iz.IdInforme",
                "iz.Meteoro_Identificador = %s",
                (meteoro_id,),
            )
            contadores["Puntos_ZWO"] = self._eliminar_con_join(
                "Puntos_ZWO",
                "pz",
                f"INNER JOIN `{self.tabla_informe_z}` iz ON pz.Informe_Z_IdInforme = iz.IdInforme",
                "iz.Meteoro_Identificador = %s",
                (meteoro_id,),
            )
            contadores["Trayectoria_medida"] = self._eliminar_con_join(
                "Trayectoria_medida",
                "tm",
                f"INNER JOIN `{self.tabla_informe_z}` iz ON tm.Informe_Z_IdInforme = iz.IdInforme",
                "iz.Meteoro_Identificador = %s",
                (meteoro_id,),
            )
            contadores["Trayectoria_por_regresion"] = self._eliminar_con_join(
                "Trayectoria_por_regresion",
                "tpr",
                f"INNER JOIN `{self.tabla_informe_z}` iz ON tpr.Informe_Z_IdInforme = iz.IdInforme",
                "iz.Meteoro_Identificador = %s",
                (meteoro_id,),
            )

            contadores["Datos_meteoro_fotometria"] = self._eliminar_con_join(
                "Datos_meteoro_fotometria",
                "dmf",
                f"""
                INNER JOIN `{self.tabla_fotometria}` ifot
                    ON dmf.Informe_Fotometria_Identificador = ifot.Identificador
                """,
                "ifot.Meteoro_Identificador = %s",
                (meteoro_id,),
            )
            contadores["Estrellas_usadas_para_regresión"] = self._eliminar_con_join(
                "Estrellas_usadas_para_regresión",
                "eur",
                f"""
                INNER JOIN `{self.tabla_fotometria}` ifot
                    ON eur.Informe_Fotometria_Identificador = ifot.Identificador
                """,
                "ifot.Meteoro_Identificador = %s",
                (meteoro_id,),
            )
            contadores["Puntos_del_ajuste"] = self._eliminar_con_join(
                "Puntos_del_ajuste",
                "pa",
                f"""
                INNER JOIN `{self.tabla_fotometria}` ifot
                    ON pa.Informe_Fotometria_Identificador = ifot.Identificador
                """,
                "ifot.Meteoro_Identificador = %s",
                (meteoro_id,),
            )

            contadores["Lluvia_Activa_InfRad"] = self._eliminar_con_join(
                "Lluvia_Activa_InfRad",
                "lair",
                f"""
                INNER JOIN `{self.tabla_radiante}` ir
                    ON lair.Informe_Radiante_Identificador = ir.Identificador
                """,
                "ir.Meteoro_Identificador = %s",
                (meteoro_id,),
            )
            contadores["Trayectoria_estimada"] = self._eliminar_con_join(
                "Trayectoria_estimada",
                "te",
                f"""
                INNER JOIN `{self.tabla_radiante}` ir
                    ON te.Informe_Radiante_Identificador = ir.Identificador
                """,
                "ir.Meteoro_Identificador = %s",
                (meteoro_id,),
            )

            tabla_velocidades = self._tabla_velocidades_angulares()
            if tabla_velocidades:
                contadores[tabla_velocidades] = self._eliminar_con_join(
                    tabla_velocidades,
                    "va",
                    f"""
                    INNER JOIN `{self.tabla_radiante}` ir
                        ON va.Informe_Radiante_Identificador = ir.Identificador
                    """,
                    "ir.Meteoro_Identificador = %s",
                    (meteoro_id,),
                )

            query = f"DELETE FROM `{self.tabla_informe_z}` WHERE Meteoro_Identificador = %s"
            self.cursor.execute(query, (meteoro_id,))
            contadores["Informe_Z"] = self.cursor.rowcount

            query = f"DELETE FROM `{self.tabla_fotometria}` WHERE Meteoro_Identificador = %s"
            self.cursor.execute(query, (meteoro_id,))
            contadores["Informe_Fotometria"] = self.cursor.rowcount

            query = f"DELETE FROM `{self.tabla_radiante}` WHERE Meteoro_Identificador = %s"
            self.cursor.execute(query, (meteoro_id,))
            contadores["Informe_Radiante"] = self.cursor.rowcount

            contadores["Ecuacion_parametrica"] = self._eliminar_ecuaciones_huerfanas(ecuacion_ids)

            query = f"DELETE FROM `{self.tabla_meteoro}` WHERE Identificador = %s"
            self.cursor.execute(query, (meteoro_id,))
            contadores["Meteoro"] = self.cursor.rowcount

            if contadores["Meteoro"] != 1:
                raise RuntimeError(
                    f"Proteccion activada: se iban a borrar {contadores['Meteoro']} registros de Meteoro"
                )

            self.conexion.commit()
            return contadores
        except Exception:
            self.conexion.rollback()
            raise

    def _eliminar_ecuaciones_huerfanas(self, ecuacion_ids):
        if not ecuacion_ids or not self.tabla_existe("Ecuacion_parametrica"):
            return 0

        placeholders = ", ".join(["%s"] * len(ecuacion_ids))
        query = f"""
            DELETE ec
            FROM `Ecuacion_parametrica` ec
            WHERE ec.IdEc IN ({placeholders})
              AND NOT EXISTS (
                  SELECT 1
                  FROM `{self.tabla_informe_z}` iz
                  WHERE iz.Ecuacion_parametrica_IdEc = ec.IdEc
              )
        """
        self.cursor.execute(query, tuple(ecuacion_ids))
        return self.cursor.rowcount


def mostrar_estadisticas(bolido, estadisticas):
    print("\nBolido seleccionado")
    print("=" * 60)
    print(f"ID: {bolido['Identificador']}")
    print(f"Fecha: {bolido['Fecha']}")
    print(f"Hora: {bolido['Hora']}")

    print("\nRegistros que se eliminaran")
    print("=" * 60)
    total = 0
    for tabla, cantidad in estadisticas.items():
        if cantidad:
            print(f"  {tabla}: {cantidad}")
            total += cantidad
    print(f"  TOTAL: {total}")
    print("\nNo se eliminaran Observatorio, Lluvia ni otras tablas compartidas.")


def confirmar_o_cancelar(sin_confirmacion):
    if sin_confirmacion:
        return True

    print("\nEsta operacion no se puede deshacer.")
    respuesta = input("Escribe SI para confirmar el borrado de este unico bolido: ").strip()
    return respuesta.upper() == "SI"


def main():
    parser = argparse.ArgumentParser(
        description="Elimina un unico bolido y sus relaciones propias.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
Ejemplos:
  python3 eliminar_bolido.py 20250901
  python3 eliminar_bolido.py 2025-09-01
  python3 eliminar_bolido.py 20250901 --identificador 123
  python3 eliminar_bolido.py 20250901 --dry-run
        """,
    )
    parser.add_argument(
        "fecha",
        nargs="?",
        help="Fecha concreta del bolido: YYYYMMDD o YYYY-MM-DD",
    )
    parser.add_argument(
        "--identificador",
        type=int,
        help="Identificador exacto de Meteoro. Si se indica, debe pertenecer a la fecha.",
    )
    parser.add_argument(
        "--sin-confirmacion",
        action="store_true",
        help="No pedir confirmacion interactiva.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostrar lo que se borraria sin modificar la base de datos.",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("ELIMINADOR DE UNICO BOLIDO")
    print("=" * 60)

    eliminador = EliminadorBolido()
    fecha_input = args.fecha or input("Fecha del bolido (YYYYMMDD o YYYY-MM-DD): ").strip()
    fecha = eliminador.validar_fecha(fecha_input)
    if not fecha:
        sys.exit(1)

    if not eliminador.conectar_mysql():
        sys.exit(1)

    try:
        bolido = eliminador.elegir_bolido(fecha, args.identificador)
        if not bolido:
            sys.exit(0)

        estadisticas = eliminador.obtener_estadisticas(bolido["Identificador"])
        mostrar_estadisticas(bolido, estadisticas)

        if args.dry_run:
            print("\nDry-run activo: no se ha modificado la base de datos.")
            return

        if not confirmar_o_cancelar(args.sin_confirmacion):
            print("Operacion cancelada por el usuario.")
            return

        contadores = eliminador.eliminar_bolido(bolido)
        print("\nEliminacion completada correctamente.")
        print("Resumen:")
        for tabla, cantidad in contadores.items():
            if cantidad:
                print(f"  {tabla}: {cantidad}")
    except KeyboardInterrupt:
        print("\nOperacion interrumpida por el usuario.")
        sys.exit(1)
    except Exception as exc:
        print(f"\nError durante la eliminacion: {exc}")
        sys.exit(1)
    finally:
        eliminador.cerrar_conexion()


if __name__ == "__main__":
    main()
