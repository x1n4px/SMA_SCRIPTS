#!/usr/bin/env python3
"""
Script de prueba para verificar que el formato de fecha y hora
se procesa correctamente
"""

def separar_fecha(fecha):
    """
    Separa la fecha en mes, día y año
    """
    if fecha:
        fecha_str = str(fecha)
        # Asumiendo formato YYYY-MM-DD
        partes = fecha_str.split('-')
        if len(partes) == 3:
            año = int(partes[0])
            mes = int(partes[1])
            dia = int(partes[2])
            return mes, dia, año
        else:
            print(f"Formato de fecha no reconocido: {fecha_str}")
            return None, None, None
    return None, None, None

def separar_hora(hora):
    """
    Separa la hora en segundos, minutos y horas
    Maneja formato HH:MM:SS.fraccion (ej: 05:18:48.0364)
    """
    if hora:
        hora_str = str(hora)
        # Verificar si tiene fracciones de segundo
        if '.' in hora_str:
            # Separar parte de tiempo y fracción
            parte_tiempo, fraccion = hora_str.split('.')
            # Obtener milisegundos de la fracción
            milisegundos = int(fraccion[:3].ljust(3, '0'))  # Tomar los primeros 3 dígitos
        else:
            parte_tiempo = hora_str
            milisegundos = 0
        
        # Asumiendo formato HH:MM:SS
        partes = parte_tiempo.split(':')
        if len(partes) == 3:
            horas = int(partes[0])
            minutos = int(partes[1])
            segundos = int(float(partes[2]))  # float por si queda algo decimal
            return segundos, minutos, horas, milisegundos
        else:
            print(f"Formato de hora no reconocido: {hora_str}")
            return None, None, None, None
    return None, None, None, None

# Datos de ejemplo proporcionados
fecha_ejemplo1 = "2023-01-02"
hora_ejemplo1 = "05:18:48.0364"
hora_ejemplo2 = "02:38:53.4329"

print("="*50)
print("PRUEBA DE PROCESAMIENTO DE FORMATO")
print("="*50)

# Probar con fecha
print(f"\nFecha original: {fecha_ejemplo1}")
mes, dia, año = separar_fecha(fecha_ejemplo1)
print(f"Mes (mm): {mes:02d}")
print(f"Día (dd): {dia:02d}")
print(f"Año (aaaa): {año:04d}")

# Probar con primera hora
print(f"\nHora original: {hora_ejemplo1}")
segundos, minutos, horas, milisegundos = separar_hora(hora_ejemplo1)
print(f"Segundos (ss): {segundos:02d}")
print(f"Minutos (mm): {minutos:02d}")
print(f"Horas (hh): {horas:02d}")
print(f"Milisegundos: {milisegundos:03d}")

# Probar con segunda hora
print(f"\nHora original: {hora_ejemplo2}")
segundos, minutos, horas, milisegundos = separar_hora(hora_ejemplo2)
print(f"Segundos (ss): {segundos:02d}")
print(f"Minutos (mm): {minutos:02d}")
print(f"Horas (hh): {horas:02d}")
print(f"Milisegundos: {milisegundos:03d}")

print("\n" + "="*50)
print("FIN DE PRUEBA")
print("="*50)