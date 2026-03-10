#!/usr/bin/env python3
"""
Funciones auxiliares para formatear números decimales sin notación científica
"""

from decimal import Decimal, getcontext

# Establecer precisión alta para manejar números muy pequeños
getcontext().prec = 50

def formato_decimal(valor):
    """
    Convierte un valor numérico a formato decimal sin notación científica.
    Mantiene hasta 30 decimales de precisión.
    
    Args:
        valor: float, string o None
    
    Returns:
        String con el número en formato decimal o None si el valor es None/NULL
    """
    if valor is None or valor == "NULL":
        return None
    
    try:
        # Convertir a Decimal para evitar notación científica
        if isinstance(valor, str):
            # Si es string, verificar si ya tiene notación científica
            if 'e' in valor.lower() or 'E' in valor:
                # Convertir de notación científica a decimal
                num = Decimal(valor)
            else:
                # Es un string normal
                num = Decimal(valor)
        else:
            # Es un float o int
            # Usar string para evitar problemas de precisión de float
            num = Decimal(str(valor))
        
        # Formatear con precisión fija
        # Usar formato 'f' para forzar notación decimal
        resultado = format(num, 'f')
        
        # Si el número es muy largo, limitar a 30 decimales
        if '.' in resultado:
            parte_entera, parte_decimal = resultado.split('.')
            if len(parte_decimal) > 30:
                parte_decimal = parte_decimal[:30].rstrip('0')
                if parte_decimal:
                    resultado = f"{parte_entera}.{parte_decimal}"
                else:
                    resultado = parte_entera
        
        return resultado
        
    except Exception as e:
        # Si hay algún error, devolver el valor original como string
        return str(valor)

def procesar_float_para_sql(valor, es_nulo_permitido=True):
    """
    Procesa un valor float para inserción en SQL, evitando notación científica.
    
    Args:
        valor: El valor a procesar (string, float o None)
        es_nulo_permitido: Si True, devuelve None para valores NULL/None
    
    Returns:
        float formateado o None
    """
    if es_nulo_permitido and (valor is None or valor == "NULL" or valor == ""):
        return None
    
    try:
        # Primero convertir a decimal para mantener precisión
        if isinstance(valor, str):
            if valor == "NULL" or valor == "":
                return None if es_nulo_permitido else 0.0
            # Convertir string a Decimal
            num = Decimal(valor)
        else:
            # Convertir float/int a Decimal via string
            num = Decimal(str(valor))
        
        # Convertir de vuelta a float usando el string decimal
        return float(format(num, 'f'))
        
    except Exception:
        if es_nulo_permitido:
            return None
        return 0.0

# Función de prueba
if __name__ == "__main__":
    # Casos de prueba
    valores_prueba = [
        4.69629718876025e-08,
        "4.69629718876025e-08",
        0.00000134889896050256,
        "0.00000134889896050256",
        1.23456789e-15,
        0.123456789,
        123456789,
        "NULL",
        None
    ]
    
    print("Pruebas de formato_decimal:")
    for valor in valores_prueba:
        resultado = formato_decimal(valor)
        print(f"  {valor} -> {resultado}")
    
    print("\nPruebas de procesar_float_para_sql:")
    for valor in valores_prueba:
        resultado = procesar_float_para_sql(valor)
        print(f"  {valor} -> {resultado}")