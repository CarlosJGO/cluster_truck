"""
PASO 3 · Explorar los datos: cuántos hay, cómo son y cómo los divide Dask.
Ejecuta:  python 02_explorar.py
"""
import pandas as pd

from config import conectar, cronometro, leer_ventas_csv, titulo

pd.set_option("display.width", 140)
pd.set_option("display.max_columns", 20)

if __name__ == "__main__":
    client = conectar()

    # Esto NO lee los archivos todavía: solo prepara un "plan" (evaluación perezosa)
    ventas = leer_ventas_csv()

    titulo("Estructura")
    print(f"  Particiones (trozos en que Dask divide los datos): {ventas.npartitions}")
    print("  Columnas y tipos:")
    for col, tipo in ventas.dtypes.items():
        print(f"    {col:<16} {tipo}")

    titulo("Primeras 5 filas")
    with cronometro("head()"):
        print(ventas.head(5).to_string())  # solo lee el primer trozo: es rápido

    titulo("Tamaño total")
    with cronometro("len()"):
        filas = len(ventas)  # tiene que leer TODOS los archivos: tarda más
    print(f"  Filas totales: {filas:,}")

    titulo("Estadísticas de la columna 'monto'")
    with cronometro("describe()"):
        print(ventas["monto"].describe().compute().map("{:,.0f}".format).to_string())

    # ------------------------------------------------------------------
    # TU TURNO
    #  a) ¿Cuántas tiendas distintas hay?      Pista: ventas["tienda"].nunique()
    #  b) ¿Cuántas ventas hubo en cada ciudad? Pista: ventas["ciudad"].value_counts()
    #  Recuerda terminar con .compute() para obtener el resultado.
    # ------------------------------------------------------------------

    client.close()
