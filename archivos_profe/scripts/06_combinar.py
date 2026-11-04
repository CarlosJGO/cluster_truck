"""
PASO 7 · Combinar las ventas con la tabla de tiendas.
Ejecuta:  python 06_combinar.py
"""
import dask.dataframe as dd
import pandas as pd

from config import conectar, cronometro, guardar_resultado, leer_ventas_limpias, ruta, titulo

pd.set_option("display.float_format", "{:,.0f}".format)

if __name__ == "__main__":
    client = conectar()

    ventas = leer_ventas_limpias(columnas=["tienda", "monto", "es_devolucion"])
    ventas = ventas[~ventas["es_devolucion"]]

    # tiendas.csv tiene solo 40 filas: la leemos con pandas normal.
    tiendas = pd.read_csv(ruta("tiendas.csv"))
    print(f"Tabla de tiendas: {len(tiendas)} filas")
    print(tiendas.head().to_string(index=False))

    titulo("Ventas por región")
    with cronometro("merge + groupby"):
        # Dask envía la tabla pequeña a cada worker y la une con su trozo de ventas
        # ("broadcast join"). No hace falta mover las ventas entre PCs.
        unidas = dd.merge(ventas, tiendas[["tienda", "region"]], on="tienda", how="left")
        por_region = unidas.groupby("region")["monto"].sum().compute().sort_values(ascending=False)
    print(por_region.to_string())
    guardar_resultado(por_region.to_frame("ventas_cop"), "D_ventas_por_region.csv")

    # ------------------------------------------------------------------
    # TU TURNO (escríbelo en mi_analisis.py)
    #  4) ¿Qué 5 tiendas venden más por metro cuadrado?
    #     Pista: agrupa las ventas por tienda, haz .compute() y luego un merge
    #     con pandas con la tabla tiendas. Divide monto / metros_cuadrados.
    #  5) Tasa de devolución por categoría = devoluciones / ventas × 100.
    # ------------------------------------------------------------------

    client.close()
