"""
PASO 8 · Medir el rendimiento: CSV vs Parquet, y cluster vs una sola PC con pandas.
Ejecuta:  python 07_rendimiento.py
La misma pregunta (ventas por categoría) se responde de 4 formas distintas.
"""
import glob

import dask.dataframe as dd
import pandas as pd

from config import (
    MODO,
    conectar,
    cronometro,
    guardar_resultado,
    leer_ventas_csv,
    leer_ventas_limpias,
    ruta,
    titulo,
)

if __name__ == "__main__":
    client = conectar()
    tiempos = {}
    hilos = sum(w["nthreads"] for w in client.scheduler_info()["workers"].values())

    titulo("1 · Dask leyendo los CSV")
    with cronometro("Dask + CSV", tiempos):
        leer_ventas_csv().groupby("categoria")["monto"].sum().compute()

    titulo("2 · Dask leyendo Parquet (todas las columnas)")
    with cronometro("Dask + Parquet", tiempos):
        leer_ventas_limpias().groupby("categoria")["monto"].sum().compute()

    titulo("3 · Dask leyendo Parquet (solo 2 columnas)")
    with cronometro("Dask + Parquet 2 columnas", tiempos):
        leer_ventas_limpias(["categoria", "monto"]).groupby("categoria")["monto"].sum().compute()

    titulo("4 · pandas normal en esta PC (sin cluster)")
    with cronometro("pandas en 1 PC", tiempos):
        partes = []
        for archivo in sorted(glob.glob(ruta("ventas_*.csv"))):
            partes.append(pd.read_csv(archivo, usecols=["categoria", "monto"]))
        pd.concat(partes).groupby("categoria")["monto"].sum()

    titulo(f"Resumen (modo {MODO}, {hilos} hilos)")
    base = tiempos["pandas en 1 PC"]
    for nombre, seg in tiempos.items():
        print(f"  {nombre:<28} {seg:>7.2f} s   {base / seg:>5.1f}× vs pandas")
    tabla = pd.DataFrame({"segundos": tiempos})
    tabla["modo"], tabla["hilos"] = MODO, hilos
    guardar_resultado(tabla, f"tiempos_{MODO}.csv")

    client.close()
