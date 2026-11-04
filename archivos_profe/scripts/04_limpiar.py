"""
PASO 5 · Limpiar los datos y guardarlos en formato Parquet.
Ejecuta:  python 04_limpiar.py
Crea la carpeta 'ventas_limpias' dentro de la carpeta de datos de tu grupo.
"""
from dask.distributed import progress

from config import conectar, cronometro, leer_ventas_csv, leer_ventas_limpias, ruta, titulo

if __name__ == "__main__":
    client = conectar()
    ventas = leer_ventas_csv()

    # 1. Quitar ventas sin monto (fallas del sistema de caja)
    ventas = ventas.dropna(subset=["monto"])

    # 2. Quitar duplicados dentro de cada mes (cada partición)
    ventas = ventas.map_partitions(lambda p: p.drop_duplicates(subset="id_venta"))

    # 3. Arreglar categorías: quitar espacios y dejar "Primera letra en mayúscula"
    ventas["categoria"] = ventas["categoria"].str.strip().str.capitalize()

    # 4. Columnas nuevas que usaremos en el análisis
    ventas["anio"] = ventas["fecha"].dt.year
    ventas["mes"] = ventas["fecha"].dt.month
    ventas["es_devolucion"] = ventas["monto"] < 0

    titulo("Guardando en Parquet (lo hacen los workers en paralelo)")
    with cronometro("limpiar y escribir"):
        tarea = ventas.to_parquet(ruta("ventas_limpias"), write_index=False,
                                  overwrite=True, compute=False)
        futuro = client.compute(tarea)
        progress(futuro)  # barra de progreso en la consola
        futuro.result()
    print()

    titulo("Verificación")
    limpias = leer_ventas_limpias()
    filas, devol, categorias = limpias.shape[0].compute(), limpias["es_devolucion"].sum().compute(), \
        sorted(limpias["categoria"].unique().compute())
    print(f"  Filas limpias:   {filas:,}")
    print(f"  De ellas, devoluciones: {devol:,}")
    print(f"  Categorías ({len(categorias)}): {', '.join(categorias)}")

    client.close()
