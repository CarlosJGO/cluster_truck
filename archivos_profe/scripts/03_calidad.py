"""
PASO 4 · Diagnosticar la calidad de los datos antes de limpiarlos.
Ejecuta:  python 03_calidad.py
"""
import dask.dataframe as dd

from config import conectar, cronometro, leer_ventas_csv, titulo

if __name__ == "__main__":
    client = conectar()
    ventas = leer_ventas_csv()

    titulo("Problemas encontrados")
    with cronometro("diagnóstico completo"):
        # dd.compute(...) calcula varias cosas leyendo los datos UNA sola vez
        filas, vacios, devoluciones, duplicadas, sin_cliente = dd.compute(
            ventas.shape[0],
            ventas["monto"].isna().sum(),
            (ventas["monto"] < 0).sum(),
            # Las ventas duplicadas siempre están en el mismo archivo mensual,
            # así que basta con buscarlas dentro de cada partición:
            ventas.map_partitions(lambda p: p.duplicated(subset="id_venta").sum()).sum(),
            ventas["cliente_id"].isna().sum(),
        )
    print(f"  Filas totales:              {filas:>12,}")
    print(f"  Montos vacíos:              {vacios:>12,}  ({vacios / filas:.2%})")
    print(f"  Devoluciones (monto < 0):   {devoluciones:>12,}  ({devoluciones / filas:.2%})")
    print(f"  Ventas duplicadas:          {duplicadas:>12,}  ({duplicadas / filas:.2%})")
    print(f"  Ventas sin cliente:         {sin_cliente:>12,}  ({sin_cliente / filas:.2%})  <- esto es normal")

    titulo("Categorías: deberían ser 8")
    conteo = ventas["categoria"].value_counts().compute()
    print(f"  Valores distintos encontrados: {len(conteo)}")
    print("  Algunos ejemplos mal escritos (las comillas muestran los espacios):")
    malas = [c for c in conteo.index if c != c.strip() or c != c.strip().capitalize()]
    for c in malas[:10]:
        print(f"    '{c}'  -> {conteo[c]:,} filas")

    client.close()
