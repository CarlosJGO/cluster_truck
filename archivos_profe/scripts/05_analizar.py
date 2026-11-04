"""
PASO 6 · Responder preguntas de negocio con los datos limpios.
Ejecuta:  python 05_analizar.py
"""
import dask.dataframe as dd
import pandas as pd

from config import conectar, cronometro, guardar_resultado, leer_ventas_limpias, titulo

pd.set_option("display.float_format", "{:,.0f}".format)

if __name__ == "__main__":
    client = conectar()

    v = leer_ventas_limpias()
    # persist(): carga los datos en la MEMORIA de los workers una sola vez.
    # Las preguntas siguientes serán mucho más rápidas porque no releen el disco.
    v = v.persist()
    ventas = v[~v["es_devolucion"]]

    titulo("Pregunta A · ¿Cuánto vendió cada categoría?")
    with cronometro("ventas por categoría"):
        por_cat = ventas.groupby("categoria")["monto"].sum().compute()
    por_cat = por_cat.sort_values(ascending=False).to_frame("ventas_cop")
    por_cat["porcentaje"] = (por_cat["ventas_cop"] / por_cat["ventas_cop"].sum() * 100).round(1)
    print(por_cat.to_string())
    guardar_resultado(por_cat, "A_ventas_por_categoria.csv")

    titulo("Pregunta B · ¿Cuánto se vendió cada mes?")
    with cronometro("ventas por mes"):
        por_mes = ventas.groupby(["anio", "mes"])["monto"].sum().compute().sort_index()
    for (anio, mes), monto in por_mes.items():
        barra = "█" * int(monto / por_mes.max() * 40)
        print(f"  {anio}-{mes:02d}  {monto:>18,.0f}  {barra}")
    guardar_resultado(por_mes.to_frame("ventas_cop"), "B_ventas_por_mes.csv")

    titulo("Pregunta C · Varias cifras de una sola vez")
    with cronometro("resumen general"):
        total, n_ventas, ticket, clientes = dd.compute(
            ventas["monto"].sum(), ventas.shape[0], ventas["monto"].mean(),
            ventas["cliente_id"].nunique(),
        )
    print(f"  Ventas totales:      $ {total:,.0f} COP")
    print(f"  Número de ventas:      {n_ventas:,}")
    print(f"  Ticket promedio:     $ {ticket:,.0f} COP")
    print(f"  Clientes registrados:  {clientes:,}")

    # ------------------------------------------------------------------
    # TU TURNO (escríbelo en mi_analisis.py)
    #  1) Top 10 tiendas que más venden y su ciudad.   Pista: .nlargest(10)
    #  2) Ticket promedio por método de pago.          Pista: .groupby(...)["monto"].mean()
    #  3) % de ventas con descuento cada mes.          Pista: (ventas["descuento"] > 0)
    # ------------------------------------------------------------------

    client.close()
