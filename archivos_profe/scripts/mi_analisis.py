"""
TU ANÁLISIS · Completa cada TODO. Este archivo es lo que entregas.
Ejecuta:  python mi_analisis.py

Integrantes del grupo:
  -
  -
"""
import dask.dataframe as dd
import pandas as pd

from config import conectar, guardar_resultado, leer_ventas_limpias, ruta, titulo

pd.set_option("display.float_format", "{:,.0f}".format)

if __name__ == "__main__":
    client = conectar()

    v = leer_ventas_limpias().persist()
    ventas = v[~v["es_devolucion"]]
    devoluciones = v[v["es_devolucion"]]
    tiendas = pd.read_csv(ruta("tiendas.csv"))

    titulo("1 · Top 10 tiendas que más venden (con su ciudad)")
    # TODO: agrupa por ["tienda", "ciudad"], suma "monto" y quédate con las 10 mayores
    top_tiendas = None
    print(top_tiendas)

    titulo("2 · Ticket promedio por método de pago")
    # TODO: ¿qué método tiene el ticket más alto? ¿Por qué crees que pasa?
    ticket_pago = None
    print(ticket_pago)

    titulo("3 · Porcentaje de ventas con descuento, por mes")
    # TODO: ¿qué mes destaca? ¿Qué evento comercial ocurre ese mes?
    pct_descuento = None
    print(pct_descuento)

    titulo("4 · Top 5 tiendas por venta por metro cuadrado")
    # TODO: ¿son las mismas tiendas del punto 1? ¿Qué te dice eso?
    por_m2 = None
    print(por_m2)

    titulo("5 · Tasa de devolución por categoría (%)")
    # TODO: devoluciones por categoría / ventas por categoría * 100
    tasa_dev = None
    print(tasa_dev)

    titulo("RETO · Clientes registrados que compraron TODOS los meses")
    # TODO: quita las ventas sin cliente, quédate con (cliente_id, anio, mes) sin repetir,
    #       cuenta cuántos meses tiene cada cliente y filtra los que tienen todos.
    clientes_fieles = None
    print(clientes_fieles)

    # Guarda tus resultados para el informe, por ejemplo:
    # guardar_resultado(top_tiendas.to_frame("ventas_cop"), "1_top_tiendas.csv")

    client.close()
