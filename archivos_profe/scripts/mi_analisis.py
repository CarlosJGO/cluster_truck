import dask.dataframe as dd
import pandas as pd

from config import conectar, guardar_resultado, leer_ventas_limpias, ruta, titulo

pd.set_option("display.float_format", "{:,.2f}".format)

if __name__ == "__main__":
    client = conectar()

    v = leer_ventas_limpias().persist()
    ventas = v[~v["es_devolucion"]]
    devoluciones = v[v["es_devolucion"]]
    tiendas = pd.read_csv(ruta("tiendas.csv"))

    titulo("1 · Top 10 tiendas que más venden (con su ciudad)")

    top_tiendas = (
        ventas.groupby(["tienda", "ciudad"])["monto"]
        .sum()
        .nlargest(10)
        .compute()
    )
    print(top_tiendas)

    titulo("2 · Ticket promedio por método de pago")

    ticket_pago = (
        ventas.groupby("metodo_pago")["monto"]
        .mean()
        .compute()
        .sort_values(ascending=False)
    )
    print(ticket_pago)

    titulo("3 · Porcentaje de ventas con descuento, por mes")

    pct_descuento = (
        ventas.assign(con_descuento=ventas["descuento"] > 0)
        .groupby("mes")["con_descuento"]
        .mean()
        .compute()
        .mul(100)
        .sort_index()
    )
    print(pct_descuento)

    titulo("4 · Top 5 tiendas por venta por metro cuadrado")

    ventas_tienda = ventas.groupby("tienda")["monto"].sum().compute()

    tiendas["tienda"] = tiendas["tienda"].astype(ventas_tienda.index.dtype)

    por_m2 = (
        tiendas.set_index("tienda")
        .join(ventas_tienda.rename("ventas_totales"))
    )

    por_m2["ventas_por_m2"] = (
        por_m2["ventas_totales"] / por_m2["metros_cuadrados"]
    )

    por_m2 = por_m2["ventas_por_m2"].nlargest(5)
    print(por_m2)

    titulo("5 · Tasa de devolución por categoría (%)")

    total_ventas = ventas.groupby("categoria")["monto"].sum().compute()
    total_devoluciones = (
        devoluciones.groupby("categoria")["monto"]
        .sum()
        .compute()
    )

    tasa_dev = (
        total_devoluciones.div(total_ventas).mul(100).fillna(0)
        .sort_values(ascending=False)
    )
    print(tasa_dev)

    titulo("RETO · Clientes registrados que compraron TODOS los meses")

    clientes_fieles = (
        ventas[ventas["cliente_id"].notnull()]
        [["cliente_id", "anio", "mes"]]
        .drop_duplicates()
        .groupby("cliente_id")["mes"]
        .count()
        .compute()
    )

    clientes_fieles = clientes_fieles[clientes_fieles == 12]
    print(clientes_fieles)

    # Guarda tus resultados para el informe.
    guardar_resultado(top_tiendas.to_frame("ventas_cop"), "1_top_tiendas.csv")
    guardar_resultado(ticket_pago.to_frame("ticket_promedio"), "2_ticket_pago.csv")
    guardar_resultado(pct_descuento.to_frame("porcentaje"), "3_descuentos.csv")
    guardar_resultado(por_m2.to_frame("ventas_por_m2"), "4_ventas_m2.csv")
    guardar_resultado(tasa_dev.to_frame("tasa_devolucion"), "5_devoluciones.csv")
    guardar_resultado(clientes_fieles.to_frame("meses_comprados"), "6_clientes_fieles.csv")

    client.close()