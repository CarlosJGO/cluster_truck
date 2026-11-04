"""
Generador de datos de ventas para practicar con el cluster de Dask.

Crea un archivo CSV por mes (ventas_AAAA_MM.csv) con ventas simuladas de una
cadena de tiendas en Colombia, más dos tablas de apoyo (tiendas.csv y
productos.csv). Los datos traen "suciedad" a propósito (montos vacíos,
devoluciones, filas duplicadas, categorías mal escritas) para practicar limpieza.

Usa una semilla fija: todos los que ejecuten el mismo tamaño obtienen
exactamente los mismos datos, así se pueden comparar resultados.

Uso:
    python generar_datos.py --tamano pequeno            (~100 MB, para probar)
    python generar_datos.py --tamano mediano            (~1 GB, clase con cluster)
    python generar_datos.py --tamano grande             (~5 GB, 24 archivos)
    python generar_datos.py --tamano mediano --salida "\\\\192.168.1.10\\datos"

Requisitos: numpy y pandas.
"""

import argparse
import calendar
import time
from pathlib import Path

import numpy as np
import pandas as pd

SEMILLA = 2025

TAMANOS = {
    "pequeno": {"anios": [2025], "filas_mes": 75_000},
    "mediano": {"anios": [2025], "filas_mes": 750_000},
    "grande": {"anios": [2024, 2025], "filas_mes": 1_900_000},
}

# ---------------------------------------------------------------- catálogos
CIUDADES = {
    # ciudad: (región, peso de población)
    "Bogotá": ("Andina", 30),
    "Medellín": ("Andina", 14),
    "Cali": ("Pacífica", 11),
    "Barranquilla": ("Caribe", 9),
    "Cartagena": ("Caribe", 6),
    "Bucaramanga": ("Andina", 5),
    "Pereira": ("Andina", 4),
    "Manizales": ("Andina", 3),
    "Santa Marta": ("Caribe", 3),
    "Riohacha": ("Caribe", 2),
    "Villavicencio": ("Orinoquía", 3),
    "Pasto": ("Pacífica", 2),
}

PRODUCTOS = {
    # categoría: [(producto, precio base en COP), ...]
    "Electrónica": [
        ("Audífonos inalámbricos", 189_000), ("Celular gama media", 1_150_000),
        ("Cargador USB-C", 59_000), ("Parlante Bluetooth", 249_000),
        ("Televisor 50 pulgadas", 2_090_000), ("Portátil 14 pulgadas", 2_850_000),
        ("Mouse inalámbrico", 69_000), ("Reloj inteligente", 459_000),
    ],
    "Hogar": [
        ("Juego de sábanas", 129_000), ("Olla a presión", 219_000),
        ("Licuadora", 279_000), ("Toallas x2", 79_000),
        ("Lámpara LED", 89_000), ("Juego de vasos x6", 49_000),
        ("Freidora de aire", 389_000), ("Almohada", 65_000),
    ],
    "Ropa": [
        ("Camiseta básica", 45_000), ("Jean clásico", 139_000),
        ("Chaqueta impermeable", 259_000), ("Medias x3", 29_000),
        ("Vestido casual", 159_000), ("Tenis urbanos", 289_000),
        ("Sudadera", 119_000), ("Gorra", 39_000),
    ],
    "Alimentos": [
        ("Café 500 g", 24_000), ("Arroz 5 kg", 21_000),
        ("Aceite 1 L", 14_500), ("Chocolate de mesa", 9_800),
        ("Panela x4", 12_000), ("Atún x3", 22_500),
        ("Leche x6", 27_000), ("Galletas surtidas", 8_900),
    ],
    "Deportes": [
        ("Balón de fútbol", 99_000), ("Mancuernas 5 kg", 149_000),
        ("Bicicleta urbana", 1_290_000), ("Tapete de yoga", 79_000),
        ("Guayos", 239_000), ("Termo deportivo", 45_000),
    ],
    "Juguetes": [
        ("Rompecabezas 1000 piezas", 69_000), ("Carro a control remoto", 189_000),
        ("Muñeca", 99_000), ("Bloques de construcción", 159_000),
        ("Juego de mesa", 119_000),
    ],
    "Libros": [
        ("Novela", 59_000), ("Libro de cocina", 89_000),
        ("Cuaderno x5", 32_000), ("Libro infantil", 39_000),
    ],
    "Belleza": [
        ("Protector solar", 54_000), ("Champú", 26_000),
        ("Perfume", 249_000), ("Crema hidratante", 47_000),
        ("Kit de maquillaje", 135_000),
    ],
}

# 200.000 clientes posibles; los primeros compran más que los últimos
POP_CLIENTES = 1 / np.arange(1, 200_001) ** 0.7
POP_CLIENTES /= POP_CLIENTES.sum()

METODOS_PAGO =["Efectivo", "Tarjeta débito", "Tarjeta crédito", "Transferencia"]
PESO_PAGO = [0.34, 0.30, 0.20, 0.16]
PESO_PAGO_CARO = [0.08, 0.17, 0.62, 0.13]

# Probabilidad de devolución según la categoría (la ropa se devuelve más)
TASA_DEVOLUCION = {"Ropa": 0.014, "Electrónica": 0.009, "Juguetes": 0.006, "Deportes": 0.006,
                   "Hogar": 0.005, "Belleza": 0.003, "Libros": 0.002, "Alimentos": 0.001}

# Más ventas en diciembre, noviembre (Black Friday) y junio (prima de mitad de año)
FACTOR_MES = {1: 0.80, 2: 0.82, 3: 0.92, 4: 0.90, 5: 1.00, 6: 1.12,
              7: 0.98, 8: 0.95, 9: 0.93, 10: 1.00, 11: 1.25, 12: 1.60}


def crear_catalogos(rng):
    filas = []
    for cat, items in PRODUCTOS.items():
        for nombre, precio in items:
            filas.append((cat, nombre, precio))
    productos = pd.DataFrame(filas, columns=["categoria", "producto", "precio_base"])
    productos.insert(0, "id_producto", [f"P{i:03d}" for i in range(1, len(productos) + 1)])

    nombres = list(CIUDADES)
    pesos = np.array([CIUDADES[c][1] for c in nombres], dtype=float)
    n_tiendas = 40
    ciudades = list(nombres)  # al menos una tienda por ciudad
    ciudades += list(rng.choice(nombres, size=n_tiendas - len(nombres), p=pesos / pesos.sum()))
    tiendas = pd.DataFrame({
        "tienda": [f"T{i:03d}" for i in range(1, n_tiendas + 1)],
        "ciudad": ciudades,
    })
    tiendas["region"] = tiendas["ciudad"].map(lambda c: CIUDADES[c][0])
    tiendas["metros_cuadrados"] = (rng.integers(12, 90, size=n_tiendas) * 10).astype(int)
    tiendas["anio_apertura"] = rng.integers(2005, 2024, size=n_tiendas)
    return productos, tiendas


def generar_mes(rng, anio, mes, filas_base, productos, tiendas, pop_tiendas, pop_productos):
    n = int(filas_base * FACTOR_MES[mes])
    dias = calendar.monthrange(anio, mes)[1]

    # Fechas: los fines de semana venden más
    fechas_dia = pd.date_range(f"{anio}-{mes:02d}-01", periods=dias, freq="D")
    peso_dia = np.where(fechas_dia.dayofweek >= 5, 1.35, 1.0)
    dia = rng.choice(dias, size=n, p=peso_dia / peso_dia.sum())
    segundos = rng.integers(8 * 3600, 21 * 3600, size=n)  # tiendas abiertas 8:00 a 21:00
    fecha = fechas_dia.values[dia] + segundos.astype("timedelta64[s]")

    t_idx = rng.choice(len(tiendas), size=n, p=pop_tiendas)
    p_idx = rng.choice(len(productos), size=n, p=pop_productos)

    categoria = productos["categoria"].to_numpy()[p_idx].astype(object)
    precio_base = productos["precio_base"].to_numpy()[p_idx]
    es_alimento = categoria == "Alimentos"

    cantidad = 1 + rng.poisson(np.where(es_alimento, 2.0, 0.35))
    precio = np.round(precio_base * rng.normal(1.0, 0.03, size=n) / 100) * 100

    prob_desc = 0.45 if mes == 11 else 0.25
    descuento = np.where(rng.random(n) < prob_desc,
                         rng.choice([0.05, 0.10, 0.15, 0.20, 0.30], size=n), 0.0)
    monto = np.round(cantidad * precio * (1 - descuento))

    # Clientes: unos pocos compran mucho; ~18 % de ventas sin cliente registrado
    cliente_num = rng.choice(len(POP_CLIENTES), size=n, p=POP_CLIENTES) + 1
    cliente = np.array(pd.Series(cliente_num).map("C{:06d}".format), dtype=object)
    cliente[rng.random(n) < 0.18] = None

    df = pd.DataFrame({
        "fecha": fecha,
        "tienda": tiendas["tienda"].to_numpy()[t_idx],
        "ciudad": tiendas["ciudad"].to_numpy()[t_idx],
        "categoria": categoria,
        "producto": productos["producto"].to_numpy()[p_idx],
        "cantidad": cantidad,
        "precio_unitario": precio.astype(np.int64),
        "descuento": descuento,
        "monto": monto,
        # Las compras caras se pagan más con tarjeta de crédito
        "metodo_pago": np.where(precio_base >= 400_000,
                                rng.choice(METODOS_PAGO, size=n, p=PESO_PAGO_CARO),
                                rng.choice(METODOS_PAGO, size=n, p=PESO_PAGO)),
        "cliente_id": cliente,
    })

    # ---------- suciedad intencional ----------
    # 1) Devoluciones: cantidad y monto negativos (0,1 % a 1,4 % según la categoría)
    dev = rng.random(n) < pd.Series(categoria).map(TASA_DEVOLUCION).to_numpy()
    df.loc[dev, "cantidad"] *= -1
    df.loc[dev, "monto"] *= -1
    # 2) Montos vacíos por fallas del sistema de caja (0,8 %)
    df.loc[rng.random(n) < 0.008, "monto"] = np.nan
    # 3) Categorías mal escritas (1 %): mayúsculas, minúsculas o espacios de más
    sucia = np.flatnonzero(rng.random(n) < 0.01)
    variantes = [str.upper, str.lower, lambda s: s + " ", lambda s: " " + s]
    eleccion = rng.integers(0, len(variantes), size=len(sucia))
    df.loc[sucia, "categoria"] = [variantes[e](c) for e, c in zip(eleccion, categoria[sucia])]
    # Ordenar por fecha y numerar las ventas
    df = df.sort_values("fecha", kind="stable", ignore_index=True)
    df.insert(0, "id_venta", pd.Series(np.arange(1, n + 1)).map(f"V{anio}{mes:02d}-{{:07d}}".format))

    # 4) Filas duplicadas por doble envío del datáfono (0,3 %): fila idéntica, mismo id_venta
    dup = df.iloc[np.flatnonzero(rng.random(n) < 0.003)]
    df = pd.concat([df, dup]).sort_index(kind="stable").reset_index(drop=True)
    return df


def main():
    ap = argparse.ArgumentParser(description="Genera CSV de ventas para practicar con Dask.")
    ap.add_argument("--tamano", choices=TAMANOS, default="pequeno")
    ap.add_argument("--salida", default="datos", help="Carpeta destino (puede ser una ruta de red)")
    ap.add_argument("--filas-mes", type=int, help="Sobrescribe las filas base por mes")
    args = ap.parse_args()

    cfg = TAMANOS[args.tamano]
    filas_mes = args.filas_mes or cfg["filas_mes"]
    salida = Path(args.salida)
    salida.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(SEMILLA)
    productos, tiendas = crear_catalogos(rng)
    productos.to_csv(salida / "productos.csv", index=False, encoding="utf-8")
    tiendas.to_csv(salida / "tiendas.csv", index=False, encoding="utf-8")

    pop_t = rng.lognormal(0, 0.6, size=len(tiendas)); pop_t /= pop_t.sum()
    pop_p = 1 / np.arange(1, len(productos) + 1) ** 0.6
    pop_p = rng.permutation(pop_p); pop_p /= pop_p.sum()

    print(f"Generando tamaño '{args.tamano}' en {salida.resolve()}")
    inicio, total_filas, total_bytes = time.time(), 0, 0
    for anio in cfg["anios"]:
        for mes in range(1, 13):
            t0 = time.time()
            df = generar_mes(rng, anio, mes, filas_mes, productos, tiendas, pop_t, pop_p)
            ruta = salida / f"ventas_{anio}_{mes:02d}.csv"
            df.to_csv(ruta, index=False, encoding="utf-8", date_format="%Y-%m-%d %H:%M:%S")
            total_filas += len(df)
            total_bytes += ruta.stat().st_size
            print(f"  {ruta.name}: {len(df):>10,} filas  {ruta.stat().st_size / 1e6:7.1f} MB"
                  f"  ({time.time() - t0:.1f} s)")

    print(f"Listo: {total_filas:,} filas, {total_bytes / 1e9:.2f} GB en {time.time() - inicio:.0f} s")


if __name__ == "__main__":
    main()
