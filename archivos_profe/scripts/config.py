"""
Configuración común de la práctica.
Edita SOLO la parte marcada. El resto lo usan los demás scripts.
"""

# ======================= EDITA SOLO ESTA PARTE =======================

# "cluster" = usar el cluster de la clase
# "local"   = practicar en tu propia PC (sin cluster)
MODO = "cluster"

# Dirección del scheduler (te la da tu profesor)
SCHEDULER = "tcp://192.168.1.10:8786"

# Carpeta de datos de tu grupo, en la carpeta compartida.
# Usa SIEMPRE la ruta de red completa (\\IP\carpeta), nunca una letra como Z:
DATOS_CLUSTER = r"\\192.168.1.10\datos\grupo01"

# Carpeta de datos cuando trabajas en modo "local"
DATOS_LOCAL = r"datos"

# =====================================================================

import logging
import os
import time
from contextlib import contextmanager

import dask.dataframe as dd
from dask.distributed import Client, LocalCluster

logging.getLogger("distributed.shuffle").setLevel(logging.ERROR)  # oculta avisos internos

# Tipos de cada columna del CSV. Indicarlos evita errores y acelera la lectura.
TIPOS = {
    "id_venta": "string", "tienda": "string", "ciudad": "string",
    "categoria": "string", "producto": "string", "metodo_pago": "string",
    "cliente_id": "string", "cantidad": "int64", "precio_unitario": "int64",
    "descuento": "float64", "monto": "float64",
}


def carpeta_datos():
    """Devuelve la carpeta de datos según el modo elegido."""
    return DATOS_CLUSTER if MODO == "cluster" else os.path.abspath(DATOS_LOCAL)


def ruta(*partes):
    """Une la carpeta de datos con un nombre de archivo o carpeta."""
    return os.path.join(carpeta_datos(), *partes)


def conectar():
    """Conecta con el cluster (o crea uno local) y muestra un resumen."""
    if MODO == "cluster":
        client = Client(SCHEDULER, timeout="15s")
    else:
        client = Client(LocalCluster(n_workers=4, threads_per_worker=2))
    workers = client.scheduler_info()["workers"]
    hilos = sum(w["nthreads"] for w in workers.values())
    print(f"Conectado en modo '{MODO}': {len(workers)} workers, {hilos} hilos")
    print(f"Panel web: {client.dashboard_link}")
    print(f"Datos en:  {carpeta_datos()}")
    return client


def leer_ventas_csv():
    """Lee todos los CSV mensuales como un DataFrame de Dask (sin calcular nada aún)."""
    return dd.read_csv(ruta("ventas_*.csv"), dtype=TIPOS,
                       parse_dates=["fecha"], blocksize="64MB")


def leer_ventas_limpias(columnas=None):
    """Lee los datos limpios en Parquet que crea 04_limpiar.py."""
    return dd.read_parquet(ruta("ventas_limpias"), columns=columnas)


def titulo(texto):
    print(f"\n{'=' * 64}\n {texto}\n{'=' * 64}")


@contextmanager
def cronometro(nombre, tiempos=None):
    """Mide cuánto tarda el bloque de código que encierra."""
    t0 = time.time()
    yield
    segundos = time.time() - t0
    print(f"  ⏱  {nombre}: {segundos:.2f} s")
    if tiempos is not None:
        tiempos[nombre] = round(segundos, 2)


def guardar_resultado(df, nombre):
    """Guarda un resultado pequeño (pandas) en la carpeta 'resultados' de tu PC."""
    os.makedirs("resultados", exist_ok=True)
    archivo = os.path.join("resultados", nombre)
    df.to_csv(archivo, encoding="utf-8-sig")  # utf-8-sig para que Excel muestre bien las tildes
    print(f"  Guardado en {archivo}")


# Permite cambiar la carpeta sin editar el archivo (opcional, para el profesor)
if os.environ.get("PRACTICA_DATOS"):
    DATOS_CLUSTER = DATOS_LOCAL = os.environ["PRACTICA_DATOS"]
if os.environ.get("PRACTICA_MODO"):
    MODO = os.environ["PRACTICA_MODO"]
