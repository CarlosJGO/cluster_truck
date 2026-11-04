"""
Configuración común de la práctica.
Edita SOLO la parte marcada. El resto lo usan los demás scripts.
"""

import glob
import logging
import os
import re
import shutil
import socket
import time
from contextlib import contextmanager
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import yaml
from dask import delayed
from dask.distributed import Client, LocalCluster, get_client, get_worker

# ======================= EDITA SOLO ESTA PARTE =======================

# "cluster" = usar el scheduler configurado en cluster.yaml
# "local"   = practicar en tu propia PC (sin cluster)
MODO = os.environ.get("PRACTICA_MODO", "cluster")

# Las rutas de datos son locales a cada PC y se toman de cluster.yaml.
ROOT_DIR = Path(__file__).resolve().parents[2]
CLUSTER_CONFIG_PATH = Path(__file__).resolve().with_name("cluster.yaml")
if not CLUSTER_CONFIG_PATH.is_file():
    CLUSTER_CONFIG_PATH = ROOT_DIR / "cluster.yaml"
with CLUSTER_CONFIG_PATH.open(encoding="utf-8") as archivo:
    CLUSTER_CONFIG = yaml.safe_load(archivo)

COORDINADORA = CLUSTER_CONFIG["coordinadora"]
PUERTO_SCHEDULER = CLUSTER_CONFIG["puerto_scheduler"]
SCHEDULER = os.environ.get(
    "PRACTICA_SCHEDULER",
    os.environ.get(
        "DASK_SCHEDULER_ADDRESS",
        f"tcp://{CLUSTER_CONFIG['maquinas'][COORDINADORA]['ip']}:{PUERTO_SCHEDULER}",
    ),
)
DATOS_POR_WORKER = {
    name: machine["datos"] for name, machine in CLUSTER_CONFIG["maquinas"].items()
}


def _detectar_maquina_local():
    coordinator_ip = CLUSTER_CONFIG["maquinas"][COORDINADORA]["ip"]
    connection = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        connection.connect((coordinator_ip, PUERTO_SCHEDULER))
        local_ip = connection.getsockname()[0]
    except OSError:
        return None
    finally:
        connection.close()
    return next(
        (name for name, machine in CLUSTER_CONFIG["maquinas"].items() if machine["ip"] == local_ip),
        None,
    )


_MAQUINA_LOCAL = _detectar_maquina_local()
_DATOS_LOCAL_DEFAULT = DATOS_POR_WORKER.get(_MAQUINA_LOCAL, "datos")
DATOS_CLUSTER = os.path.expandvars(
    os.path.expanduser(os.environ.get("PRACTICA_DATOS", _DATOS_LOCAL_DEFAULT))
)
DATOS_LOCAL = DATOS_CLUSTER

# =====================================================================

logging.getLogger("distributed.shuffle").setLevel(logging.ERROR)  # oculta avisos internos

# Tipos de cada columna del CSV. Indicarlos evita errores y acelera la lectura.
TIPOS = {
    "id_venta": "string", "tienda": "string", "ciudad": "string",
    "categoria": "string", "producto": "string", "metodo_pago": "string",
    "cliente_id": "string", "cantidad": "int64", "precio_unitario": "int64",
    "descuento": "float64", "monto": "float64",
}


def carpeta_datos():
    """Devuelve la carpeta local del worker que ejecuta la tarea."""
    if MODO == "cluster":
        try:
            worker_name = get_worker().name
        except ValueError:
            return DATOS_CLUSTER
        if worker_name in DATOS_POR_WORKER:
            return os.path.expandvars(os.path.expanduser(DATOS_POR_WORKER[worker_name]))
        unique_paths = set(DATOS_POR_WORKER.values())
        if len(unique_paths) == 1:
            return os.path.expandvars(os.path.expanduser(unique_paths.pop()))
        raise RuntimeError(
            f"El worker '{worker_name}' no tiene una ruta de datos configurada en cluster.yaml."
        )
    return DATOS_CLUSTER


def ruta(*partes):
    """Une la carpeta de datos con un nombre de archivo o carpeta."""
    return os.path.join(carpeta_datos(), *partes)


def conectar():
    """Conecta con el cluster (o crea uno local) y muestra un resumen."""
    if MODO == "cluster":
        client = Client(SCHEDULER, timeout="15s")
    else:
        client = Client(LocalCluster(n_workers=4, threads_per_worker=2))
    client.upload_file(str(CLUSTER_CONFIG_PATH), load=False)
    client.upload_file(str(Path(__file__).resolve()))
    workers = client.scheduler_info()["workers"]
    hilos = sum(w["nthreads"] for w in workers.values())
    print(f"Conectado en modo '{MODO}': {len(workers)} workers, {hilos} hilos")
    print(f"Panel web: {client.dashboard_link}")
    print(f"Datos en:  {carpeta_datos()}")
    return client


def leer_ventas_csv():
    """Lee todos los CSV mensuales como un DataFrame de Dask (sin calcular nada aún)."""
    archivos = sorted(glob.glob(ruta("ventas_*.csv")))
    if not archivos:
        raise FileNotFoundError(f"No se encontraron archivos ventas_*.csv en {DATOS_CLUSTER}.")
    metadata = pd.read_csv(
        archivos[0], dtype=TIPOS, parse_dates=["fecha"], nrows=1
    ).iloc[:0]
    return dd.from_map(
        _leer_csv_en_worker,
        [os.path.basename(archivo) for archivo in archivos],
        meta=metadata,
        label="leer-ventas-locales",
        enforce_metadata=True,
    )


def _leer_csv_en_worker(nombre_archivo):
    return pd.read_csv(
        os.path.join(carpeta_datos(), nombre_archivo),
        dtype=TIPOS,
        parse_dates=["fecha"],
    )


def leer_ventas_limpias(columnas=None):
    """Lee las particiones Parquet desde los workers que las guardaron."""
    client = get_client()
    workers = client.scheduler_info()["workers"]
    archivos_por_worker = client.run(_listar_parquet_en_worker)
    meta = _meta_ventas_limpias()
    if columnas:
        meta = meta[columnas]

    particiones = []
    rutas_vistas = set()
    for address, archivos in archivos_por_worker.items():
        if address not in workers:
            continue
        ruta_worker = os.path.normcase(os.path.abspath(archivos["ruta"]))
        if ruta_worker in rutas_vistas:
            continue
        rutas_vistas.add(ruta_worker)
        worker_name = workers[address]["name"]
        for nombre_archivo in archivos["archivos"]:
            future = client.submit(
                _leer_parquet_en_worker,
                nombre_archivo,
                columnas,
                workers=[worker_name],
                allow_other_workers=False,
            )
            particiones.append(delayed(future))
    if not particiones:
        raise FileNotFoundError("No se encontraron particiones en ventas_limpias de los workers.")
    return dd.from_delayed(particiones, meta=meta, verify_meta=True)


def _meta_ventas_limpias():
    archivos = sorted(glob.glob(ruta("ventas_*.csv")))
    if not archivos:
        raise FileNotFoundError(f"No se encontraron archivos ventas_*.csv en {DATOS_CLUSTER}.")
    metadata = pd.read_csv(
        archivos[0], dtype=TIPOS, parse_dates=["fecha"], nrows=1
    ).iloc[:0]
    metadata["anio"] = pd.Series(dtype="int64")
    metadata["mes"] = pd.Series(dtype="int64")
    metadata["es_devolucion"] = pd.Series(dtype="bool")
    return metadata


def _listar_parquet_en_worker():
    carpeta = Path(carpeta_datos()) / "ventas_limpias"
    return {
        "ruta": str(carpeta.resolve()),
        "archivos": sorted(path.name for path in carpeta.glob("part-*.parquet")),
    } if carpeta.is_dir() else {"ruta": str(carpeta.resolve()), "archivos": []}


def _leer_parquet_en_worker(nombre_archivo, columnas=None):
    ruta_archivo = Path(carpeta_datos()) / "ventas_limpias" / nombre_archivo
    return pd.read_parquet(ruta_archivo, columns=columnas)


def preparar_parquet_en_worker():
    carpeta = Path(carpeta_datos()) / "ventas_limpias"
    if carpeta.exists():
        shutil.rmtree(carpeta)
    carpeta.mkdir(parents=True, exist_ok=True)


def escribir_parquet_en_worker(particion, partition_info=None):
    carpeta = Path(carpeta_datos()) / "ventas_limpias"
    carpeta.mkdir(parents=True, exist_ok=True)
    worker_name = re.sub(r"[^A-Za-z0-9_-]+", "_", str(get_worker().name))
    nombre = f"part-{worker_name}-{partition_info['number']:05d}.parquet"
    particion.to_parquet(carpeta / nombre, index=False)
    return pd.Series([nombre], name="archivo", dtype="object")


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
