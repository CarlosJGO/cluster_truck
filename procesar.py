
import glob
import os
import time
from pathlib import Path

import pandas as pd
import yaml
from dask.distributed import Client


BASE_DIR = Path(__file__).resolve().parent
RUTA_CONFIG = BASE_DIR / "cluster.yaml"


def procesar_archivo(nombre_archivo, carpeta_datos):
    ruta = os.path.join(carpeta_datos, nombre_archivo)
    df = pd.read_csv(ruta, usecols=["ciudad", "ventas"])
    ventas = df.groupby("ciudad")["ventas"].sum()
    return len(df), {str(ciudad): int(total) for ciudad, total in ventas.items()}


def main():
    with RUTA_CONFIG.open(encoding="utf-8") as archivo:
        config = yaml.safe_load(archivo)
    maquinas = config["maquinas"]
    coordinadora = maquinas[config["coordinadora"]]
    carpeta_datos = os.environ.get("CLUSTER_DATOS", coordinadora["datos"])
    scheduler_address = os.environ.get(
        "DASK_SCHEDULER_ADDRESS",
        f"tcp://{coordinadora['ip']}:{config['puerto_scheduler']}",
    )
    archivos = sorted(Path(path).name for path in glob.glob(os.path.join(carpeta_datos, "*.csv")))
    if not archivos:
        raise FileNotFoundError(f"No hay archivos CSV en {carpeta_datos}.")

    client = Client(scheduler_address)

    try:
        workers = client.scheduler_info()["workers"]
        if not workers:
            raise RuntimeError("No hay workers conectados al scheduler.")
        print("Workers conectados:", len(workers))
        print("Nombres:", [w.get("name") for w in workers.values()])
        print("Panel:", client.dashboard_link)

        inicio = time.time()
        disponibles = list(workers.items())
        tareas = []
        for indice, nombre_archivo in enumerate(archivos):
            direccion, datos_worker = disponibles[indice % len(disponibles)]
            nombre_worker = datos_worker["name"]
            if nombre_worker not in maquinas:
                raise ValueError(f"El worker '{nombre_worker}' no aparece en cluster.yaml.")
            carpeta_worker = maquinas[nombre_worker]["datos"]
            tareas.append(
                client.submit(
                    procesar_archivo,
                    nombre_archivo,
                    carpeta_worker,
                    workers=[direccion],
                    pure=False,
                )
            )

        resultados = client.gather(tareas)
        total_filas = sum(filas for filas, _ in resultados)
        ventas_por_ciudad = {}
        for _, ventas in resultados:
            for ciudad, total in ventas.items():
                ventas_por_ciudad[ciudad] = ventas_por_ciudad.get(ciudad, 0) + total

        print("\nTotal de filas:", total_filas)
        print("\nVentas por ciudad:")
        print(pd.Series(ventas_por_ciudad).sort_index())

        print(f"\nTiempo total: {time.time() - inicio:.2f} segundos")

    finally:
        client.close()


if __name__ == "__main__":
    main()
