
import sys
import time
from pathlib import Path

import dask.dataframe as dd


BASE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = BASE_DIR / "archivos_profe" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from config import conectar, leer_ventas_csv


def main():
    client = conectar()

    try:
        workers = client.scheduler_info()["workers"]
        if not workers:
            raise RuntimeError("No hay workers conectados al scheduler.")
        print("Workers conectados:", len(workers))
        print("Nombres:", [w.get("name") for w in workers.values()])
        print("Panel:", client.dashboard_link)

        inicio = time.time()
        ventas = leer_ventas_csv()
        total_filas, ventas_por_ciudad = dd.compute(
            ventas.shape[0], ventas.groupby("ciudad")["monto"].sum()
        )

        print("\nTotal de filas:", total_filas)
        print("\nVentas por ciudad:")
        print(ventas_por_ciudad.sort_index())

        print(f"\nTiempo total: {time.time() - inicio:.2f} segundos")

    finally:
        client.close()


if __name__ == "__main__":
    main()
