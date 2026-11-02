
import os
import time
import dask.dataframe as dd
from dask.distributed import Client

# Los CSV se buscan dentro de la carpeta del proyecto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PATRON_CSV = os.path.join(BASE_DIR, "datos", "*.csv")


def main():
    client = Client("tcp://127.0.0.1:8786")

    try:
        workers = client.scheduler_info()["workers"]
        print("Workers conectados:", len(workers))
        print("Nombres:", [w.get("name") for w in workers.values()])
        print("Panel:", client.dashboard_link)

        inicio = time.time()

        # Dask organiza el trabajo por particiones
        df = dd.read_csv(PATRON_CSV)

        # Calcula las filas y las ventas por ciudad
        total_filas = df.shape[0].compute()
        resultado = df.groupby("ciudad")["ventas"].sum().compute()

        print("\nTotal de filas:", total_filas)
        print("\nVentas por ciudad:")
        print(resultado)

        print(f"\nTiempo total: {time.time() - inicio:.2f} segundos")

    finally:
        client.close()


if __name__ == "__main__":
    main()
