"""
PASO 1 · Conectarse al cluster y comprobar que todos los workers responden.
Ejecuta:  python 01_conectar.py
"""
import socket
from collections import Counter

from config import conectar, titulo


def nombre_de_la_pc(_):
    # Esta función NO se ejecuta en tu PC: se ejecuta en los workers.
    return socket.gethostname()


if __name__ == "__main__":
    client = conectar()

    titulo("Workers del cluster")
    for direccion, w in client.scheduler_info()["workers"].items():
        memoria_gb = w["memory_limit"] / 1e9
        print(f"  {w['name']!s:<22} {direccion:<26} hilos={w['nthreads']}  memoria={memoria_gb:.1f} GB")

    titulo("Prueba: 200 tareas repartidas")
    futuros = client.map(nombre_de_la_pc, range(200))
    conteo = Counter(client.gather(futuros))
    for pc, n in conteo.most_common():
        print(f"  {pc:<22} resolvió {n:>3} tareas  {'█' * (n // 4)}")

    titulo("Versiones")
    client.get_versions(check=True)  # da error si alguna PC tiene versiones distintas
    print("  Todas las PCs tienen las mismas versiones. ¡Listo para trabajar!")

    client.close()
