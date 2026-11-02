#!/usr/bin/env python3
"""
cluster.py: un solo archivo para levantar el cluster Dask (Linux y Windows).

USO (con el entorno virtual activado):
    python cluster.py scheduler            # en la PC coordinadora
    python cluster.py worker               # en cada worker (detecta su nombre por IP)
    python cluster.py worker worker-ana    # o indicando el nombre a mano
    python cluster.py verificar            # desde la coordinadora, cuando todos esten conectados
    python cluster.py firewall             # imprime los comandos de firewall para esta PC

ANTES DE USARLO: edita la seccion CONFIGURACION y deja el MISMO archivo en todas las PCs
(por ejemplo con git). Lanza el worker desde una terminal normal, no como administrador.
"""

import os
import shutil
import socket
import subprocess
import sys
from pathlib import Path

# =====================  CONFIGURACION (editar)  =====================
RED_LAN = "10.42.0.1/24"
COORD_NAME = "linuxMint"        # nombre (clave de MAQUINAS) de la PC coordinadora
PUERTO_SCHEDULER = 8786
PUERTO_DASHBOARD = 8787
PUERTO_WORKER = 8789
PUERTO_NANNY = 8790

MAQUINAS = {
    # nombre:        {ip, hilos, datos (ruta local de los datos en ESA PC)}
    "linuxMint": {"ip": "10.42.0.4", "hilos": 1, "datos": "/home/carlosjgo/Documentos/UNIVERSIDAD/Big Data/cluster/datos"},
    "worker-1":  {"ip": "10.42.0.2", "hilos": 8, "datos": r"C:\Users\DELL\OneDrive\Documentos\Desktop\cluster_truck\datos"},
    "worker-2":  {"ip": "192.168.1.12", "hilos": 6, "datos": r"C:\cluster\datos"},
    "worker_rg": {"ip": "10.42.0.7", "hilos": 6, "datos": r"C:\Users\LENOVO\Documents\Rosendo_Gonzalez\nuevo_cluster"},
}
# ====================================================================

IP_COORD = MAQUINAS[COORD_NAME]["ip"]
ES_WINDOWS = os.name == "nt"


def mi_ip():
    """IP local usada para llegar a la coordinadora."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((IP_COORD, PUERTO_SCHEDULER))
        return s.getsockname()[0]
    except OSError:
        return None
    finally:
        s.close()


def detectar_nombre():
    ip = mi_ip()
    for nombre, m in MAQUINAS.items():
        if m["ip"] == ip:
            return nombre
    sys.exit(f"No pude detectar esta PC por IP ({ip}). Usa: python cluster.py worker <nombre>")


def comando_dask():
    """Ruta al ejecutable 'dask' del entorno virtual activo."""
    bin_dir = Path(sys.executable).parent
    for candidato in ("dask.exe", "dask"):
        ruta = bin_dir / candidato
        if ruta.exists():
            return str(ruta)
    ruta = shutil.which("dask")
    if ruta:
        return ruta
    sys.exit("No encuentro el comando 'dask'. Activa el entorno virtual e instala dask[complete].")


def ejecutar(cmd, env=None):
    print("Ejecutando:", " ".join(cmd), "\n(Ctrl + C para detener)\n")
    try:
        subprocess.run(cmd, env=env, check=False)
    except KeyboardInterrupt:
        print("\nDetenido.")


def scheduler():
    ejecutar([comando_dask(), "scheduler", "--host", IP_COORD, "--port", str(PUERTO_SCHEDULER)])
    

def worker(nombre=None):
    nombre = nombre or detectar_nombre()
    if nombre not in MAQUINAS:
        sys.exit(f"'{nombre}' no esta en MAQUINAS: {', '.join(MAQUINAS)}")
    m = MAQUINAS[nombre]
    carpeta = Path(m["datos"])
    if not carpeta.is_dir():
        print(f"AVISO: la carpeta de datos no existe en esta PC: {carpeta}")
    else:
        print(f"Datos: {carpeta} ({len(list(carpeta.glob('*.csv')))} CSV)")

    env = os.environ.copy()
    env["CLUSTER_DATOS"] = m["datos"]
    cmd = [
        comando_dask(), "worker", f"tcp://{IP_COORD}:{PUERTO_SCHEDULER}",
        "--name", nombre,
        "--nthreads", str(m["hilos"]),
        "--host", m["ip"],
        "--worker-port", str(PUERTO_WORKER),
        "--nanny-port", str(PUERTO_NANNY),
    ]
    ejecutar(cmd, env=env)


def _inventario():
    """Se ejecuta EN cada worker."""
    carpeta = os.environ["CLUSTER_DATOS"]
    return {
        f: os.path.getsize(os.path.join(carpeta, f))
        for f in sorted(os.listdir(carpeta))
        if f.endswith(".csv")
    }


def verificar():
    from dask.distributed import Client

    client = Client(f"tcp://{IP_COORD}:{PUERTO_SCHEDULER}")
    workers = client.scheduler_info()["workers"]
    print(f"\nWorkers conectados: {len(workers)}")
    for addr, w in workers.items():
        print(" -", w["name"], addr, "hilos:", w["nthreads"])

    esperados = set(MAQUINAS)
    conectados = {w["name"] for w in workers.values()}
    faltan = esperados - conectados
    print("Faltan por conectar:", ", ".join(sorted(faltan)) if faltan else "ninguno")

    try:
        client.get_versions(check=True)
        print("Versiones: OK (iguales en todas las PCs)")
    except Exception as e:
        print("Versiones: DIFERENTES ->", e)

    print("Maquinas:", client.run(socket.gethostname))

    inv = client.run(_inventario)
    referencia = next(iter(inv.values()))
    for addr, datos in inv.items():
        estado = "OK" if datos == referencia else "DIFERENTE"
        print(f" - {addr}: {len(datos)} archivos, datos {estado}")
    client.close()


def firewall():
    nombre = None
    ip = mi_ip()
    for n, m in MAQUINAS.items():
        if m["ip"] == ip:
            nombre = n
    es_coord = nombre == COORD_NAME
    print(f"PC detectada: {nombre or 'desconocida'} ({ip})\n")
    if ES_WINDOWS:
        print("PowerShell COMO ADMINISTRADOR (red en perfil Privado):\n")
        if es_coord:
            print(f'New-NetFirewallRule -DisplayName "Dask scheduler" -Direction Inbound -Protocol TCP -LocalPort {PUERTO_SCHEDULER},{PUERTO_DASHBOARD} -RemoteAddress {RED_LAN} -Profile Private -Action Allow')
        print(f'New-NetFirewallRule -DisplayName "Dask worker" -Direction Inbound -Protocol TCP -LocalPort {PUERTO_WORKER},{PUERTO_NANNY} -RemoteAddress {RED_LAN} -Profile Private -Action Allow')
    else:
        print("Terminal Linux (ufw):\n")
        if es_coord:
            print(f"sudo ufw allow from {RED_LAN} to any port {PUERTO_SCHEDULER} proto tcp")
            print(f"sudo ufw allow from {RED_LAN} to any port {PUERTO_DASHBOARD} proto tcp")
        print(f"sudo ufw allow from {RED_LAN} to any port {PUERTO_WORKER}:{PUERTO_NANNY} proto tcp")


def main():
    args = sys.argv[1:]
    if not args or args[0] not in ("scheduler", "worker", "verificar", "firewall"):
        print(__doc__)
        sys.exit(1)
    if args[0] == "scheduler":
        scheduler()
    elif args[0] == "worker":
        worker(args[1] if len(args) > 1 else None)
    elif args[0] == "verificar":
        verificar()
    else:
        firewall()


if __name__ == "__main__":      # obligatorio en Windows
    main()
