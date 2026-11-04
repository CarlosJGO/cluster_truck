#!/usr/bin/env python3
"""
cluster.py: un solo archivo para levantar el cluster Dask (Linux y Windows).

USO (con el entorno virtual activado):
    python web_config.py                 # interfaz local para configurar y controlar procesos
    python cluster.py scheduler            # en la PC coordinadora
    python cluster.py worker               # en cada worker (detecta su nombre por IP)
    python cluster.py worker worker-ana    # o indicando el nombre a mano
    python cluster.py verificar            # desde la coordinadora, cuando todos esten conectados
    python cluster.py firewall             # imprime los comandos de firewall para esta PC

ANTES DE USARLO: edita la seccion CONFIGURACION y deja el MISMO archivo en todas las PCs
(por ejemplo con git). Lanza el worker desde una terminal normal, no como administrador.
"""

import os
import socket
import subprocess
import sys
from pathlib import Path

import yaml

# La configuración compartida se mantiene en cluster.yaml.
RUTA_CONFIG = Path(__file__).with_name("cluster.yaml")
ROOT_DIR = RUTA_CONFIG.resolve().parent
with RUTA_CONFIG.open(encoding="utf-8") as archivo:
    CONFIG = yaml.safe_load(archivo)

if not isinstance(CONFIG, dict):
    sys.exit(f"La configuración no es válida: {RUTA_CONFIG}")

RED_LAN = CONFIG["red_lan"]
COORD_NAME = CONFIG["coordinadora"]
PUERTO_SCHEDULER = CONFIG["puerto_scheduler"]
PUERTO_DASHBOARD = CONFIG["puerto_dashboard"]
PUERTO_WORKER = 8789
PUERTO_NANNY = 8790

MAQUINAS = CONFIG["maquinas"]
if not isinstance(MAQUINAS, dict) or COORD_NAME not in MAQUINAS:
    sys.exit("La configuración debe incluir 'maquinas' y la coordinadora indicada.")

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


def ejecutar(cmd, env=None):
    print("Ejecutando:", " ".join(cmd), "\n(Ctrl + C para detener)\n")
    try:
        subprocess.run(cmd, env=env, check=False)
    except KeyboardInterrupt:
        print("\nDetenido.")


def scheduler():
    ip_local = mi_ip()
    if ip_local != IP_COORD:
        sys.exit(
            f"Esta PC usa {ip_local or 'una IP desconocida'} para llegar al scheduler; "
            f"la coordinadora configurada {COORD_NAME} tiene {IP_COORD}."
        )
    ejecutar([
        sys.executable,
        "-m",
        "distributed.cli.dask_scheduler",
        "--host",
        IP_COORD,
        "--port",
        str(PUERTO_SCHEDULER),
    ])
    

def worker(nombre=None):
    nombre = nombre or detectar_nombre()
    if nombre not in MAQUINAS:
        sys.exit(f"'{nombre}' no esta en MAQUINAS: {', '.join(MAQUINAS)}")
    m = MAQUINAS[nombre]
    ip_local = mi_ip()
    if not ip_local:
        sys.exit(f"No se pudo detectar la interfaz local para llegar a {IP_COORD}:{PUERTO_SCHEDULER}.")
    if ip_local != m["ip"]:
        sys.exit(f"La IP de esta PC es {ip_local}, pero {nombre} está configurada como {m['ip']}.")
    carpeta = Path(os.path.expandvars(os.path.expanduser(m["datos"])))
    if not carpeta.is_absolute():
        carpeta = ROOT_DIR / carpeta
    carpeta = carpeta.resolve()
    if not carpeta.is_dir():
        sys.exit(f"La carpeta local de datos no existe: {carpeta}")
    archivos = sorted(carpeta.glob("ventas_*.csv"))
    if not archivos:
        sys.exit(f"No hay archivos ventas_*.csv en la carpeta local: {carpeta}")
    print(f"Datos locales: {carpeta} ({len(archivos)} CSV)")

    env = os.environ.copy()
    env["CLUSTER_PROJECT_ROOT"] = str(ROOT_DIR)
    env["CLUSTER_DATOS"] = str(carpeta)
    cmd = [
        sys.executable, "-m", "distributed.cli.dask_worker",
        f"tcp://{IP_COORD}:{PUERTO_SCHEDULER}",
        "--name", nombre,
        "--nthreads", str(m["hilos"]),
        "--host", ip_local,
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
