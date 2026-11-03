#!/usr/bin/env python3
"""Interfaz web local para configurar el clúster Dask y ejecutar el procesamiento."""

import ipaddress
import json
import os
import re
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "cluster.yaml"
PAGE_PATH = ROOT / "web" / "index.html"
PROCESS_PATH = ROOT / "procesar.py"
MAX_REQUEST_SIZE = 1_000_000
CONFIG_LOCK = threading.Lock()
PROCESS_LOCK = threading.Lock()
PROCESS_STATE = {
    "running": False,
    "returncode": None,
    "output": "",
}


def load_config():
    with CONFIG_LOCK:
        with CONFIG_PATH.open(encoding="utf-8") as file:
            config = yaml.safe_load(file)
    if not isinstance(config, dict):
        raise ValueError("El contenido de cluster.yaml debe ser un objeto YAML.")
    validate_config(config)
    return config


def validate_config(config):
    if not isinstance(config, dict):
        raise ValueError("La configuración debe ser un objeto.")

    try:
        ipaddress.ip_network(config.get("red_lan"), strict=False)
    except (TypeError, ValueError):
        raise ValueError("La red LAN debe tener formato CIDR, por ejemplo 192.168.1.0/24.")

    for key in ("puerto_scheduler", "puerto_dashboard"):
        port = config.get(key)
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError(f"{key} debe ser un puerto entre 1 y 65535.")

    machines = config.get("maquinas")
    if not isinstance(machines, dict) or not machines:
        raise ValueError("Agrega al menos un nodo.")

    for name, machine in machines.items():
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,62}", name):
            raise ValueError(f"Nombre de nodo inválido: {name!r}. Usa letras, números, guion y guion bajo.")
        if not isinstance(machine, dict):
            raise ValueError(f"La configuración de {name} debe ser un objeto.")
        try:
            ipaddress.ip_address(machine.get("ip"))
        except (TypeError, ValueError):
            raise ValueError(f"La IP de {name} no es válida.")
        if machine.get("so") not in ("linux", "windows"):
            raise ValueError(f"El sistema operativo de {name} debe ser Linux o Windows.")
        threads = machine.get("hilos")
        if isinstance(threads, bool) or not isinstance(threads, int) or not 1 <= threads <= 1024:
            raise ValueError(f"Los hilos de {name} deben ser un número entre 1 y 1024.")
        data_path = machine.get("datos")
        if not isinstance(data_path, str) or not data_path.strip():
            raise ValueError(f"Indica la ruta de datos de {name}.")

    if config.get("coordinadora") not in machines:
        raise ValueError("La coordinadora seleccionada debe existir en la lista de nodos.")


def save_config(config):
    validate_config(config)
    temporary_path = CONFIG_PATH.with_suffix(".yaml.tmp")
    with CONFIG_LOCK:
        with temporary_path.open("w", encoding="utf-8", newline="\n") as file:
            yaml.safe_dump(config, file, allow_unicode=True, sort_keys=False)
        os.replace(temporary_path, CONFIG_PATH)


def append_output(text):
    with PROCESS_LOCK:
        PROCESS_STATE["output"] = (PROCESS_STATE["output"] + text)[-30000:]


def run_processing(data_path, scheduler_address):
    environment = os.environ.copy()
    environment["CLUSTER_DATOS"] = data_path
    environment["DASK_SCHEDULER_ADDRESS"] = scheduler_address
    try:
        process = subprocess.Popen(
            [sys.executable, str(PROCESS_PATH)],
            cwd=ROOT,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        with process.stdout:
            for line in process.stdout:
                append_output(line)
        returncode = process.wait()
        if returncode:
            append_output(f"\nEl procesamiento terminó con código {returncode}.\n")
    except OSError as error:
        append_output(f"\nNo se pudo iniciar el procesamiento: {error}\n")
        returncode = 1
    finally:
        with PROCESS_LOCK:
            PROCESS_STATE["returncode"] = returncode
            PROCESS_STATE["running"] = False


class RequestHandler(BaseHTTPRequestHandler):
    def send_json(self, status, value):
        body = json.dumps(value, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def read_json(self):
        length = int(self.headers.get("Content-Length", "0"))
        if not 0 < length <= MAX_REQUEST_SIZE:
            raise ValueError("El cuerpo de la solicitud está vacío o supera el límite permitido.")
        try:
            return json.loads(self.rfile.read(length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("El cuerpo debe contener JSON válido.") from error

    def do_GET(self):
        if self.path == "/":
            try:
                body = PAGE_PATH.read_bytes()
            except OSError as error:
                self.send_json(500, {"error": f"No se pudo leer la interfaz: {error}"})
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/api/config":
            try:
                self.send_json(200, load_config())
            except (OSError, ValueError, yaml.YAMLError) as error:
                self.send_json(500, {"error": str(error)})
        elif self.path == "/api/process/status":
            with PROCESS_LOCK:
                self.send_json(200, dict(PROCESS_STATE))
        else:
            self.send_json(404, {"error": "Ruta no encontrada."})

    def do_PUT(self):
        if self.path != "/api/config":
            self.send_json(404, {"error": "Ruta no encontrada."})
            return
        try:
            config = self.read_json()
            save_config(config)
        except (OSError, ValueError, yaml.YAMLError) as error:
            self.send_json(400, {"error": str(error)})
            return
        self.send_json(200, {"ok": True, "message": "Configuración guardada en cluster.yaml."})

    def do_POST(self):
        if self.path != "/api/process":
            self.send_json(404, {"error": "Ruta no encontrada."})
            return
        try:
            config = load_config()
            request_data = self.read_json()
            if not isinstance(request_data, dict):
                raise ValueError("La solicitud debe indicar el nodo con los CSV locales.")
            data_node = request_data.get("data_node")
            if not isinstance(data_node, str) or data_node not in config["maquinas"]:
                raise ValueError("Selecciona un nodo válido como fuente de datos.")
            data_machine = config["maquinas"][data_node]
            coordinator = config["maquinas"][config["coordinadora"]]
            data_path = Path(data_machine["datos"]).expanduser()
            if not data_path.is_absolute():
                data_path = ROOT / data_path
            data_path = data_path.resolve()
            if not data_path.is_dir():
                raise ValueError(f"La carpeta configurada no existe en esta PC: {data_path}")
            csv_count = sum(1 for _ in data_path.glob("*.csv"))
            if not csv_count:
                raise ValueError(f"No hay archivos CSV en {data_path}.")
        except (OSError, ValueError, yaml.YAMLError) as error:
            self.send_json(400, {"error": str(error)})
            return

        with PROCESS_LOCK:
            if PROCESS_STATE["running"]:
                self.send_json(409, {"error": "Ya hay un procesamiento en curso."})
                return
            PROCESS_STATE.update(
                running=True,
                returncode=None,
                output=(
                    f"Fuente local: {data_node} ({data_path}); "
                    f"conectando al scheduler; {csv_count} CSV.\n"
                ),
            )

        scheduler_address = (
            f"tcp://{coordinator['ip']}:{config['puerto_scheduler']}"
        )
        thread = threading.Thread(
            target=run_processing,
            args=(str(data_path), scheduler_address),
            daemon=True,
        )
        thread.start()
        self.send_json(202, {"ok": True, "message": "Procesamiento iniciado."})

    def log_message(self, _format, *_args):
        return


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    if not 1 <= port <= 65535:
        raise SystemExit("El puerto debe estar entre 1 y 65535.")
    server = ThreadingHTTPServer(("127.0.0.1", port), RequestHandler)
    print(f"Interfaz disponible en http://127.0.0.1:{port} (solo esta PC). Ctrl+C para cerrar.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServidor detenido.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
