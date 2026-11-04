#!/usr/bin/env python3
"""Interfaz web local para configurar el clúster Dask y ejecutar el procesamiento."""

import ipaddress
import json
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

import yaml
import psutil


ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "cluster.yaml"
PAGE_PATH = ROOT / "web" / "index.html"
PROCESS_PATH = ROOT / "procesar.py"
CLUSTER_PATH = ROOT / "cluster.py"
PRACTICE_SCRIPTS_DIR = ROOT / "archivos_profe" / "scripts"
PRACTICE_SCRIPTS = {
    "02_explorar.py": "Explorar datos",
    "03_calidad.py": "Revisar calidad",
    "04_limpiar.py": "Limpiar y guardar Parquet",
    "05_analizar.py": "Analizar ventas",
    "06_combinar.py": "Combinar tiendas",
    "07_rendimiento.py": "Medir rendimiento",
    "mi_analisis.py": "Mi análisis",
}
PRACTICE_PARQUET_SCRIPTS = {
    "04_limpiar.py",
    "05_analizar.py",
    "06_combinar.py",
    "07_rendimiento.py",
    "mi_analisis.py",
}
MAX_REQUEST_SIZE = 1_000_000
CONFIG_LOCK = threading.Lock()
PROCESS_LOCK = threading.Lock()
PRACTICE_LOCK = threading.Lock()
CLUSTER_LOCK = threading.Lock()
GIT_LOCK = threading.Lock()
PROCESS_STATE = {
    "running": False,
    "returncode": None,
    "output": "",
}
PRACTICE_STATE = {
    "running": False,
    "returncode": None,
    "output": "",
    "progress": "",
    "script": None,
    "machine": None,
}
CLUSTER_PROCESSES = {
    role: {"process": None, "name": None, "running": False, "returncode": None, "output": ""}
    for role in ("scheduler", "worker")
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


def local_ip_for(config):
    coordinator = config["maquinas"][config["coordinadora"]]
    connection = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        connection.connect((coordinator["ip"], config["puerto_scheduler"]))
        return connection.getsockname()[0]
    except OSError:
        return None
    finally:
        connection.close()


def detect_network(config):
    local_ip = local_ip_for(config)
    if not local_ip:
        raise ValueError("No se pudo detectar una IP local para llegar a la coordinadora.")

    for interface, addresses in psutil.net_if_addrs().items():
        for address in addresses:
            if address.family != socket.AF_INET or address.address != local_ip:
                continue
            if not address.netmask:
                raise ValueError(f"No se encontró la máscara de red de la interfaz {interface}.")
            network = ipaddress.ip_interface(f"{local_ip}/{address.netmask}").network
            matching_machines = [
                name for name, machine in config["maquinas"].items()
                if machine["ip"] == local_ip
            ]
            return {
                "ip": local_ip,
                "red_lan": network.with_prefixlen,
                "interfaz": interface,
                "nodos_coincidentes": matching_machines,
                "advertencia": (
                    "La máscara /32 identifica solo esta IP; confirma que esta interfaz pertenece a la red compartida."
                    if network.prefixlen == 32 else None
                ),
            }
    raise ValueError(f"La IP detectada {local_ip} no coincide con una interfaz IPv4 local con máscara.")


def apply_detected_network(machine_name, role, expected_ip, expected_network, config):
    if not isinstance(machine_name, str) or machine_name not in config["maquinas"]:
        raise ValueError("Selecciona la máquina que representa este equipo.")
    expected_role = "coordinadora" if machine_name == config["coordinadora"] else "worker"
    if role != expected_role:
        raise ValueError(f"El nodo seleccionado está configurado como {expected_role}.")

    detection = detect_network(config)
    if detection["ip"] != expected_ip or detection["red_lan"] != expected_network:
        raise ValueError("La red cambió desde la última verificación. Vuelve a verificar antes de aplicar.")
    matching_machines = detection["nodos_coincidentes"]
    if len(matching_machines) > 1:
        raise ValueError(
            f"La IP actual ya está duplicada entre estos nodos: {', '.join(matching_machines)}. Resuelve la identidad antes de actualizar."
        )
    if matching_machines and machine_name not in matching_machines:
        raise ValueError(
            f"La IP actual coincide con {matching_machines[0]}, no con {machine_name}. Selecciona el nodo detectado."
        )

    config["maquinas"][machine_name]["ip"] = detection["ip"]
    config["red_lan"] = detection["red_lan"]
    save_config(config)
    return {"ok": True, "config": config, "detection": detection, "role": expected_role}


def append_cluster_output(role, text):
    with CLUSTER_LOCK:
        state = CLUSTER_PROCESSES[role]
        state["output"] = (state["output"] + text)[-12000:]


def local_dask_scheduler_process(port):
    try:
        connections = psutil.net_connections(kind="tcp")
    except psutil.Error as error:
        raise RuntimeError(f"No se pudieron inspeccionar los procesos locales: {error}") from error

    for connection in connections:
        if (
            connection.status != psutil.CONN_LISTEN
            or connection.laddr.port != port
            or connection.pid is None
        ):
            continue
        try:
            process = psutil.Process(connection.pid)
            command = process.cmdline()
        except psutil.NoSuchProcess:
            continue
        except psutil.AccessDenied as error:
            raise RuntimeError(
                f"No hay permiso para identificar el proceso que escucha en el puerto {port}."
            ) from error

        arguments = [argument.lower().replace("\\", "/") for argument in command]
        executable_names = [Path(argument).name for argument in arguments]
        is_module_scheduler = any(
            "distributed.cli.dask_scheduler" in argument for argument in arguments
        )
        is_legacy_scheduler = (
            any(name in ("dask", "dask.exe") for name in executable_names)
            and "scheduler" in arguments
        )
        if is_module_scheduler or is_legacy_scheduler:
            return process
    return None


def monitor_cluster_process(role, process):
    with process.stdout:
        for line in process.stdout:
            append_cluster_output(role, line)
    returncode = process.wait()
    with CLUSTER_LOCK:
        state = CLUSTER_PROCESSES[role]
        if state["process"] is process:
            state.update(running=False, returncode=returncode)


def start_cluster_process(role, machine_name, config):
    if not isinstance(role, str) or role not in CLUSTER_PROCESSES:
        raise ValueError("El proceso debe ser scheduler o worker.")
    if not isinstance(machine_name, str) or machine_name not in config["maquinas"]:
        raise ValueError("Selecciona una máquina válida.")
    if role == "scheduler" and machine_name != config["coordinadora"]:
        raise ValueError("El scheduler solo puede iniciarse en la máquina coordinadora.")
    if role == "scheduler":
        coordinator = config["maquinas"][config["coordinadora"]]
        try:
            with socket.create_connection(
                (coordinator["ip"], config["puerto_scheduler"]), timeout=1
            ):
                raise RuntimeError(
                    f"Ya hay un scheduler escuchando en {coordinator['ip']}:{config['puerto_scheduler']}."
                )
        except OSError:
            pass

    with CLUSTER_LOCK:
        state = CLUSTER_PROCESSES[role]
        process = state["process"]
        if process is not None and process.poll() is None:
            raise RuntimeError(f"El proceso {role} ya está activo.")
        environment = os.environ.copy()
        environment["PYTHONUNBUFFERED"] = "1"
        command = [sys.executable, str(CLUSTER_PATH), role]
        if role == "worker":
            command.append(machine_name)
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
            start_new_session=os.name != "nt",
        )
        state.update(
            process=process,
            name=machine_name,
            running=True,
            returncode=None,
            output=f"Iniciando {role} para {machine_name}...\n",
        )
    threading.Thread(target=monitor_cluster_process, args=(role, process), daemon=True).start()
    return {"ok": True, "message": f"{role.capitalize()} iniciado para {machine_name}."}


def stop_cluster_process(role):
    if not isinstance(role, str):
        raise ValueError("El proceso debe ser scheduler o worker.")
    with CLUSTER_LOCK:
        state = CLUSTER_PROCESSES.get(role)
        process = state["process"] if state else None
        managed_process = process is not None and process.poll() is None

    if not managed_process:
        if role != "scheduler":
            raise RuntimeError(f"No hay un proceso {role} activo iniciado desde esta interfaz.")
        process = local_dask_scheduler_process(load_config()["puerto_scheduler"])
        if process is None:
            raise RuntimeError("No se encontró un proceso Dask Scheduler local en el puerto configurado.")
        pid = process.pid
        process.terminate()
        try:
            process.wait(timeout=8)
        except psutil.TimeoutExpired:
            process.kill()
            process.wait(timeout=4)
        with CLUSTER_LOCK:
            CLUSTER_PROCESSES["scheduler"].update(
                running=False,
                returncode=0,
                output=f"Scheduler local detenido (PID {pid}).\n",
            )
        return {"ok": True, "message": f"Scheduler local detenido (PID {pid})."}

    try:
        if os.name == "nt":
            process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            os.killpg(process.pid, signal.SIGTERM)
    except (OSError, ValueError):
        process.terminate()
    try:
        process.wait(timeout=8)
    except subprocess.TimeoutExpired:
        if os.name == "nt":
            process.kill()
        else:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except OSError:
                process.kill()
    return {"ok": True, "message": f"Proceso {role} detenido."}


def cluster_snapshot(config):
    local_ip = local_ip_for(config)
    detected_node = next(
        (name for name, machine in config["maquinas"].items() if machine["ip"] == local_ip),
        None,
    )
    with CLUSTER_LOCK:
        processes = {}
        for role, state in CLUSTER_PROCESSES.items():
            process = state["process"]
            running = process is not None and process.poll() is None
            state["running"] = running
            if not running and process is not None:
                state["returncode"] = process.returncode
            processes[role] = {
                "running": running,
                "name": state["name"],
                "returncode": state["returncode"],
                "output": state["output"],
            }

    if not processes["scheduler"]["running"]:
        try:
            scheduler_process = local_dask_scheduler_process(config["puerto_scheduler"])
        except RuntimeError as error:
            processes["scheduler"]["output"] = str(error)
        else:
            if scheduler_process is not None:
                processes["scheduler"].update(
                    running=True,
                    name=config["coordinadora"],
                    returncode=None,
                    output=f"Scheduler Dask local detectado (PID {scheduler_process.pid}).",
                    pid=scheduler_process.pid,
                )

    configured_threads = sum(machine["hilos"] for machine in config["maquinas"].values())
    cluster = {
        "connected": False,
        "workers": [],
        "worker_count": 0,
        "active_threads": 0,
        "memory_limit": 0,
        "error": None,
    }
    try:
        from distributed import Client

        address = f"tcp://{config['maquinas'][config['coordinadora']]['ip']}:{config['puerto_scheduler']}"
        client = Client(address, timeout="2s", set_as_default=False)
        try:
            workers = client.scheduler_info().get("workers", {})
            cluster["workers"] = [
                {
                    "name": worker.get("name", address),
                    "nthreads": worker.get("nthreads", 0),
                    "memory_limit": worker.get("memory_limit", 0),
                }
                for address, worker in workers.items()
            ]
            cluster["connected"] = True
            cluster["worker_count"] = len(workers)
            cluster["active_threads"] = sum(worker["nthreads"] for worker in cluster["workers"])
            cluster["memory_limit"] = sum(worker["memory_limit"] for worker in cluster["workers"])
        finally:
            client.close()
    except Exception as error:
        cluster["error"] = str(error)

    return {
        "detected_ip": local_ip,
        "detected_node": detected_node,
        "processes": processes,
        "capacity": {
            "machine_count": len(config["maquinas"]),
            "configured_threads": configured_threads,
        },
        "cluster": cluster,
    }


def run_git(*arguments, timeout=20):
    environment = os.environ.copy()
    environment["GIT_TERMINAL_PROMPT"] = "0"
    environment["GCM_INTERACTIVE"] = "Never"
    return subprocess.run(
        ["git", *arguments],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )


def git_snapshot(fetch_error=None):
    branch = run_git("branch", "--show-current")
    if branch.returncode:
        raise RuntimeError(branch.stderr.strip() or "No se pudo consultar el repositorio Git.")
    status = run_git("status", "--porcelain")
    upstream = run_git("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}")
    ahead = behind = 0
    if upstream.returncode == 0:
        counts = run_git("rev-list", "--left-right", "--count", f"HEAD...{upstream.stdout.strip()}")
        if counts.returncode == 0:
            ahead, behind = (int(value) for value in counts.stdout.split())
    return {
        "branch": branch.stdout.strip(),
        "dirty": bool(status.stdout.strip()),
        "changes": [line[3:] for line in status.stdout.splitlines()],
        "ahead": ahead,
        "behind": behind,
        "has_upstream": upstream.returncode == 0,
        "fetch_error": fetch_error,
    }


def check_git_updates():
    with GIT_LOCK:
        remote = run_git("remote", "get-url", "origin")
        if remote.returncode:
            return git_snapshot(remote.stderr.strip() or "No hay un remoto origin configurado.")
        fetch = run_git("fetch", "--quiet", "origin", timeout=10)
        return git_snapshot(fetch.stderr.strip() if fetch.returncode else None)


def sync_git_config():
    with GIT_LOCK:
        add = run_git("add", "--", "cluster.yaml")
        if add.returncode:
            raise RuntimeError(add.stderr.strip() or "No se pudo preparar cluster.yaml.")
        staged = run_git("diff", "--cached", "--quiet", "--", "cluster.yaml")
        if staged.returncode == 1:
            commit = run_git("commit", "--only", "-m", "Actualizar configuración del clúster", "--", "cluster.yaml")
            if commit.returncode:
                raise RuntimeError(commit.stderr.strip() or "No se pudo crear el commit de cluster.yaml.")
        elif staged.returncode != 0:
            raise RuntimeError("No se pudo comprobar el estado de cluster.yaml.")

        branch = run_git("branch", "--show-current")
        if branch.returncode or not branch.stdout.strip():
            raise RuntimeError("No se pudo determinar la rama actual.")
        push = run_git("push", "-u", "origin", branch.stdout.strip(), timeout=45)
        if push.returncode:
            raise RuntimeError(push.stderr.strip() or "No se pudo publicar los cambios en GitHub.")
        return git_snapshot()


def pull_git_updates(stash_changes=False):
    with GIT_LOCK:
        changes = run_git("diff", "--quiet", "HEAD", "--")
        if changes.returncode not in (0, 1):
            raise RuntimeError("No se pudo comprobar si hay cambios locales.")
        if changes.returncode == 1:
            if not stash_changes:
                return {"needs_stash": True, "status": git_snapshot()}
            stash = run_git("stash", "push", "-m", f"cluster-web pre-pull {int(time.time())}")
            if stash.returncode:
                raise RuntimeError(stash.stderr.strip() or "No se pudieron guardar los cambios locales.")

        pull = run_git("pull", "--ff-only", timeout=45)
        if pull.returncode:
            raise RuntimeError(pull.stderr.strip() or "No se pudieron traer los cambios. La rama puede requerir resolución manual.")
        return {"needs_stash": False, "status": git_snapshot()}


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


def append_practice_output(text):
    with PRACTICE_LOCK:
        PRACTICE_STATE["output"] = (PRACTICE_STATE["output"] + text)[-50000:]


def append_practice_progress(text):
    with PRACTICE_LOCK:
        PRACTICE_STATE["progress"] = (PRACTICE_STATE["progress"] + text)[-20000:]


def monitor_dask_activity(client, stop_event):
    last_snapshot = None
    last_report = 0.0
    while not stop_event.wait(1):
        try:
            workers = client.scheduler_info()["workers"]
            processing = client.processing()
        except Exception as error:
            append_practice_progress(f"[Dask] Seguimiento interrumpido: {error}\n")
            return

        activity = []
        for address, worker in workers.items():
            active_count = len(processing.get(address, ()))
            state = f"procesando {active_count} tarea(s)" if active_count else "sin tareas activas"
            activity.append(f"{worker.get('name', address)}: {state}")
        snapshot = tuple(activity)
        now = time.monotonic()
        if snapshot != last_snapshot or now - last_report >= 5:
            state = "; ".join(activity) if activity else "sin workers conectados"
            append_practice_progress(f"[Dask {time.strftime('%H:%M:%S')}] {state}\n")
            last_snapshot = snapshot
            last_report = now


def append_task_stream_summary(events, workers):
    names_by_address = {
        address: worker.get("name", address) for address, worker in workers.items()
    }
    totals = {
        name: {"tasks": 0, "seconds": 0.0}
        for name in names_by_address.values()
    }
    for event in events:
        startstops = event.get("startstops", ())
        compute_seconds = sum(
            max(0.0, item["stop"] - item["start"])
            for item in startstops
            if item.get("action") == "compute" and "start" in item and "stop" in item
        )
        if not compute_seconds:
            continue
        address = event.get("worker") or next(
            (item.get("worker") for item in startstops if item.get("worker")), None
        )
        name = names_by_address.get(address, address or "worker desconocido")
        totals.setdefault(name, {"tasks": 0, "seconds": 0.0})
        totals[name]["tasks"] += 1
        totals[name]["seconds"] += compute_seconds

    append_practice_progress("\n[Dask] Trabajo completado por máquina:\n")
    if not any(total["tasks"] for total in totals.values()):
        append_practice_progress("  No se registraron tareas de cómputo.\n")
        return
    for name in sorted(totals):
        total = totals[name]
        state = "completada" if total["tasks"] else "sin tareas asignadas"
        append_practice_progress(
            f"  {name}: {state}; {total['tasks']} tarea(s), "
            f"{total['seconds']:.2f} s acumulados de cómputo\n"
        )


def run_practice_script(script_name, data_path, scheduler_address):
    environment = os.environ.copy()
    environment["PRACTICA_MODO"] = "cluster"
    environment["PRACTICA_DATOS"] = data_path
    environment["PRACTICA_SCHEDULER"] = scheduler_address
    environment["DASK_SCHEDULER_ADDRESS"] = scheduler_address
    environment["PYTHONUNBUFFERED"] = "1"
    environment["PYTHONPATH"] = os.pathsep.join(
        [str(PRACTICE_SCRIPTS_DIR), environment.get("PYTHONPATH", "")]
    )
    monitor_client = None
    task_stream = None
    workers = {}
    monitor_stop = threading.Event()
    monitor_thread = None
    returncode = 1
    try:
        try:
            from distributed import Client, get_task_stream

            monitor_client = Client(scheduler_address, timeout="3s", set_as_default=False)
            workers = monitor_client.scheduler_info()["workers"]
            task_stream = get_task_stream(client=monitor_client)
            task_stream.__enter__()
            append_practice_progress(
                f"[Dask] Seguimiento activo: {len(workers)} worker(s).\n"
            )
        except Exception as error:
            append_practice_progress(f"[Dask] No se pudo activar el seguimiento: {error}\n")
            if monitor_client is not None:
                monitor_client.close()
                monitor_client = None

        process = subprocess.Popen(
            [sys.executable, str(PRACTICE_SCRIPTS_DIR / script_name)],
            cwd=PRACTICE_SCRIPTS_DIR,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        if monitor_client is not None:
            monitor_thread = threading.Thread(
                target=monitor_dask_activity,
                args=(monitor_client, monitor_stop),
                daemon=True,
            )
            monitor_thread.start()
        with process.stdout:
            for line in process.stdout:
                append_practice_output(line)
        returncode = process.wait()
        if returncode:
            append_practice_output(f"\nEl script terminó con código {returncode}.\n")
    except OSError as error:
        append_practice_output(f"\nNo se pudo iniciar {script_name}: {error}\n")
    finally:
        monitor_stop.set()
        if monitor_thread is not None:
            monitor_thread.join(timeout=3)
        if task_stream is not None:
            try:
                task_stream.__exit__(None, None, None)
                append_task_stream_summary(task_stream.data, workers)
            except Exception as error:
                append_practice_progress(f"[Dask] No se pudo resumir el stream: {error}\n")
        if monitor_client is not None:
            monitor_client.close()
        with PRACTICE_LOCK:
            PRACTICE_STATE["returncode"] = returncode
            PRACTICE_STATE["running"] = False


def nodes_missing_pyarrow(config):
    from distributed import Client

    coordinator = config["maquinas"][config["coordinadora"]]
    address = f"tcp://{coordinator['ip']}:{config['puerto_scheduler']}"
    client = Client(address, timeout="3s", set_as_default=False)
    try:
        workers = client.scheduler_info()["workers"]
        if not workers:
            raise RuntimeError("El scheduler no tiene workers conectados.")
        check_pyarrow = lambda: (
            __import__("pyarrow").__version__
            if __import__("importlib.util").util.find_spec("pyarrow") else None
        )
        scheduler_version = client.run_on_scheduler(check_pyarrow)
        worker_versions = client.run(check_pyarrow)
        missing = []
        if scheduler_version != "26.0.1":
            missing.append(f"scheduler {config['coordinadora']}")
        for worker_address, version in worker_versions.items():
            if version != "26.0.1":
                worker = workers.get(worker_address, {})
                missing.append(f"worker {worker.get('name', worker_address)}")
        return missing
    finally:
        client.close()


class RequestHandler(BaseHTTPRequestHandler):
    def handle(self):
        try:
            super().handle()
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            self.close_connection = True

    def has_local_origin(self):
        host = urlsplit(f"//{self.headers.get('Host', '')}").hostname
        if host not in ("127.0.0.1", "localhost"):
            return False
        origin = self.headers.get("Origin")
        if not origin:
            return True
        parsed_origin = urlsplit(origin)
        origin_port = parsed_origin.port or 80
        return (
            parsed_origin.scheme == "http"
            and parsed_origin.hostname in ("127.0.0.1", "localhost")
            and origin_port == self.server.server_port
        )

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
        elif self.path == "/api/cluster/status":
            try:
                self.send_json(200, cluster_snapshot(load_config()))
            except (OSError, ValueError, yaml.YAMLError) as error:
                self.send_json(500, {"error": str(error)})
        elif self.path == "/api/network/detect":
            try:
                config = load_config()
                detection = detect_network(config)
                detection["coordinadora"] = config["coordinadora"]
                detection["nodo_detectado"] = detection["nodos_coincidentes"][0] if len(detection["nodos_coincidentes"]) == 1 else None
                self.send_json(200, detection)
            except (OSError, ValueError, yaml.YAMLError) as error:
                self.send_json(400, {"error": str(error)})
        elif self.path == "/api/git/status":
            try:
                with GIT_LOCK:
                    self.send_json(200, git_snapshot())
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                self.send_json(500, {"error": str(error)})
        elif self.path == "/api/process/status":
            with PROCESS_LOCK:
                self.send_json(200, dict(PROCESS_STATE))
        elif self.path == "/api/practice/scripts":
            self.send_json(200, [
                {"name": name, "label": label}
                for name, label in PRACTICE_SCRIPTS.items()
                if (PRACTICE_SCRIPTS_DIR / name).is_file()
            ])
        elif self.path == "/api/practice/status":
            with PRACTICE_LOCK:
                self.send_json(200, dict(PRACTICE_STATE))
        else:
            self.send_json(404, {"error": "Ruta no encontrada."})

    def do_PUT(self):
        if not self.has_local_origin():
            self.send_json(403, {"error": "Solo se aceptan cambios desde la interfaz local."})
            return
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
        if not self.has_local_origin():
            self.send_json(403, {"error": "Solo se aceptan cambios desde la interfaz local."})
            return
        if self.path == "/api/network/apply":
            try:
                request_data = self.read_json()
                if not isinstance(request_data, dict):
                    raise ValueError("La solicitud debe indicar el nodo y su rol.")
                result = apply_detected_network(
                    request_data.get("machine"),
                    request_data.get("role"),
                    request_data.get("expected_ip"),
                    request_data.get("expected_network"),
                    load_config(),
                )
            except (OSError, ValueError, yaml.YAMLError) as error:
                self.send_json(400, {"error": str(error)})
                return
            self.send_json(200, result)
            return
        if self.path == "/api/cluster/start":
            try:
                request_data = self.read_json()
                if not isinstance(request_data, dict):
                    raise ValueError("La solicitud debe indicar el rol y la máquina local.")
                result = start_cluster_process(
                    request_data.get("role"), request_data.get("machine"), load_config()
                )
            except RuntimeError as error:
                self.send_json(409, {"error": str(error)})
                return
            except (OSError, ValueError, yaml.YAMLError) as error:
                self.send_json(400, {"error": str(error)})
                return
            self.send_json(202, result)
            return
        if self.path == "/api/cluster/stop":
            try:
                request_data = self.read_json()
                if not isinstance(request_data, dict):
                    raise ValueError("La solicitud debe indicar el proceso que se detendrá.")
                result = stop_cluster_process(request_data.get("role"))
            except RuntimeError as error:
                self.send_json(409, {"error": str(error)})
                return
            except ValueError as error:
                self.send_json(400, {"error": str(error)})
                return
            self.send_json(200, result)
            return
        if self.path == "/api/git/check":
            try:
                self.send_json(200, check_git_updates())
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                self.send_json(502, {"error": str(error)})
            return
        if self.path == "/api/git/sync":
            try:
                self.send_json(200, {"ok": True, "status": sync_git_config()})
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                self.send_json(409, {"error": str(error)})
            return
        if self.path == "/api/git/pull":
            try:
                request_data = self.read_json()
                if not isinstance(request_data, dict):
                    raise ValueError("La solicitud debe indicar si se guardan los cambios locales.")
                result = pull_git_updates(bool(request_data.get("stash_changes")))
            except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
                self.send_json(409, {"error": str(error)})
                return
            self.send_json(409 if result["needs_stash"] else 200, result)
            return
        if self.path == "/api/practice/run":
            try:
                request_data = self.read_json()
                if not isinstance(request_data, dict):
                    raise ValueError("La solicitud debe indicar el script y la máquina local.")
                script_name = request_data.get("script")
                machine_name = request_data.get("machine")
                if not isinstance(script_name, str) or script_name not in PRACTICE_SCRIPTS:
                    raise ValueError("Selecciona un script de práctica permitido.")
                config = load_config()
                if not isinstance(machine_name, str) or machine_name not in config["maquinas"]:
                    raise ValueError("Selecciona la máquina que ejecuta esta interfaz.")
                if script_name in PRACTICE_PARQUET_SCRIPTS:
                    missing_pyarrow = nodes_missing_pyarrow(config)
                    if missing_pyarrow:
                        raise ValueError(
                            "Instala pyarrow==26.0.1 en: " + ", ".join(missing_pyarrow) + "."
                        )
                local_ip = local_ip_for(config)
                local_name = next(
                    (name for name, machine in config["maquinas"].items() if machine["ip"] == local_ip),
                    None,
                )
                if local_name and machine_name != local_name:
                    raise ValueError(f"Esta interfaz está en {local_name}; selecciona esa máquina, no {machine_name}.")
                data_path = Path(config["maquinas"][machine_name]["datos"]).expanduser()
                if not data_path.is_absolute():
                    data_path = ROOT / data_path
                data_path = data_path.resolve()
                if not data_path.is_dir():
                    raise ValueError(f"La ruta local de datos de {machine_name} no existe en esta PC: {data_path}")
                csv_count = sum(1 for _ in data_path.glob("ventas_*.csv"))
                if not csv_count:
                    raise ValueError(f"No hay archivos ventas_*.csv en {data_path}.")
            except (OSError, ValueError, yaml.YAMLError) as error:
                self.send_json(400, {"error": str(error)})
                return

            with PRACTICE_LOCK:
                if PRACTICE_STATE["running"]:
                    self.send_json(409, {"error": "Ya hay un script de práctica en ejecución."})
                    return
                PRACTICE_STATE.update(
                    running=True,
                    returncode=None,
                    output=f"Script: {script_name}\nMáquina: {machine_name}\nDatos locales: {data_path}\nCSV: {csv_count}\n\n",
                    progress="Preparando seguimiento del clúster…\n",
                    script=script_name,
                    machine=machine_name,
                )

            coordinator = config["maquinas"][config["coordinadora"]]
            scheduler_address = f"tcp://{coordinator['ip']}:{config['puerto_scheduler']}"
            thread = threading.Thread(
                target=run_practice_script,
                args=(script_name, str(data_path), scheduler_address),
                daemon=True,
            )
            thread.start()
            self.send_json(202, {"ok": True, "message": f"{script_name} iniciado."})
            return
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
