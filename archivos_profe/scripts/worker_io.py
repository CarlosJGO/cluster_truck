"""Funciones livianas que se ejecutan en los workers del clúster."""

import os
import re
import shutil
from pathlib import Path

import pandas as pd
import yaml
import dask
from distributed import get_worker


CONFIG_PATH = Path(__file__).resolve().with_name("cluster.yaml")
if not CONFIG_PATH.is_file():
    CONFIG_PATH = Path(__file__).resolve().parents[2] / "cluster.yaml"
with CONFIG_PATH.open(encoding="utf-8") as file:
    CONFIG = yaml.safe_load(file)

DATA_PATHS = {
    name: machine["datos"] for name, machine in CONFIG["maquinas"].items()
}


def configure_dask_strings():
    dask.config.set({"dataframe.convert-string": False})
    pd.options.mode.string_storage = "python"


def data_path():
    if os.environ.get("PRACTICA_MODO") == "local":
        return os.path.expandvars(os.path.expanduser(os.environ.get("PRACTICA_DATOS", "datos")))
    try:
        worker_name = str(get_worker().name)
    except ValueError:
        worker_name = None
    if worker_name in DATA_PATHS:
        return os.path.expandvars(os.path.expanduser(DATA_PATHS[worker_name]))
    unique_paths = set(DATA_PATHS.values())
    if len(unique_paths) == 1:
        return os.path.expandvars(os.path.expanduser(unique_paths.pop()))
    raise RuntimeError(
        f"El worker '{worker_name}' no tiene una ruta de datos configurada en cluster.yaml."
    )


def read_csv_file(filename, dtype, parse_dates):
    return pd.read_csv(
        os.path.join(data_path(), filename), dtype=dtype, parse_dates=parse_dates
    )


def list_parquet_files():
    directory = Path(data_path()) / "ventas_limpias"
    return {
        "ruta": str(directory.resolve()),
        "archivos": sorted(path.name for path in directory.glob("part-*.parquet")),
    } if directory.is_dir() else {"ruta": str(directory.resolve()), "archivos": []}


def read_parquet_file(filename, columns=None):
    return pd.read_parquet(Path(data_path()) / "ventas_limpias" / filename, columns=columns)


def prepare_parquet_directory():
    directory = Path(data_path()) / "ventas_limpias"
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir(parents=True, exist_ok=True)


def write_parquet_partition(partition, partition_info=None):
    directory = Path(data_path()) / "ventas_limpias"
    directory.mkdir(parents=True, exist_ok=True)
    worker_name = re.sub(r"[^A-Za-z0-9_-]+", "_", str(get_worker().name))
    filename = f"part-{worker_name}-{partition_info['number']:05d}.parquet"
    partition.to_parquet(directory / filename, index=False)
    return pd.Series([filename], name="archivo", dtype="object")