# Instrucciones y progreso — Clúster Dask + NFS

## 1. Configurar la comunicación entre las máquinas

Abrir los puertos necesarios en los firewalls de ambas máquinas:

**En Ashley (192.168.1.11):**

```bash
sudo ufw allow from 192.168.1.9 to 192.168.1.11 port 8786 proto tcp
sudo ufw allow from 192.168.1.9 to 192.168.1.11 port 2049 proto tcp
```

**En Alejandra (192.168.1.9):**

```bash
sudo ufw allow from 192.168.1.11 to 192.168.1.9 port 8789 proto tcp
```

La primera regla permite que Alejandra se conecte al scheduler de Ashley. La segunda permite que Alejandra acceda al servidor NFS de Ashley. La tercera permite que Ashley se conecte al worker de Alejandra.

## 2. Iniciar el scheduler en la máquina coordinadora

En la máquina que actuará como coordinadora:

```bash
dask scheduler --host 192.168.1.11 --port 8786
```

## 3. Iniciar un worker en Ashley

```bash
dask worker tcp://192.168.1.11:8786 \
    --nthreads 4 \
    --name ashley
```

## 4. Iniciar un worker en Alejandra

```bash
dask worker tcp://192.168.1.11:8786 \
    --nthreads 1 \
    --name alejandra \
    --worker-port 8789
```

Ambos workers deben aparecer registrados en el scheduler.

Se comprobó la ejecución de una tarea directamente en Alejandra mediante `client.submit()`.

## 5. Instalar NFS en ambas máquinas

**Ashley (CachyOS):**

```bash
sudo pacman -S nfs-utils
```

**Alejandra (Linux Mint):**

```bash
sudo apt install nfs-common nfs-kernel-server
```

Se instalan ambos componentes en Alejandra para permitir que también pueda actuar como servidor NFS cuando sea necesario.

## 6. Configurar Ashley como servidor NFS

Agregar la carpeta de datos a `/etc/exports`:

```text
/home/carlosjgo/Documentos/UNIVERSIDAD/Big\040Data/cluster/datos 192.168.1.9(ro,sync,no_subtree_check)
```

Aplicar la configuración:

```bash
sudo exportfs -arv
```

Iniciar el servidor:

```bash
sudo systemctl enable --now nfs-server.service
```

Comprobar las exportaciones:

```bash
sudo exportfs -v
```

La carpeta se comparte en modo de solo lectura (`ro`) y únicamente con Alejandra (`192.168.1.9`).

## 7. Montar los datos de Ashley en Alejandra

Crear el punto de montaje:

```bash
sudo mkdir -p /mnt/cluster-datos
```

Montar la carpeta compartida:

```bash
sudo mount -t nfs4 -o ro '192.168.1.11:/home/carlosjgo/Documentos/UNIVERSIDAD/Big Data/cluster/datos' /mnt/cluster-datos
```

Comprobar el acceso:

```bash
ls -lh /mnt/cluster-datos
```

Comprobar la cantidad de CSV:

```bash
find /mnt/cluster-datos -maxdepth 1 -type f -name '*.csv' | wc -l
```

**Resultado comprobado:** Alejandra puede acceder a los 50 CSV almacenados físicamente en Ashley mediante NFS.

## 8. Unificar la ruta de los datos en ambas máquinas

**Objetivo:** que los workers encuentren los archivos en la misma ruta absoluta, independientemente de dónde estén almacenados físicamente.

Ruta común:

```text
/mnt/cluster-datos/
```

En Ashley ya se creó el directorio:

```bash
sudo mkdir -p /mnt/cluster-datos
```

**Pendiente:**
- Configurar Ashley para que `/mnt/cluster-datos` apunte a su carpeta local `datos/`.
- Modificar `procesar.py` para utilizar `/mnt/cluster-datos/*.csv`.
- Probar el procesamiento distribuido con ambos workers.

## 9. Configurar el modo inverso

**Pendiente:** preparar Alejandra como servidor NFS y Ashley como cliente.

Objetivo:
- En casa: Ashley coordina y comparte los datos; Alejandra trabaja como worker.
- En la universidad: Alejandra coordina y comparte los datos; Ashley puede permanecer desconectada.

La lógica de procesamiento debe mantenerse igual en ambos escenarios. Solo debe cambiar la configuración de conexión y almacenamiento.