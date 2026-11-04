# Guía para montar un clúster Dask entre varias PCs (Linux y Windows)

> **Guía histórica:** las secciones de NFS/SMB no aplican a la configuración actual. Para trabajar sin carpetas compartidas, sigue [Guia_Dask.md](Guia_Dask.md).

Esta guía explica cómo levantar un clúster de Dask con varias computadoras de la misma red y cómo hacer que todos los workers tengan acceso a los datos. Está escrita sin nombres de máquinas: cualquier persona del equipo puede usarla para **crear** el clúster o para **unirse** a uno existente.

---

## 0. Cómo leer esta guía

### Roles

| Rol | Qué hace | Cuántos hay |
|---|---|---|
| **PC coordinadora** | Ejecuta el *scheduler* (reparte el trabajo). Normalmente también ejecuta el *client* (el script que envía las tareas) y puede tener su propio worker. | 1 |
| **PC worker** | Ejecuta un *worker* que procesa las tareas. | 1 o más |
| **Otros workers** | Igual que la PC worker; se unen exactamente con los mismos pasos. | 0 o más |

La PC coordinadora también puede ser worker al mismo tiempo (se abren dos terminales).

### Variables que debes reemplazar

Todos los comandos usan estos marcadores. **Reemplázalos por tus valores reales** antes de ejecutar.

| Marcador | Significado | Ejemplo |
|---|---|---|
| `<IP_COORD>` | IP de la PC coordinadora | `192.168.1.11` |
| `<IP_WORKER>` | IP de la PC worker en la que estás | `192.168.1.9` |
| `<RED_LAN>` | Red local en notación CIDR | `192.168.1.0/24` |
| `<NOMBRE_WORKER>` | Nombre único para cada worker | `worker-ana` |
| `<N_HILOS>` | Hilos del worker (normalmente = núcleos de CPU) | `4` |
| `<RUTA_DATOS_COORD>` | Carpeta con los datos en la PC que los aloja | `/srv/cluster/datos` |

### Convención de bloques de código

- Bloques `bash` → Linux (Mint, Ubuntu, CachyOS/Arch, etc.).
- Bloques `powershell` → Windows 11. **Abre PowerShell** (como administrador cuando se indique).
- Si un bloque no indica sistema, funciona igual en ambos.

Los comandos largos se escriben **en una sola línea** a propósito, porque el carácter de continuación de línea es distinto en Linux (`\`), PowerShell (`` ` ``) y cmd (`^`).

---

## 1. Arquitectura

```
                         ┌───────────────────────────┐
                         │      PC COORDINADORA      │
                         │  scheduler  (puerto 8786) │
                         │  dashboard  (puerto 8787) │
                         │  client (tu script .py)   │
                         └─────────────┬─────────────┘
                                       │ red local (LAN)
                 ┌─────────────────────┼─────────────────────┐
                 │                     │                     │
        ┌────────┴────────┐   ┌────────┴────────┐   ┌────────┴────────┐
        │    PC WORKER    │   │  OTRO WORKER    │   │  OTRO WORKER    │
        │ worker :8789    │   │ worker :8789    │   │ worker :8789    │
        │ nanny  :8790    │   │ nanny  :8790    │   │ nanny  :8790    │
        └─────────────────┘   └─────────────────┘   └─────────────────┘
```

Puntos clave:

- Los workers se conectan al scheduler (`8786`).
- El scheduler **también** abre conexiones hacia los workers, y los workers se conectan **entre sí** para intercambiar resultados. Por eso los puertos de los workers deben estar abiertos en el firewall de cada worker.
- El client se conecta al scheduler (`8786`).
- **Dask no tiene autenticación por defecto**: quien llegue al scheduler puede ejecutar código en todos los workers. Úsalo solo en redes de confianza (tu casa, la red del equipo), nunca expuesto a internet.

### Puertos usados

| Puerto | Quién lo abre | Para qué |
|---|---|---|
| `8786/tcp` | PC coordinadora | Scheduler (workers y client se conectan aquí) |
| `8787/tcp` | PC coordinadora | Dashboard web (opcional, para ver el estado) |
| `8789/tcp` | Cada worker | Puerto del worker |
| `8790/tcp` | Cada worker | Puerto del *nanny* (el proceso que supervisa al worker) |
| `2049/tcp` | PC que aloja los datos | Solo si usas NFS (v4) |
| `445/tcp` | PC que aloja los datos | Solo si usas SMB |

> Si en una misma PC lanzas varios procesos worker (`--nworkers N`), usa rangos: `--worker-port 8789:8799 --nanny-port 8800:8810` y abre esos rangos en el firewall.

---

## 2. Requisitos previos (en TODAS las PCs)

### 2.1 Red

- Todas las PCs deben estar en la **misma red local** y poder hacerse `ping` entre sí.
- Conviene que cada PC tenga **IP fija** (o reserva DHCP en el router). Si la IP cambia, hay que reconfigurar todo.
- Averigua tu IP:

```bash
# Linux
ip -4 addr show | grep inet
```

```powershell
# Windows
ipconfig
```

- Prueba la conectividad desde un worker hacia la coordinadora:

```bash
ping <IP_COORD>
```

- **Windows:** la red debe estar marcada como **Privada**, porque las reglas de firewall de esta guía se crean para el perfil Privado. En *Configuración → Red e Internet → Wi-Fi/Ethernet → propiedades de la red* → "Tipo de perfil de red: Privada".
- **Redes públicas o de universidad:** muchas redes Wi-Fi institucionales aíslan a los clientes entre sí (no permiten que dos PCs se vean). Si no logras ni siquiera hacer `ping`, usa un punto de acceso/hotspot propio, un cable a un switch, o una VPN de malla como Tailscale o ZeroTier (en ese caso usa las IPs de la VPN como `<IP_COORD>` y `<IP_WORKER>`).

### 2.2 Python y Dask: **mismas versiones en todas las PCs**

Este es el error más común: si una PC tiene otra versión de Python, `dask`, `distributed`, `pandas` o `numpy`, el clúster falla con errores raros de serialización. Acordad una versión y fijadla.

Ejemplo (ajusta las versiones a las que decida el equipo, por ejemplo la del primero que monte el clúster):

**Linux:**

```bash
python3 --version                      # debe coincidir en versión mayor.menor con las demás PCs
python3 -m venv ~/venv-dask
source ~/venv-dask/bin/activate
pip install --upgrade pip
pip install "dask[complete]==<VERSION>" "distributed==<VERSION>" pandas
```

**Windows (PowerShell):**

```powershell
py --version
py -m venv $HOME\venv-dask
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned   # solo la primera vez, para permitir activar el venv
& $HOME\venv-dask\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install "dask[complete]==<VERSION>" "distributed==<VERSION>" pandas
```

> En CachyOS/Arch (y otras distros recientes) `pip install` global está bloqueado (PEP 668). Usa siempre un entorno virtual como el de arriba.

Ver las versiones instaladas (en cada PC) para compararlas:

```bash
python -c "import sys, dask, distributed, pandas; print(sys.version.split()[0], dask.__version__, distributed.__version__, pandas.__version__)"
```

> **Todos los comandos `dask ...` de esta guía se ejecutan con el entorno virtual activado.**

---

## 3. Firewall

Hay que permitir tráfico entrante en cada PC según su rol. Se recomienda limitarlo a la red local (`<RED_LAN>`).

### 3.1 PC coordinadora

**Linux (ufw):**

```bash
sudo ufw allow from <RED_LAN> to any port 8786 proto tcp
sudo ufw allow from <RED_LAN> to any port 8787 proto tcp
```

**Windows (PowerShell como administrador):**

```powershell
New-NetFirewallRule -DisplayName "Dask scheduler" -Direction Inbound -Protocol TCP -LocalPort 8786,8787 -RemoteAddress <RED_LAN> -Profile Private -Action Allow
```

### 3.2 PC worker (y cada "otro worker")

**Linux (ufw):**

```bash
sudo ufw allow from <RED_LAN> to any port 8789:8790 proto tcp
```

**Windows (PowerShell como administrador):**

```powershell
New-NetFirewallRule -DisplayName "Dask worker" -Direction Inbound -Protocol TCP -LocalPort 8789,8790 -RemoteAddress <RED_LAN> -Profile Private -Action Allow
```

### 3.3 PC que aloja los datos (solo si usas el Modo A, ver sección 7)

Se explica dentro de cada variante del Modo A.

### Notas

- Si `sudo ufw status` dice **inactive**, ufw no está bloqueando nada y no necesitas esas reglas (pero verifica que no haya otro firewall, como `firewalld` o `nftables`).
- En Arch/CachyOS, ufw puede no estar instalado: `sudo pacman -S ufw && sudo systemctl enable --now ufw`.
- En Windows, la primera vez que ejecutes `dask scheduler` o `dask worker` puede aparecer una ventana de "Windows Defender Firewall ha bloqueado algunas características": marca **Redes privadas** y acepta. Con las reglas de arriba normalmente no aparece.
- Si quieres reglas más estrictas, reemplaza `<RED_LAN>` por la IP de una PC concreta (`<IP_COORD>` o `<IP_WORKER>`).

---

## 4. Levantar el scheduler (PC coordinadora)

Con el entorno virtual activado:

```bash
dask scheduler --host <IP_COORD> --port 8786
```

Debe mostrar algo como `Scheduler at: tcp://<IP_COORD>:8786`. **Deja esta terminal abierta.**

El dashboard queda en `http://<IP_COORD>:8787/status` (necesita el paquete `bokeh`, que viene con `dask[complete]`).

---

## 5. Levantar workers

Cada worker se lanza con **una terminal propia**, con el entorno virtual activado.

### 5.1 Worker en Linux

```bash
dask worker tcp://<IP_COORD>:8786 --name <NOMBRE_WORKER> --nthreads <N_HILOS> --host <IP_WORKER> --worker-port 8789 --nanny-port 8790
```

### 5.2 Worker en Windows (PowerShell)

```powershell
dask worker tcp://<IP_COORD>:8786 --name <NOMBRE_WORKER> --nthreads <N_HILOS> --host <IP_WORKER> --worker-port 8789 --nanny-port 8790
```

### 5.3 Worker en la misma PC coordinadora

Es el mismo comando; usa `<IP_COORD>` como `<IP_WORKER>`. Abre una segunda terminal.

### Consejos de rendimiento

- Para trabajo con pandas/CSV (mucho Python puro), suele rendir mejor **más procesos con pocos hilos** que un solo proceso con muchos hilos. Por ejemplo, en una PC de 8 núcleos: `--nworkers 4 --nthreads 2` (recuerda usar rangos de puertos, ver sección 1).
- Cada worker debe tener un `--name` **distinto**.
- Deja a la PC con algo de CPU/RAM libre si también la usas para otra cosa (puedes usar menos hilos que núcleos).

Para detener un worker o el scheduler: `Ctrl + C` en su terminal.

---

## 6. Verificar el clúster

Desde cualquier PC con el entorno virtual activado (normalmente la coordinadora):

```python
from dask.distributed import Client
import socket

client = Client("tcp://<IP_COORD>:8786")

# 1) ¿Están todos los workers?
info = client.scheduler_info()
for addr, w in info["workers"].items():
    print(w["name"], addr, "hilos:", w["nthreads"])

# 2) ¿Las versiones coinciden en todas las PCs? (lanza error si hay diferencias)
client.get_versions(check=True)

# 3) ¿Cada worker ejecuta código y en qué máquina está?
print(client.run(socket.gethostname))
```

Resultado esperado: todos los workers listados, `get_versions` sin errores y `client.run(...)` devolviendo el nombre de cada máquina.

Si algo falla, ve a la sección 11 (solución de problemas).

---

## 7. Datos: elige un modo

Los workers necesitan leer los datos. Hay dos formas de dárselos:

| | **Modo A: carpeta compartida** (NFS o SMB) | **Modo B: Dask envía los datos por la red** |
|---|---|---|
| Idea | Los datos viven en una PC y los demás workers leen la carpeta por red | El client lee los archivos y los manda a los workers con `scatter` |
| Configuración | Más pasos (servidor + montaje en cada worker) | Casi ninguna |
| Rendimiento | Cada worker lee solo lo que necesita, bajo demanda | Los datos pasan por la red cada vez que se envían; usa RAM del client |
| Ideal para | Muchos archivos o archivos grandes, ejecuciones repetidas | Pocos datos, pruebas rápidas, equipos que no pueden montar carpetas |
| Código | Los workers abren archivos por **ruta** | Los workers reciben el **contenido** como argumento |

> Puedes combinar: empezar con el Modo B para probar rápido y pasar al Modo A cuando el volumen de datos lo justifique.

Una regla importante del Modo A, sea NFS o SMB: **la ruta donde cada worker "ve" los datos puede ser distinta en cada PC** (en Linux `/mnt/cluster-datos`, en Windows `Z:\`). Para que el código no dependa de eso, cada worker define una variable de entorno `CLUSTER_DATOS` con su ruta local, y el código la lee en el worker (ver sección 8).

---

### 7A. Modo A: carpeta compartida

Primero decide **qué PC aloja los datos** (normalmente la coordinadora) y **qué protocolo** usar:

| Si la PC que aloja los datos es... | Protocolo recomendado | Por qué |
|---|---|---|
| Linux | **NFS** (sección 7A.1) o SMB con Samba (7A.2) | NFS es lo más natural entre Linux |
| Windows | **SMB** (sección 7A.2) | Windows 11 **no puede actuar como servidor NFS** (solo Windows Server lo incluye) |

| Si el worker es... | Cómo monta la carpeta |
|---|---|
| Linux | NFS (cliente `nfs-common`/`nfs-utils`) o SMB (`cifs-utils`) |
| Windows | SMB (nativo, lo más sencillo). NFS solo en ediciones Pro/Enterprise/Education y con más fricción (ver 7A.1.4) |

> **Para un equipo mixto Linux + Windows, SMB suele ser la opción con menos problemas.** NFS se describe porque es excelente entre máquinas Linux.

#### Recomendaciones generales para la carpeta de datos

- Usa una ruta **sin espacios ni tildes** en la PC que aloja los datos (por ejemplo `/srv/cluster/datos` en Linux o `C:\cluster\datos` en Windows). Los espacios obligan a escribir `\040` en `/etc/exports` y complican comillas en todos lados.
- Comparte la carpeta en modo **solo lectura** (`ro`): los workers solo necesitan leer.
- Evita compartir carpetas dentro de `/home/<usuario>` en Linux: ese directorio suele tener permisos restringidos y los clientes NFS no podrán entrar (ver problemas de permisos en 7A.1.2).

---

#### 7A.1 NFS (servidor Linux)

##### 7A.1.1 Instalar los paquetes

| Rol | Debian / Ubuntu / Mint | Arch / CachyOS |
|---|---|---|
| Servidor (PC que aloja los datos) | `sudo apt install nfs-kernel-server` | `sudo pacman -S nfs-utils` |
| Cliente (workers Linux) | `sudo apt install nfs-common` | `sudo pacman -S nfs-utils` |

Si una PC puede tener ambos roles según el escenario (por ejemplo, para poder invertir quién coordina), instala los dos paquetes en todas las PCs Linux.

##### 7A.1.2 Configurar el servidor

1. **Prepara la carpeta** y asegúrate de que sea legible por "otros" (NFS mapea usuarios por UID, y el cliente suele no coincidir):

```bash
sudo mkdir -p /srv/cluster/datos
# copia ahí tus CSV...
sudo chmod -R o+rX /srv/cluster
```

Si prefieres compartir una carpeta que ya existe en otra ruta, asegúrate de que **todos los directorios padre** tengan permiso de ejecución para "otros" (`chmod o+x`), o el cliente verá `Permission denied`.

2. **Agrega la carpeta a `/etc/exports`** (una línea por carpeta):

```text
/srv/cluster/datos <RED_LAN>(ro,sync,no_subtree_check)
```

Para restringir a PCs concretas, lista IPs en lugar de la red:

```text
/srv/cluster/datos <IP_WORKER_1>(ro,sync,no_subtree_check) <IP_WORKER_2>(ro,sync,no_subtree_check)
```

Si la ruta tiene espacios, se escriben como `\040`: `/home/user/Big\040Data/datos`.

3. **Aplica y arranca el servicio:**

```bash
sudo exportfs -arv
```

Debian/Ubuntu/Mint:

```bash
sudo systemctl enable --now nfs-kernel-server
```

Arch/CachyOS:

```bash
sudo systemctl enable --now nfs-server.service
```

4. **Comprueba las exportaciones:**

```bash
sudo exportfs -v
```

5. **Abre el firewall del servidor** para los clientes (NFSv4 solo necesita el puerto 2049):

```bash
sudo ufw allow from <RED_LAN> to any port 2049 proto tcp
```

##### 7A.1.3 Montar en un worker Linux

```bash
sudo mkdir -p /mnt/cluster-datos
sudo mount -t nfs4 -o ro <IP_COORD>:/srv/cluster/datos /mnt/cluster-datos
```

(Reemplaza `<IP_COORD>` por la IP de la PC que aloja los datos y la ruta por la de `<RUTA_DATOS_COORD>`. Si la ruta tiene espacios, ponla entre comillas simples.)

Comprobar:

```bash
ls -lh /mnt/cluster-datos
find /mnt/cluster-datos -maxdepth 1 -type f -name '*.csv' | wc -l
```

Para desmontar: `sudo umount /mnt/cluster-datos`.

**Opcional, montaje automático al iniciar** (línea en `/etc/fstab`; el `noauto,x-systemd.automount` evita que el arranque se quede esperando si la otra PC está apagada):

```text
<IP_COORD>:/srv/cluster/datos  /mnt/cluster-datos  nfs4  ro,noauto,x-systemd.automount,_netdev  0  0
```

Luego `sudo systemctl daemon-reload` y accede a la carpeta para que se monte.

**Si la PC que aloja los datos es también worker** y no quiere pasar por la red, puede hacer que `/mnt/cluster-datos` apunte a su carpeta local con un montaje *bind*:

```bash
sudo mkdir -p /mnt/cluster-datos
sudo mount --bind /srv/cluster/datos /mnt/cluster-datos
```

(Alternativa más simple: no montar nada y definir `CLUSTER_DATOS=/srv/cluster/datos` en ese worker.)

##### 7A.1.4 Montar en un worker Windows con NFS (poco recomendado)

Solo está disponible en **Windows 11 Pro / Enterprise / Education** (no en Home) y suele dar más problemas que SMB: el cliente de Windows usa NFSv3 (requiere que el servidor lo tenga habilitado y puertos adicionales como `111` y el de `mountd`), y el mapeo de usuarios anónimos puede dar errores de permisos. Si aun así quieres probarlo:

```powershell
# PowerShell como administrador (requiere reiniciar)
Enable-WindowsOptionalFeature -Online -FeatureName ServicesForNFS-ClientOnly, ClientForNFS-Infrastructure -NoRestart
```

Luego, en una terminal normal:

```powershell
mount -o anon,ro \\<IP_COORD>\srv\cluster\datos Z:
```

Si te da problemas, usa SMB (7A.2) o el Modo B.

---

#### 7A.2 SMB (Windows o Linux como servidor; Windows y Linux como clientes)

##### Servidor SMB en Windows

PowerShell como administrador (crea el recurso compartido de solo lectura para un usuario concreto):

```powershell
New-SmbShare -Name "cluster-datos" -Path "C:\cluster\datos" -ReadAccess "<USUARIO_WINDOWS>"
New-NetFirewallRule -DisplayName "SMB datos cluster" -Direction Inbound -Protocol TCP -LocalPort 445 -RemoteAddress <RED_LAN> -Profile Private -Action Allow
```

Los clientes se autenticarán con el usuario y contraseña de esa cuenta de Windows (o crea una cuenta local dedicada solo para esto).

##### Servidor SMB en Linux (Samba)

```bash
# Debian/Ubuntu/Mint
sudo apt install samba
# Arch/CachyOS
sudo pacman -S samba
```

Agrega al final de `/etc/samba/smb.conf` (en Arch puede que debas crear el archivo):

```ini
[cluster-datos]
   path = /srv/cluster/datos
   read only = yes
   valid users = <USUARIO_SAMBA>
```

Crea la contraseña de Samba para un usuario del sistema y reinicia:

```bash
sudo smbpasswd -a <USUARIO_SAMBA>
sudo systemctl enable --now smbd      # Debian/Ubuntu/Mint
sudo systemctl enable --now smb       # Arch/CachyOS
sudo ufw allow from <RED_LAN> to any port 445 proto tcp
```

##### Cliente SMB en Windows

```powershell
net use Z: \\<IP_COORD>\cluster-datos /user:<USUARIO> * /persistent:yes
dir Z:\
```

(El `*` te pedirá la contraseña.) Si prefieres no usar letra de unidad, puedes usar directamente la ruta UNC `\\<IP_COORD>\cluster-datos` como valor de `CLUSTER_DATOS`.

> **Importante en Windows:** las unidades de red mapeadas (`Z:`) solo existen para la sesión de usuario que las creó. Lanza el `dask worker` desde una terminal **normal (no "como administrador")** con la misma cuenta que hizo el `net use`, o el worker no verá `Z:`. Si necesitas ejecutar como administrador o como servicio, usa la ruta UNC en lugar de la letra.

##### Cliente SMB en Linux

```bash
# Debian/Ubuntu/Mint
sudo apt install cifs-utils
# Arch/CachyOS
sudo pacman -S cifs-utils

sudo mkdir -p /mnt/cluster-datos
sudo mount -t cifs //<IP_COORD>/cluster-datos /mnt/cluster-datos -o ro,username=<USUARIO>,uid=$(id -u),gid=$(id -g)
```

---

#### 7A.3 Definir `CLUSTER_DATOS` en cada worker

Antes de lanzar `dask worker` en cada PC, define la variable de entorno con la ruta **local** donde esa PC ve los datos. El worker hereda las variables del terminal desde el que se lanza.

**Linux:**

```bash
export CLUSTER_DATOS=/mnt/cluster-datos
dask worker tcp://<IP_COORD>:8786 --name <NOMBRE_WORKER> --nthreads <N_HILOS> --host <IP_WORKER> --worker-port 8789 --nanny-port 8790
```

**Windows (PowerShell):**

```powershell
$env:CLUSTER_DATOS = "Z:\"
dask worker tcp://<IP_COORD>:8786 --name <NOMBRE_WORKER> --nthreads <N_HILOS> --host <IP_WORKER> --worker-port 8789 --nanny-port 8790
```

Verifica desde el client que **todos** los workers ven los datos (ver ejemplo en la sección 8.1).

---

### 7B. Modo B: Dask envía los datos por la red

No hay que compartir carpetas ni montar nada. Los datos solo existen en la PC donde corre el client; el client los lee y los envía a los workers.

Requisitos: el firewall de la sección 3 y nada más.

Cosas a tener en cuenta:

- Los datos viajan por la red y pasan por la memoria del client. Con archivos muy grandes o muchos GB, procesa **por lotes**.
- Usa `client.scatter(...)` para enviar datos: se transmiten directamente a los workers. **No pases datos grandes como argumento directo de `client.submit`/`client.map`**, porque se incrustan en el grafo de tareas y sobrecargan el scheduler.
- Si tu código está en archivos `.py` propios (módulos que importas), los workers también necesitan esos archivos: usa `client.upload_file("mi_modulo.py")`, que los envía a todos los workers conectados. (Las funciones definidas directamente en tu script se envían solas.)
- Todas las librerías que uses (pandas, numpy, etc.) deben estar instaladas en cada worker, con la misma versión.

Ejemplo completo en la sección 8.2.

---

## 8. Ejemplos de código

### 8.1 Modo A: lectura desde la carpeta compartida

```python
# procesar_modo_a.py
import os
import socket
import pandas as pd
from dask.distributed import Client


def datos_dir():
    # Cada worker usa SU ruta local (variable CLUSTER_DATOS definida al lanzarlo)
    return os.environ.get("CLUSTER_DATOS", "/mnt/cluster-datos")


def verificar_datos():
    carpeta = datos_dir()
    return socket.gethostname(), carpeta, len([f for f in os.listdir(carpeta) if f.endswith(".csv")])


def listar_csv():
    return sorted(f for f in os.listdir(datos_dir()) if f.endswith(".csv"))


def procesar_archivo(nombre):
    ruta = os.path.join(datos_dir(), nombre)   # se resuelve en el worker
    df = pd.read_csv(ruta)
    return {"archivo": nombre, "filas": len(df), "worker": socket.gethostname()}


if __name__ == "__main__":      # obligatorio en Windows
    client = Client("tcp://<IP_COORD>:8786")
    client.get_versions(check=True)

    # ¿Todos los workers ven los datos?
    for addr, resultado in client.run(verificar_datos).items():
        print(addr, resultado)

    # Obtener la lista de archivos desde un worker (el client no necesita tener la carpeta)
    nombres = client.submit(listar_csv, pure=False).result()

    futuros = client.map(procesar_archivo, nombres)
    resultados = client.gather(futuros)

    for r in resultados:
        print(r)
```

Se pasa solo el **nombre** del archivo y cada worker construye la ruta con su propia `CLUSTER_DATOS`; así el mismo código funciona en Linux y Windows.

### 8.2 Modo B: el client envía el contenido

```python
# procesar_modo_b.py
import io
import socket
from pathlib import Path
import pandas as pd
from dask.distributed import Client


def procesar_bytes(nombre, contenido):
    df = pd.read_csv(io.BytesIO(contenido))
    return {"archivo": nombre, "filas": len(df), "worker": socket.gethostname()}


if __name__ == "__main__":      # obligatorio en Windows
    client = Client("tcp://<IP_COORD>:8786")
    client.get_versions(check=True)

    carpeta = Path("datos")                 # carpeta local del client
    rutas = sorted(carpeta.glob("*.csv"))
    TAM_LOTE = 10                           # ajusta según tu RAM

    resultados = []
    for i in range(0, len(rutas), TAM_LOTE):
        lote = rutas[i:i + TAM_LOTE]
        datos = client.scatter([r.read_bytes() for r in lote])   # reparte entre workers
        futuros = [client.submit(procesar_bytes, r.name, d) for r, d in zip(lote, datos)]
        resultados.extend(client.gather(futuros))

    for r in resultados:
        print(r)
```

### 8.3 Notas de código válidas en ambos modos

- **Windows:** todo script que cree un `Client` o use multiprocesamiento debe estar protegido con `if __name__ == "__main__":`.
- Las funciones que reciben las tareas deben ser **definibles en el worker**: pon los `import` necesarios dentro de la función o al principio del archivo, y no dependas de variables globales del client.
- Para ver qué hace cada worker, abre el dashboard: `http://<IP_COORD>:8787/status`.

---

## 9. Cambiar de coordinador (escenarios casa / universidad / otro)

La lógica de procesamiento no cambia entre escenarios. Solo cambian **tres cosas**:

| Qué cambia | Cómo |
|---|---|
| Quién ejecuta el scheduler | Esa PC ejecuta `dask scheduler --host <IP_COORD> ...` con su propia IP |
| A dónde se conectan los workers y el client | Todos usan la nueva `<IP_COORD>` en `tcp://<IP_COORD>:8786` |
| Quién aloja los datos (solo Modo A) | La PC que ahora aloja los datos es servidor NFS/SMB; las demás montan desde su IP |

Checklist para invertir roles (ejemplo: la PC worker pasa a coordinar):

1. **Detén** worker y scheduler en todas las PCs (`Ctrl + C`).
2. En la nueva coordinadora: abre los puertos de la sección 3.1 y lanza el scheduler (sección 4).
3. Modo A: en la PC que ahora aloja los datos, configura el servidor (7A.1.2 o 7A.2) y exporta la carpeta; en las demás, **desmonta** el montaje anterior (`sudo umount /mnt/cluster-datos`, o `net use Z: /delete` en Windows) y vuelve a montar apuntando a la nueva IP.
4. Lanza los workers apuntando a la nueva `<IP_COORD>`.
5. Repite la verificación (sección 6).

Una PC puede dejarse preparada para ambos roles: instalar paquetes de servidor y cliente, y tener ya escritas las reglas de firewall. Entonces solo cambias IPs.

---

## 10. Cerrar el clúster

1. `Ctrl + C` en cada terminal de worker.
2. `Ctrl + C` en la terminal del scheduler.
3. (Opcional) Modo A: desmonta las carpetas compartidas.
4. (Opcional) Si ya no vas a usar el servidor NFS: `sudo systemctl disable --now nfs-server.service` (o `nfs-kernel-server` en Debian/Ubuntu/Mint).

---

## 11. Solución de problemas

| Síntoma | Causa probable | Qué hacer |
|---|---|---|
| El worker no logra conectarse al scheduler (`Timed out`, `Connection refused`) | Firewall, IP incorrecta o scheduler no iniciado | `ping <IP_COORD>`, revisa que el scheduler esté corriendo y que el puerto `8786` esté abierto (sección 3.1) |
| El worker se conecta pero las tareas fallan o quedan colgadas | Firewall del **worker** bloquea al scheduler u otros workers | Abre `8789` y `8790` en **todos** los workers (sección 3.2) |
| `Version mismatch` o errores raros de pickle/serialización | Distintas versiones de Python, dask, distributed, pandas o numpy | Iguala versiones en todas las PCs (sección 2.2); `client.get_versions(check=True)` te dice qué difiere |
| El worker anuncia una IP equivocada (por ejemplo la de una VPN, Docker o virtual) | La PC tiene varias interfaces de red | Lanza el worker con `--host <IP_WORKER>`; si aún falla, añade `--contact-address tcp://<IP_WORKER>:8789` |
| `mount.nfs: access denied by server` | La IP del cliente no está en `/etc/exports` | Revisa `/etc/exports`, ejecuta `sudo exportfs -arv` y `sudo exportfs -v` |
| `Permission denied` al listar la carpeta montada por NFS | Permisos de directorios padre o de archivos | `chmod -R o+rX` en la carpeta compartida y `o+x` en los padres (o muévela a `/srv`) |
| `mount.nfs: Connection timed out` | Puerto 2049 bloqueado o servicio NFS apagado | `sudo systemctl status nfs-server` (o `nfs-kernel-server`) y regla de firewall 2049 |
| Un worker Windows no encuentra `Z:\` | El worker corre en una sesión distinta de la que mapeó la unidad (por ejemplo como administrador) | Lanza el worker desde una terminal normal con el mismo usuario, o usa la ruta UNC `\\<IP>\recurso` |
| `FileNotFoundError` solo en algunos workers | `CLUSTER_DATOS` no definida o mal definida en ese worker | Usa `client.run(verificar_datos)` (sección 8.1) para ver qué ve cada uno |
| PowerShell: "la ejecución de scripts está deshabilitada" al activar el venv | Política de ejecución | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` |
| Windows: `dask` no se reconoce como comando | El venv no está activado | Activa con `& $HOME\venv-dask\Scripts\Activate.ps1` |
| Las PCs no se ven entre sí en Wi-Fi de universidad | Aislamiento de clientes en la red | Usa hotspot propio, cable, o VPN de malla (Tailscale/ZeroTier) |
| El dashboard no abre | Falta `bokeh` o puerto `8787` cerrado | Instala `dask[complete]` y abre `8787` en la coordinadora |

Comandos útiles de diagnóstico:

```bash
# Linux: ver puertos en escucha
ss -tlnp | grep -E '8786|8787|8789|8790|2049'
```

```powershell
# Windows: probar si un puerto de otra PC es alcanzable
Test-NetConnection -ComputerName <IP_COORD> -Port 8786
```

---

## 12. Checklist rápido para unirse a un clúster existente

Si alguien ya montó la coordinadora y tú solo quieres ser worker:

1. [ ] Estás en la misma red y haces `ping` a `<IP_COORD>`.
2. [ ] Instalaste el mismo Python y las mismas versiones de `dask`, `distributed`, `pandas` en un entorno virtual (sección 2.2).
3. [ ] Abriste los puertos `8789` y `8790` en tu firewall (sección 3.2).
4. [ ] Modo A: montaste la carpeta compartida y definiste `CLUSTER_DATOS` (sección 7A).
5. [ ] Activaste el entorno virtual y lanzaste el worker con un `--name` único (sección 5).
6. [ ] Quien coordina ve tu worker en el dashboard o con `client.scheduler_info()`.

Si quieres ser tú el coordinador: sigue las secciones 2 → 3.1 → 4 → 5 → 6 → 7 en ese orden.
