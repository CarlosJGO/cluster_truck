# Guía Dask: clúster con datos locales (coordinadora + workers)

Esta guía explica cómo montar un clúster de Dask entre varias PCs de la misma red **sin carpetas compartidas** (sin NFS ni SMB). Cada PC tiene los datos en su propio disco; la coordinadora solo envía **nombres de archivo** y recibe **resultados**. Sirve para equipos mixtos Linux (Mint, Ubuntu, Arch) y Windows 11.

---

## 0. Cómo leer esta guía

### Roles

| Rol | Qué hace | Cuántos |
| --- | --- | --- |
| **PC coordinadora** | Ejecuta el *scheduler* (reparte el trabajo) y el *client* (tu script). Puede ser también worker. | 1 |
| **PC worker** | Ejecuta un *worker* que procesa tareas leyendo sus datos locales. | 1 o más |

### Marcadores que debes reemplazar

| Marcador | Significado | Ejemplo |
| --- | --- | --- |
| `<IP_COORD>` | IP de la PC coordinadora | `192.168.1.11` |
| `<IP_WORKER>` | IP de la PC worker en la que estás | `192.168.1.9` |
| `<RED_LAN>` | Red local en notación CIDR | `192.168.1.0/24` |
| `<NOMBRE_WORKER>` | Nombre único por worker | `worker-ana` |
| `<N_HILOS>` | Hilos del worker (normalmente = núcleos) | `4` |
| `<VERSION>` | Versión acordada de dask/distributed | la que decida el equipo |

### Convenciones

- Bloques `bash` → Linux. Bloques `powershell` → Windows 11 (como administrador solo cuando se indique).
- Los comandos largos van **en una sola línea** a propósito: el carácter de continuación cambia entre Linux, PowerShell y cmd.
- **Todos los comandos `dask ...` se ejecutan con el entorno virtual activado.**

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
   │    WORKER 1     │   │    WORKER 2     │   │    WORKER 3     │
   │ worker :8789    │   │ worker :8789    │   │ worker :8789    │
   │ nanny  :8790    │   │ nanny  :8790    │   │ nanny  :8790    │
   │ datos locales   │   │ datos locales   │   │ datos locales   │
   └─────────────────┘   └─────────────────┘   └─────────────────┘
```

Qué viaja por la red:

- **Nombres de archivo** (coordinadora → workers) y **resultados** (workers → coordinadora).
- **Los datos de entrada no viajan**: cada worker los lee de su propio disco.
- Si una tarea necesita el resultado de otra, Dask lo transfiere directo entre workers. Por eso los puertos de los workers deben estar abiertos en todos.

> **Seguridad:** Dask no tiene autenticación por defecto. Quien llegue al puerto 8786 puede ejecutar código en todos los workers. Úsalo solo en redes de confianza, nunca expuesto a internet.

### Puertos

| Puerto | Quién lo abre | Para qué |
| --- | --- | --- |
| `8786/tcp` | Coordinadora | Scheduler (workers y client se conectan aquí) |
| `8787/tcp` | Coordinadora | Dashboard web (solo lectura, opcional) |
| `8789/tcp` | Cada worker | Puerto del worker |
| `8790/tcp` | Cada worker | Puerto del *nanny* (supervisor del worker) |

Si lanzas varios procesos en una misma PC (`--nworkers N`), usa rangos: `--worker-port 8789:8799 --nanny-port 8800:8810` y abre esos rangos.

---

## 2. Requisitos previos (en TODAS las PCs)

### 2.1 Red

- Todas las PCs en la **misma red local** y con `ping` entre ellas.
- Idealmente **IP fija** (o reserva DHCP en el router). Si la IP cambia, hay que reconfigurar todo.
- Averigua tu IP:

```bash
ip -4 addr show | grep inet
```

```powershell
ipconfig
```

- Prueba conectividad hacia la coordinadora:

```bash
ping <IP_COORD>
```

- **Windows:** la red debe estar como **Privada** (Configuración → Red e Internet → propiedades de la red → Tipo de perfil de red: Privada), porque las reglas de firewall de esta guía se crean para ese perfil.
- **Wi-Fi institucional (universidad, oficina):** muchas redes aíslan a los clientes entre sí. Si no puedes ni hacer `ping`, usa un hotspot propio, cable a un switch, o una VPN de malla como **Tailscale** o **ZeroTier** (usa entonces las IPs de la VPN como `<IP_COORD>` e `<IP_WORKER>`).

### 2.2 Python y librerías: mismas versiones en todas las PCs

El error más común: versiones distintas de Python, `dask`, `distributed`, `pandas` o `numpy` provocan fallos raros de serialización. Acuerden una versión y fíjenla.

**Linux:**

```bash
python3 --version
python3 -m venv ~/venv-dask
source ~/venv-dask/bin/activate
pip install --upgrade pip
pip install "dask[complete]==<VERSION>" "distributed==<VERSION>" pandas pyarrow
```

**Windows (PowerShell):**

```powershell
py --version
py -m venv $HOME\venv-dask
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
& $HOME\venv-dask\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install "dask[complete]==<VERSION>" "distributed==<VERSION>" pandas pyarrow
```

(`Set-ExecutionPolicy` solo la primera vez, para poder activar el venv. `pyarrow` se usa para guardar resultados en parquet.)

> En Arch/CachyOS el `pip install` global está bloqueado (PEP 668): usa siempre el entorno virtual.

Compara versiones ejecutando esto en cada PC; la salida debe ser igual (Python solo en mayor.menor):

```bash
python -c "import sys, dask, distributed, pandas; print(sys.version.split()[0], dask.__version__, distributed.__version__, pandas.__version__)"
```

---

## 3. Datos locales

Cada PC tiene los datos en una carpeta de su propio disco. La ruta puede ser **distinta en cada PC**; para que el código no dependa de eso, cada worker define la variable de entorno `CLUSTER_DATOS` con su ruta local.

### 3.1 Elegir la carpeta

- Usa rutas **sin espacios ni tildes**: por ejemplo `/srv/cluster/datos` o `~/cluster/datos` en Linux, `C:\cluster\datos` en Windows.
- Misma estructura y mismos nombres de archivo en todas las PCs.

### 3.2 Dos estrategias de reparto

|  | **Copia completa en todos** | **Datos repartidos** |
| --- | --- | --- |
| Idea | Cada PC tiene todos los archivos | Cada PC tiene solo su parte |
| Ventaja | Cualquier worker procesa cualquier archivo; si uno se cae, otro toma su trabajo | Ahorra disco |
| Desventaja | Cada PC necesita espacio para todo | Si un worker se apaga, su parte no se procesa; hay que fijar cada tarea a su dueño |
| Recomendada si... | Los datos caben en cada disco | No caben en todos los discos |

**Dask no sabe quién tiene qué.** Con copia completa no importa. Con datos repartidos hay que indicar a qué worker va cada tarea (ver sección 8.2).

### 3.3 Cómo copiar los datos a cada PC

Es un paso manual, de una sola vez. Opciones: USB o disco externo, nube compartida, `scp`/`rsync`, o **Syncthing** si los datos cambian y quieren mantenerlos sincronizados automáticamente entre las PCs.

```bash
# Ejemplo con rsync desde Linux hacia otra PC Linux
rsync -avh --progress /srv/cluster/datos/ usuario@<IP_WORKER>:/srv/cluster/datos/
```

```powershell
# Ejemplo en Windows 11 (scp viene incluido) hacia una carpeta del otro equipo
scp -r C:\cluster\datos\* usuario@<IP_WORKER>:/srv/cluster/datos/
```

### 3.4 Los datos deben ser idénticos

Si alguien tiene una versión distinta o un archivo incompleto, los resultados salen mal **sin dar error**. Antes de cada ejecución importante, verifica el inventario (sección 7).

---

## 4. Firewall

Permite tráfico entrante según el rol, limitado a la red local (`<RED_LAN>`).

### 4.1 PC coordinadora

**Linux (ufw):**

```bash
sudo ufw allow from <RED_LAN> to any port 8786 proto tcp
sudo ufw allow from <RED_LAN> to any port 8787 proto tcp
```

**Windows (PowerShell como administrador):**

```powershell
New-NetFirewallRule -DisplayName "Dask scheduler" -Direction Inbound -Protocol TCP -LocalPort 8786,8787 -RemoteAddress <RED_LAN> -Profile Private -Action Allow
```

### 4.2 Cada PC worker (incluida la coordinadora si también será worker)

**Linux (ufw):**

```bash
sudo ufw allow from <RED_LAN> to any port 8789:8790 proto tcp
```

**Windows (PowerShell como administrador):**

```powershell
New-NetFirewallRule -DisplayName "Dask worker" -Direction Inbound -Protocol TCP -LocalPort 8789,8790 -RemoteAddress <RED_LAN> -Profile Private -Action Allow
```

### Notas

- Si `sudo ufw status` dice **inactive**, ufw no bloquea nada (verifica que no haya otro firewall como `firewalld` o `nftables`).
- En Arch/CachyOS, ufw puede no estar instalado: `sudo pacman -S ufw && sudo systemctl enable --now ufw`.
- En Windows, si aparece la ventana "Windows Defender Firewall ha bloqueado algunas características", marca **Redes privadas** y acepta.

---

## 5. Levantar el scheduler (PC coordinadora)

Con el entorno virtual activado:

```bash
dask scheduler --host <IP_COORD> --port 8786
```

Debe mostrar `Scheduler at: tcp://<IP_COORD>:8786`. **Deja esa terminal abierta.**

El dashboard queda en `http://<IP_COORD>:8787/status`. Es de solo lectura: cualquiera de la red puede verlo, pero no puede modificar nada desde ahí.

---

## 6. Levantar workers

Cada worker usa **una terminal propia**, con el entorno virtual activado y la variable `CLUSTER_DATOS` definida **en esa misma terminal**.

### 6.1 Worker en Linux

```bash
export CLUSTER_DATOS=/srv/cluster/datos
dask worker tcp://<IP_COORD>:8786 --name <NOMBRE_WORKER> --nthreads <N_HILOS> --host <IP_WORKER> --worker-port 8789 --nanny-port 8790
```

### 6.2 Worker en Windows (PowerShell)

```powershell
$env:CLUSTER_DATOS = "C:\cluster\datos"
dask worker tcp://<IP_COORD>:8786 --name <NOMBRE_WORKER> --nthreads <N_HILOS> --host <IP_WORKER> --worker-port 8789 --nanny-port 8790
```

### 6.3 Worker en la misma PC coordinadora

Es el mismo comando, usando `<IP_COORD>` como `<IP_WORKER>`, en una segunda terminal.

### Consejos

- Cada worker debe tener un `--name` **distinto**.
- Para trabajo con pandas/CSV (mucho Python puro) suele rendir mejor **más procesos con pocos hilos**. Ejemplo en una PC de 8 núcleos: `--nworkers 4 --nthreads 2` (con rangos de puertos, ver sección 1). Con `--nworkers` los nombres se derivan de `--name`.
- Si usas la PC para otras cosas, deja libres algunos núcleos y algo de RAM (usa menos hilos que núcleos).
- Para detener un worker o el scheduler: `Ctrl + C` en su terminal.

---

## 7. Verificar el clúster

Desde la coordinadora, con el entorno virtual activado. Guarda esto como `verificar.py`:

```python
# verificar.py
import os
import socket
from dask.distributed import Client


def inventario():
    """Se ejecuta EN cada worker: lista los CSV de su carpeta local con su tamaño."""
    carpeta = os.environ["CLUSTER_DATOS"]
    return {
        f: os.path.getsize(os.path.join(carpeta, f))
        for f in sorted(os.listdir(carpeta))
        if f.endswith(".csv")
    }


if __name__ == "__main__":      # obligatorio en Windows
    client = Client("tcp://<IP_COORD>:8786")

    # 1) ¿Están todos los workers?
    for addr, w in client.scheduler_info()["workers"].items():
        print(w["name"], addr, "hilos:", w["nthreads"])

    # 2) ¿Las versiones coinciden? (lanza error si hay diferencias)
    client.get_versions(check=True)

    # 3) ¿En qué máquina corre cada worker?
    print(client.run(socket.gethostname))

    # 4) ¿Todos tienen los mismos datos?
    inv = client.run(inventario)
    referencia = next(iter(inv.values()))
    for worker, datos in inv.items():
        estado = "OK" if datos == referencia else "DIFERENTE"
        print(worker, len(datos), "archivos", estado)
```

(Si usas la estrategia de **datos repartidos**, el paso 4 no debe dar "iguales": en su lugar imprime `inv` y comprueba que cada worker tiene la parte que le corresponde.)

Resultado esperado: todos los workers listados, `get_versions` sin errores, un nombre de máquina por worker y todos los inventarios en OK.

---

## 8. Ejemplos de código

### Interfaz web local

Desde la carpeta del proyecto y con el entorno virtual activado, inicia la interfaz:

```bash
python web_config.py
```

Abre `http://127.0.0.1:8000` en el navegador para editar la red, los puertos y los nodos; guarda los cambios en `cluster.yaml`. La interfaz escucha solo en la PC local. Para procesar, el scheduler debe estar activo y debes seleccionar como fuente un nodo cuya ruta de datos exista en la PC donde corre la interfaz. El botón ejecuta `procesar.py` y muestra su salida en la página. Los workers procesan los archivos desde sus propias rutas configuradas; cada worker debe tener una copia completa de los mismos CSV. Detén el servidor web con `Ctrl+C`.

### 8.1 Copia completa en todos (cualquier worker procesa cualquier archivo)

El client solo pasa **nombres**; cada worker resuelve la ruta con su `CLUSTER_DATOS`. El resultado grande se guarda en el disco local del worker y solo se devuelve un resumen.

```python
# procesar.py
import os
import socket
import pandas as pd
from dask.distributed import Client


def datos_dir():
    return os.environ["CLUSTER_DATOS"]


def salida_dir():
    carpeta = os.path.join(datos_dir(), "resultados")
    os.makedirs(carpeta, exist_ok=True)
    return carpeta


def listar_csv():
    return sorted(f for f in os.listdir(datos_dir()) if f.endswith(".csv"))


def procesar_archivo(nombre):
    ruta = os.path.join(datos_dir(), nombre)      # se resuelve en el worker
    df = pd.read_csv(ruta)

    # ... tu procesamiento aquí ...
    resumen = {"archivo": nombre, "filas": len(df), "worker": socket.gethostname()}

    # Resultado grande: se guarda local y solo se devuelve la ruta
    destino = os.path.join(salida_dir(), nombre.replace(".csv", ".parquet"))
    df.to_parquet(destino)
    resumen["guardado_en"] = destino
    return resumen


if __name__ == "__main__":      # obligatorio en Windows
    client = Client("tcp://<IP_COORD>:8786")
    client.get_versions(check=True)

    nombres = client.submit(listar_csv, pure=False).result()   # lista desde un worker
    futuros = client.map(procesar_archivo, nombres)
    for r in client.gather(futuros):
        print(r)
```

### 8.2 Datos repartidos (fijar cada tarea a su dueño)

Si cada PC solo tiene una parte, indica explícitamente qué worker procesa qué archivos. Los nombres de worker son los que usaste en `--name`.

```python
# repartido.py
from dask.distributed import Client
from procesar import procesar_archivo    # reutiliza la función del ejemplo anterior

REPARTO = {
    "worker-ana":  ["datos_01.csv", "datos_02.csv"],
    "worker-luis": ["datos_03.csv", "datos_04.csv"],
    "worker-eva":  ["datos_05.csv", "datos_06.csv"],
}

if __name__ == "__main__":
    client = Client("tcp://<IP_COORD>:8786")
    client.upload_file("procesar.py")    # envía el módulo a todos los workers

    futuros = []
    for worker, archivos in REPARTO.items():
        for nombre in archivos:
            futuros.append(
                client.submit(procesar_archivo, nombre, workers=[worker], allow_other_workers=False)
            )

    for r in client.gather(futuros):
        print(r)
```

Si un worker de la lista no está conectado, esas tareas quedan esperando: revisa antes con la verificación de la sección 7.

### 8.3 Notas válidas para ambos ejemplos

- **Windows:** todo script que cree un `Client` debe estar bajo `if __name__ == "__main__":`.
- Las funciones que se envían a los workers deben poder ejecutarse allí: pon los `import` necesarios al inicio del archivo o dentro de la función, y no dependas de variables globales del client.
- **Módulos propios:** si importas archivos `.py` tuyos, los workers también los necesitan: `client.upload_file("mi_modulo.py")` los envía a todos los workers conectados. (Las funciones definidas directamente en el script que lanzas viajan solas.)
- Todas las librerías (pandas, numpy, pyarrow…) deben estar instaladas en cada worker con la misma versión.

---

## 9. Resultados: qué viaja y qué no

Todo lo que haces `gather` llega a la RAM de la PC coordinadora.

| Tipo de resultado | Qué hacer |
| --- | --- |
| Pequeño (conteos, estadísticas, un DataFrame resumido) | `gather` directo |
| Mediano (miles o millones de filas) | Funciona, pero agrega o filtra en el worker antes de devolver |
| Grande (tanto como la entrada) | No lo traigas al client: que cada worker lo **guarde en su disco local** (parquet) y devuelva solo la ruta o un resumen |

En el tercer caso los resultados quedan repartidos entre las PCs. Si luego los necesitas todos juntos, cópialos (USB, `scp`, Syncthing) o haz una segunda etapa donde cada worker envíe su parte.

---

## 10. Cambiar de coordinador y cerrar el clúster

### Cambiar de coordinador

Solo cambian **dos cosas**: la PC que ejecuta el scheduler y la IP a la que todos se conectan.

1. **Detén** workers y scheduler en todas las PCs (`Ctrl + C`).
2. En la nueva coordinadora: abre los puertos de la sección 4.1 y lanza el scheduler (sección 5).
3. Lanza los workers apuntando a la nueva `<IP_COORD>`.
4. Repite la verificación (sección 7).

### Cerrar el clúster

1. `Ctrl + C` en cada terminal de worker.
2. `Ctrl + C` en la terminal del scheduler.

---

## 11. Solución de problemas

| Síntoma | Causa probable | Qué hacer |
| --- | --- | --- |
| El worker no se conecta al scheduler (`Timed out`, `Connection refused`) | Firewall, IP incorrecta o scheduler no iniciado | `ping <IP_COORD>`, revisa que el scheduler esté corriendo y el puerto `8786` abierto (sección 4.1) |
| El worker se conecta pero las tareas fallan o quedan colgadas | El firewall del **worker** bloquea al scheduler u otros workers | Abre `8789` y `8790` en **todos** los workers (sección 4.2) |
| `Version mismatch` o errores raros de pickle | Versiones distintas de Python, dask, distributed, pandas o numpy | Iguala versiones (sección 2.2); `client.get_versions(check=True)` indica qué difiere |
| El worker anuncia una IP equivocada (VPN, Docker, virtual) | La PC tiene varias interfaces de red | Lanza con `--host <IP_WORKER>`; si aún falla, añade `--contact-address tcp://<IP_WORKER>:8789` |
| `FileNotFoundError` o `KeyError: 'CLUSTER_DATOS'` solo en algunos workers | Variable no definida o mal definida en esa terminal | Define `CLUSTER_DATOS` en la **misma terminal** donde lanzas el worker y revisa con `client.run(inventario)` |
| Resultados distintos según el worker que procesa | Datos distintos entre PCs | Revisa el inventario (sección 7); compara tamaños o hashes de los archivos |
| PowerShell: "la ejecución de scripts está deshabilitada" al activar el venv | Política de ejecución | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` |
| Windows: `dask` no se reconoce como comando | El venv no está activado | `& $HOME\venv-dask\Scripts\Activate.ps1` |
| Las PCs no se ven en la Wi-Fi de la universidad | Aislamiento de clientes | Hotspot propio, cable, o VPN de malla (Tailscale/ZeroTier) |
| El dashboard no abre | Falta `bokeh` o puerto `8787` cerrado | Instala `dask[complete]` y abre `8787` en la coordinadora |
| Tareas fijadas a un worker que no está conectado | El nombre de `workers=[...]` no coincide con el `--name` | Revisa los nombres con `client.scheduler_info()` |

Diagnóstico rápido:

```bash
# Linux: ver puertos en escucha
ss -tlnp | grep -E '8786|8787|8789|8790'
```

```powershell
# Windows: probar si un puerto de otra PC es alcanzable
Test-NetConnection -ComputerName <IP_COORD> -Port 8786
```

---

## 12. Checklists

### Para ser coordinador (secciones 2 → 3 → 4.1 → 5 → 6 → 7)

- [ ] Python y librerías con la versión acordada, en un venv (2.2).
- [ ] Carpeta de datos local y `CLUSTER_DATOS` definida (3).
- [ ] Puertos 8786/8787 abiertos (4.1) y, si también eres worker, 8789/8790 (4.2).
- [ ] Scheduler corriendo (5).
- [ ] Workers conectados y `verificar.py` en OK (7).

### Para unirse como worker

- [ ] Estás en la misma red y haces `ping` a `<IP_COORD>`.
- [ ] Mismo Python y mismas versiones de `dask`, `distributed`, `pandas` en un venv (2.2).
- [ ] Datos copiados en tu disco, idénticos a los del resto (3).
- [ ] Puertos 8789 y 8790 abiertos en tu firewall (4.2).
- [ ] Red de Windows en perfil **Privado**.
- [ ] Venv activado, `CLUSTER_DATOS` definida y worker lanzado con `--name` único (6), desde una terminal normal.
- [ ] La coordinadora te ve en el dashboard o con `client.scheduler_info()`.
