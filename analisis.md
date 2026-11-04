# Informe de resultados del clúster Dask

## 1. Resumen

Se procesaron datos de ventas simuladas de Colombia correspondientes a 2025 con Dask Distributed. Los scripts preparan y ejecutan tareas paralelas en workers, limpian los datos, los guardan en Parquet y generan agregados para responder preguntas de negocio.

La carpeta `resultados/` contiene diez archivos CSV. Los resultados agregados por categoría, mes y región coinciden en un total de **$295.465.208.230 COP**. El archivo de rendimiento registra una ejecución en modo `cluster` con 14 hilos disponibles.

## 2. Datos analizados

Los archivos de entrada están en `datos/`:

- 12 CSV mensuales, de enero a diciembre de 2025.
- **9.230.099 filas de ventas** en total, sin contar las cabeceras.
- Aproximadamente **1,06 GB** de CSV mensuales.
- `tiendas.csv`: 40 tiendas.
- `productos.csv`: 52 productos.
- El generador describe una cadena ficticia de tiendas colombianas, con ciudades y regiones, ocho categorías, métodos de pago, descuentos y clientes identificados o no identificados.

El generador (`archivos_profe/scripts/generar_datos.py`) introduce deliberadamente problemas útiles para la práctica: devoluciones con importes negativos, algunos montos vacíos, duplicados y categorías con espacios o mayúsculas inconsistentes. No se encontraron registros de consola del diagnóstico de calidad, por lo que este informe no atribuye cantidades concretas a esos problemas.

## 3. Configuración y ejecución distribuida

La configuración actual de `cluster.yaml` define la red `172.20.10.0/28`, a `linuxMint` (`172.20.10.4`) como coordinadora y el scheduler en el puerto `8786`. También configura el dashboard en `8787`, y los puertos de worker y nanny `8789` y `8790`.

El procedimiento que sigue el proyecto es:

1. **Preparar la misma configuración y los datos en cada equipo.** Cada worker lee los CSV desde su propia carpeta `datos/`; no se transfieren las ventas completas entre máquinas.
2. **Iniciar el scheduler en la coordinadora**, desde el entorno virtual y la carpeta del proyecto:

   ```bash
   python cluster.py scheduler
   ```

3. **Iniciar un worker en cada equipo** que vaya a participar:

   ```bash
   python cluster.py worker
   ```

   El script detecta el nombre del nodo a partir de su IP. También permite especificarlo explícitamente con `python cluster.py worker <nombre>`. El worker verifica que exista su carpeta de datos y configura la ruta local antes de conectarse a `tcp://172.20.10.4:8786`.
4. **Verificar el clúster** una vez conectados los workers:

   ```bash
   python cluster.py verificar
   ```

   Esta comprobación lista los workers, compara versiones, consulta los nombres de host y contrasta los inventarios de CSV.
5. **Conectar los scripts de práctica** al scheduler. `archivos_profe/scripts/config.py` crea el cliente Dask, carga la configuración y prepara las funciones que leen archivos localmente en los workers.
6. **Enviar tareas.** `01_conectar.py` distribuye 200 tareas pequeñas de prueba con `client.map()`. Los scripts posteriores construyen operaciones de Dask DataFrame y las ejecutan con `.compute()`; `04_limpiar.py` distribuye la escritura de particiones Parquet y `05_analizar.py` mantiene los datos limpios en memoria de los workers con `persist()`.

Los scripts documentan y automatizan ese flujo. Los CSV de salida confirman que hubo ejecuciones de análisis y rendimiento en modo clúster; sin embargo, no se conservaron los registros completos de consola ni un listado de nombres de workers para asociar cada resultado a una ejecución concreta.

## 4. Limpieza y preparación

`04_limpiar.py` prepara los datos para el análisis:

- descarta ventas sin `monto`;
- elimina duplicados de `id_venta` dentro de cada partición mensual;
- normaliza las categorías (quita espacios y capitaliza);
- agrega `anio`, `mes` y `es_devolucion`;
- guarda las particiones en formato Parquet en la carpeta `ventas_limpias` local de cada worker y hace una comprobación posterior.

Los resultados comerciales de `05_analizar.py`, `06_combinar.py` y `mi_analisis.py` excluyen las devoluciones al calcular ventas, salvo el análisis específico de devoluciones.

## 5. Resultados obtenidos

### Ventas por categoría

| Categoría | Ventas (COP) | Participación |
|---|---:|---:|
| Electrónica | $185.124.612.670 | 62,7 % |
| Deportes | $31.160.567.475 | 10,5 % |
| Hogar | $23.592.703.830 | 8,0 % |
| Ropa | $21.594.774.315 | 7,3 % |
| Juguetes | $13.202.141.245 | 4,5 % |
| Alimentos | $7.197.607.385 | 2,4 % |
| Belleza | $7.177.360.195 | 2,4 % |
| Libros | $6.415.441.115 | 2,2 % |

Electrónica concentra casi dos tercios de las ventas registradas.

### Ventas por mes

El total anual es **$295.465.208.230 COP**. Diciembre fue el mes con mayores ventas (**$38.418.091.020**), seguido de noviembre (**$29.137.767.905**); enero fue el menor (**$19.498.137.030**). El patrón coincide con el aumento de demanda que el generador asigna a fin de año.

### Ventas por región

| Región | Ventas (COP) |
|---|---:|
| Andina | $161.239.460.585 |
| Pacífica | $72.690.875.150 |
| Caribe | $45.914.014.120 |
| Orinoquía | $15.620.858.375 |

La región Andina representa la mayor suma. Los totales por categoría, mes y región coinciden entre sí.

### Indicadores adicionales

- **Tiendas con mayores ventas:** lideran T019 (Bogotá, **$17.751.180.710**), T013 (Pasto, **$17.626.658.355**) y T034 (Cali, **$17.364.664.865**).
- **Ticket promedio por pago:** tarjeta de crédito, **$620.926**; transferencia, **$292.577**; débito, **$244.339**; efectivo, **$178.063**.
- **Ventas con descuento:** alrededor del 25 % entre enero y octubre y en diciembre; en noviembre, **45,1 %**.
- **Ventas por metro cuadrado:** T013 encabeza el top cinco con **$88.133.292 COP/m²**.
- **Clientes fieles:** se identificaron **1.685 clientes registrados** con compras en los 12 meses de 2025.
- **Devoluciones por categoría:** el CSV reporta razones negativas, porque el script divide la suma de montos de devolución (negativos) por la suma de ventas. Interpretando el valor absoluto como proporción monetaria, va de **0,10 % en Alimentos** a **1,48 % en Ropa**. No es una tasa calculada por número de transacciones.

## 6. Comparación de rendimiento

`resultados/tiempos_cluster.csv` registra una ejecución con `modo=cluster` y **14 hilos**:

| Método | Tiempo |
|---|---:|
| Dask leyendo CSV | 2,43 s |
| Dask leyendo Parquet (todas las columnas) | 9,38 s |
| Dask leyendo Parquet (solo dos columnas) | 2,05 s |
| pandas en una PC | 8,14 s |

En esta ejecución, Dask leyendo CSV tardó aproximadamente 3,35 veces menos que pandas en una PC; Parquet con solo las columnas necesarias, unas 3,97 veces menos. Leer todas las columnas desde Parquet fue más lento que los otros métodos en esta medición. La selección de columnas reduce el trabajo de lectura y fue el método más rápido registrado.

Son mediciones de una sola ejecución, no una comparación controlada con varias repeticiones; pueden variar por caché, disco y carga de las máquinas. Además, el resultado registra 14 hilos, mientras que la configuración actual suma 27 hilos entre sus cuatro nodos. Por ello, no se debe asumir que los resultados de rendimiento corresponden exactamente a la configuración actual ni que participaron todos los nodos configurados.

## 7. Archivos de resultados

- `resultados/A_ventas_por_categoria.csv`: ventas y participación por categoría.
- `resultados/B_ventas_por_mes.csv`: ventas mensuales.
- `resultados/D_ventas_por_region.csv`: ventas por región.
- `resultados/1_top_tiendas.csv`: diez tiendas con más ventas.
- `resultados/2_ticket_pago.csv`: ticket promedio por método de pago.
- `resultados/3_descuentos.csv`: porcentaje de ventas con descuento por mes.
- `resultados/4_ventas_m2.csv`: cinco tiendas con más ventas por metro cuadrado.
- `resultados/5_devoluciones.csv`: razón monetaria de devoluciones por categoría, con signo negativo.
- `resultados/6_clientes_fieles.csv`: clientes registrados con compras en los doce meses.
- `resultados/tiempos_cluster.csv`: comparación de tiempos de procesamiento.

## 8. Conclusiones

La práctica muestra el recorrido completo de un análisis distribuido: el scheduler coordina el trabajo, los workers procesan sus copias locales, Dask combina los agregados y los scripts guardan resultados pequeños para su consulta. En los datos analizados destaca el peso de Electrónica, el aumento de ventas en noviembre y diciembre, y la concentración regional en la zona Andina. La medición disponible favorece la lectura de CSV y, especialmente, la lectura selectiva de columnas en Parquet; hace falta repetir las pruebas bajo condiciones controladas para obtener una conclusión general sobre rendimiento.
