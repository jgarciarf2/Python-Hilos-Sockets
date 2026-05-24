# 📦 Modificaciones del Parcial – Central de Pedidos (Sockets + Hilos)

Cada carpeta contiene una **modificación independiente** del taller base con `servidor.py` y `cliente.py` completamente documentados. Están diseñadas para practicar para el parcial sin IA ni internet.

---

## ¿Cómo ejecutar cada modificación?

```bash
# Terminal 1 — Iniciar el servidor
python servidor.py

# Terminal 2 (y más) — Iniciar clientes
python cliente.py
```

---

## 📋 Índice de Modificaciones

| #   | Carpeta                             | Descripción resumida                                                    | Concepto clave                   |
| --- | ----------------------------------- | ----------------------------------------------------------------------- | -------------------------------- |
| 1   | `mod01_prioridad_pedidos/`          | Los pedidos tienen prioridad (alta/media/baja). La cola se ordena.      | Ordenamiento, lógica de cola     |
| 2   | `mod02_limite_pedidos_por_cliente/` | Cada cliente puede hacer máximo N pedidos.                              | Lock, diccionario compartido     |
| 3   | `mod03_registro_log_archivo/`       | Los logs se guardan en archivo `.txt` además de consola.                | `logging`, FileHandler           |
| 4   | `mod04_reintento_automatico/`       | El cliente reintenta automáticamente si la cola está llena.             | Bucle de reintentos, sleep       |
| 5   | `mod05_estadisticas_servidor/`      | El servidor muestra estadísticas completas al finalizar.                | Lock, datetime, contadores       |
| 6   | `mod06_broadcast_estado/`           | El servidor notifica a todos los clientes cuando se despacha un pedido. | Broadcast, lista de sockets      |
| 7   | `mod07_timeout_cliente/`            | Si el servidor no responde en N segundos, el cliente/servidor abortan.  | `settimeout()`, `socket.timeout` |
| 8   | `mod08_cantidad_fija_pedidos/`      | El usuario elige cuántos pedidos enviar (entrada interactiva).          | `input()`, validación            |
| 9   | `mod09_procesadores_dinamicos/`     | El número de procesadores sube/baja según la carga de la cola.          | Escalado dinámico, Event         |
| 10  | `mod10_consulta_stock/`             | El cliente puede consultar el stock antes de hacer un pedido.           | Nuevo tipo de mensaje, protocolo |

---

## 🧠 Conceptos evaluados en cada modificación

### Mod 1 – Prioridad de pedidos

- Modificar la estructura de la cola para que NO sea FIFO puro
- Ordenar con `sorted()` o `bisect.insort()`
- El JSON del pedido lleva un campo extra: `"prioridad": 1`

### Mod 2 – Límite por cliente

- Usar un diccionario compartido protegido con `Lock`
- Contar pedidos por cliente y rechazar cuando se supera el límite
- El servidor devuelve un `error` específico al cliente

### Mod 3 – Registro en archivo

- Módulo `logging` con `FileHandler` + `StreamHandler`
- El logging de Python ya es thread-safe internamente
- Formato con timestamp, nombre de hilo, mensaje

### Mod 4 – Reintento automático

- El cliente detecta respuesta de error `"estado": "rechazado"`
- Usa `time.sleep()` entre intentos
- Máximo de reintentos configurable con constante

### Mod 5 – Estadísticas

- Diccionario de estadísticas protegido con `Lock`
- Acumuladores thread-safe para pedidos por producto/cliente
- Uso de `datetime` para medir duración total

### Mod 6 – Broadcast

- Lista de sockets activos protegida con `Lock`
- Función `broadcast()` itera sobre todos los sockets y envía
- Cliente tiene hilo receptor de broadcasts en paralelo

### Mod 7 – Timeout

- `socket.settimeout(n)` cambia el socket a modo no-bloqueante con timeout
- `socket.timeout` (excepción) se captura para manejar inactividad
- Evita clientes "zombis" que consumen recursos indefinidamente

### Mod 8 – Entrada de usuario

- `input()` en el cliente para preguntar cantidad y producto
- Validación de entrada con `try/except ValueError`
- Manejo de `KeyboardInterrupt` para salir limpiamente

### Mod 9 – Procesadores dinámicos

- Hilo monitor revisa la cola cada N segundos
- Crea nuevos hilos `Thread` en tiempo de ejecución
- `threading.Event` para señalar a procesadores que terminen

### Mod 10 – Consulta de stock

- Nuevo tipo de mensaje `"consulta_stock"` en el protocolo
- El servidor reutiliza `lock_stock` existente para responder
- El cliente decide si pedir basándose en el stock disponible

---

## 💡 Tips para el parcial (sin IA, sin internet)

1. **El protocolo JSON**: Siempre es `json.dumps(dict).encode("utf-8")` para enviar y `json.loads(datos.decode("utf-8"))` para recibir.
2. **El Lock**: Usa siempre `with lock:` en lugar de `lock.acquire()/release()` para evitar olvidar liberarlo.
3. **El Semáforo**: `acquire()` = "ocupo un espacio", `release()` = "libero un espacio". El productor hace acquire, el consumidor hace release.
4. **La Barrera**: Todos los hilos que la cruzan se bloquean hasta que **todos** llamen a `.wait()`. El número de participantes se define al crear la barrera.
5. **El Event**: `.set()` = encender la bandera, `.is_set()` = consultar, `.wait()` = bloquear hasta que esté encendida.
6. **daemon=True en hilos**: Si el programa principal termina, los hilos daemon también terminan. Sin esto, el programa no cierra.
