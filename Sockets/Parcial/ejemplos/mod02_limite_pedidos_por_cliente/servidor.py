"""
=============================================================================
ARCHIVO: servidor.py
MÓDULO:  mod02_limite_pedidos_por_cliente
AUTOR:   Programación Concurrente — Python
FECHA:   2026
=============================================================================

DESCRIPCIÓN GENERAL
-------------------
Servidor TCP concurrente que implementa la MODIFICACIÓN 2 sobre el servidor
base: LÍMITE MÁXIMO DE PEDIDOS POR CLIENTE.

CAMBIOS RESPECTO AL SERVIDOR BASE
-----------------------------------
1. Se agrega la constante MAX_PEDIDOS_POR_CLIENTE = 3.
   - Establece cuántos pedidos exitosos (no rechazados) puede hacer un mismo
     cliente durante toda su sesión de conexión.
   - Es un mecanismo de equidad (fairness) entre clientes: impide que un solo
     cliente monopolice la capacidad del servidor enviando decenas de pedidos.

2. Se agrega el diccionario contadores_por_cliente = {nombre_cliente: int}.
   - Permite al servidor rastrear cuántos pedidos ha procesado cada cliente
     identificado por su nombre (campo "cliente" del JSON recibido).
   - El contador se incrementa solo cuando el pedido es aceptado y encolado.

3. Se agrega lock_contadores (threading.Lock independiente).
   - El diccionario es un recurso compartido entre múltiples hilos de atención.
   - Sin un Lock, dos hilos podrían leer el mismo contador, ambos decidir que
     no se ha alcanzado el límite, y ambos aceptar pedidos que en conjunto
     superan MAX_PEDIDOS_POR_CLIENTE → condición de carrera (race condition).
   - lock_contadores serializa el acceso al diccionario para garantizar
     consistencia. Es un Lock SEPARADO del lock principal para evitar
     contención innecesaria (principio de bloqueo de granularidad fina).

4. Lógica de rechazo en manejar_cliente().
   - Antes de encolar el pedido, el servidor consulta el contador del cliente.
   - Si contador >= MAX_PEDIDOS_POR_CLIENTE → envía JSON de error y retorna
     sin encolar ni decrementar el semáforo.
   - Mensaje de rechazo estándar:
       {"tipo": "error", "mensaje": "Límite de pedidos alcanzado",
        "estado": "rechazado"}

5. Logs enriquecidos con contador por cliente.
   - Cada vez que se acepta o rechaza un pedido se imprime cuántos pedidos
     lleva ese cliente, facilitando la depuración y el monitoreo.

ESTRUCTURA DE CONCURRENCIA CONSERVADA DEL BASE
-----------------------------------------------
- threading.Semaphore(CAPACIDAD_MAXIMA_COLA): controla cuántos pedidos pueden
  estar simultáneamente en la cola de procesamiento.
- threading.Lock (lock_stock): protege el diccionario stock_productos.
- threading.Barrier (barrera_procesamiento): sincroniza el arranque de los
  hilos procesadores cuando hay pedidos suficientes esperando.
- threading.Event (evento_apagado): señal para detener los hilos procesadores.
- threading.Lock (lock_contadores): NUEVO — protege contadores_por_cliente.

PROTOCOLO JSON SOBRE TCP
-------------------------
Solicitud del cliente:
  {"tipo": "pedido", "cliente": "<nombre>", "producto": "<nombre>",
   "cantidad": <int>}

Respuesta de éxito:
  {"tipo": "confirmacion", "estado": "aceptado",
   "mensaje": "Pedido encolado correctamente"}

Respuesta de rechazo por límite:
  {"tipo": "error", "mensaje": "Límite de pedidos alcanzado",
   "estado": "rechazado"}

Respuesta de rechazo por stock:
  {"tipo": "error", "mensaje": "Stock insuficiente", "estado": "rechazado"}

Respuesta de rechazo por cola llena:
  {"tipo": "error", "mensaje": "Servidor ocupado, intente más tarde",
   "estado": "rechazado"}
"""

# =============================================================================
# IMPORTACIONES
# =============================================================================
import socket        # Comunicación TCP/IP: socket, bind, listen, accept, recv, send
import threading     # Concurrencia: Lock, Semaphore, Barrier, Event, Thread
import json          # Serialización del protocolo de mensajes
import time          # sleep() para simular procesamiento
import queue         # Cola thread-safe para los pedidos pendientes

# =============================================================================
# CONSTANTES DE CONFIGURACIÓN DE RED
# =============================================================================

HOST = "127.0.0.1"
"""str: Dirección IP de escucha. 127.0.0.1 = loopback (solo tráfico local).
   Para aceptar conexiones externas se usaría "0.0.0.0"."""

PORT = 65000
"""int: Puerto TCP. Rango 1024-65535 para puertos no privilegiados."""

ENCODING = "utf-8"
"""str: Codificación de los mensajes JSON. UTF-8 soporta caracteres latinos."""

BUFFER_SIZE = 4096
"""int: Tamaño máximo del buffer de recepción en bytes.
   4096 bytes es suficiente para cualquier mensaje JSON del protocolo."""

# =============================================================================
# CONSTANTES DE CONTROL DE CARGA
# =============================================================================

CAPACIDAD_MAXIMA_COLA = 10
"""int: Número máximo de pedidos que pueden estar encolados simultáneamente.
   El Semaphore implementa este límite: cada acquire() representa un slot
   ocupado en la cola; si todos los slots están ocupados, el pedido se rechaza
   (acquire con timeout=0, es decir, no bloqueante)."""

NUM_PROCESADORES = 3
"""int: Número de hilos dedicados a procesar pedidos de la cola.
   Cada hilo corre la función procesar_pedidos() en paralelo."""

PEDIDOS_MINIMOS_PARA_BARRERA = 5
"""int: La Barrier espera a que haya al menos este número de pedidos
   registrados antes de que los procesadores comiencen a trabajar.
   Sirve como mecanismo de arranque coordinado (bulk processing)."""

MAX_CLIENTES = 5
"""int: Máximo de conexiones de clientes activas simultáneamente.
   socket.listen(MAX_CLIENTES) configura el backlog del socket."""

# =============================================================================
# MODIFICACIÓN 2: CONSTANTE DE LÍMITE DE PEDIDOS POR CLIENTE
# =============================================================================

MAX_PEDIDOS_POR_CLIENTE = 3
"""int: NUEVO EN MOD-02 — Número máximo de pedidos que un mismo cliente
   (identificado por el campo "cliente" del JSON) puede realizar durante
   toda su sesión de conexión con el servidor.

   PROPÓSITO:
   ----------
   - Equidad entre clientes (fairness): evita que un cliente con muchos
     productos acapare toda la capacidad de la cola.
   - Protección contra abuso: un cliente malintencionado no puede saturar
     el servidor enviando pedidos infinitos.
   - Control de SLA (Service Level Agreement): garantizar que todos los
     clientes tengan oportunidades similares de ser atendidos.

   EFECTO OBSERVABLE:
   ------------------
   - El cliente envía hasta 5 pedidos. Los primeros 3 son aceptados.
   - Los pedidos 4 y 5 son rechazados con el mensaje de error estándar.
   - El cliente muestra el rechazo en consola y continúa al siguiente intento.
"""

# =============================================================================
# STOCK DE PRODUCTOS
# =============================================================================

stock_productos = {
    "Laptop":      10,
    "Mouse":       25,
    "Teclado":     20,
    "Monitor":      8,
    "Auriculares": 15,
    "USB":         30,
    "Cargador":    18,
    "Webcam":      12,
}
"""dict[str, int]: Inventario del almacén. Clave = nombre de producto,
   valor = unidades disponibles. Se modifica cuando se procesa un pedido."""

# =============================================================================
# ESTRUCTURAS DE DATOS COMPARTIDAS Y SUS PRIMITIVAS DE SINCRONIZACIÓN
# =============================================================================

cola_pedidos = queue.Queue()
"""queue.Queue: Cola FIFO thread-safe de pedidos aceptados pendientes de
   procesar. queue.Queue usa internamente un Lock y un Condition, por lo que
   las operaciones put() y get() son atómicas sin necesidad de Lock externo."""

# --- Lock para el stock de productos ---
lock_stock = threading.Lock()
"""threading.Lock: Mutex que protege el acceso a stock_productos.
   Sin este lock, dos hilos podrían leer el stock simultáneamente, ambos
   determinar que hay suficiente stock, ambos descontarlo y resultar en
   stock negativo (race condition crítica)."""

# --- MODIFICACIÓN 2: Diccionario de contadores por cliente ---
contadores_por_cliente = {}
"""dict[str, int]: NUEVO EN MOD-02 — Diccionario que almacena cuántos pedidos
   ha enviado cada cliente identificado por su nombre.

   Estructura: {"NombreCliente": <número_de_pedidos_aceptados>}

   Ejemplo tras tres pedidos de "Alice" y uno de "Bob":
       {"Alice": 3, "Bob": 1}

   CICLO DE VIDA:
   - Se inicializa vacío al arrancar el servidor.
   - Cuando llega el primer pedido de un cliente, se crea la entrada con 0.
   - Cada vez que se acepta un pedido, el contador se incrementa en 1.
   - Si el contador ya alcanzó MAX_PEDIDOS_POR_CLIENTE, el pedido se rechaza
     y el contador NO se incrementa.
   - El diccionario persiste mientras el servidor está activo (no se resetea
     por cliente entre conexiones).
"""

# --- MODIFICACIÓN 2: Lock para el diccionario de contadores ---
lock_contadores = threading.Lock()
"""threading.Lock: NUEVO EN MOD-02 — Mutex dedicado exclusivamente a proteger
   el acceso a contadores_por_cliente.

   ¿POR QUÉ UN LOCK SEPARADO Y NO USAR lock_stock?
   ------------------------------------------------
   Usar el mismo lock para stock Y contadores crearía un lock de granularidad
   gruesa: cualquier hilo que quiera leer/escribir contadores bloquearía
   también el acceso al stock, aunque no haya ninguna relación entre ambas
   operaciones. Esto reduce el paralelismo innecesariamente.

   Con lock_contadores separado, el acceso al stock y el acceso a contadores
   pueden ocurrir concurrentemente entre hilos distintos, mejorando el
   rendimiento.

   SECCIÓN CRÍTICA PROTEGIDA:
   --------------------------
   with lock_contadores:
       # 1. Leer el contador actual del cliente
       # 2. Comparar con MAX_PEDIDOS_POR_CLIENTE
       # 3. Incrementar el contador si se acepta el pedido

   Las tres operaciones deben ser atómicas: si entre la lectura y el
   incremento otro hilo modificara el contador, podríamos aceptar más pedidos
   del límite permitido.
"""

# --- Semáforo para la cola ---
semaforo_cola = threading.Semaphore(CAPACIDAD_MAXIMA_COLA)
"""threading.Semaphore(N): Contador atómico inicializado en CAPACIDAD_MAXIMA_COLA.
   - acquire(): decrementa el contador. Si llega a 0, bloquea (o rechaza si
     se usa timeout=0).
   - release(): incrementa el contador, señalando que un slot quedó libre.
   Garantiza que la cola nunca tenga más de CAPACIDAD_MAXIMA_COLA pedidos."""

# --- Barrera de procesamiento ---
barrera_procesamiento = threading.Barrier(PEDIDOS_MINIMOS_PARA_BARRERA)
"""threading.Barrier(N): Punto de sincronización donde N hilos se esperan
   mutuamente antes de continuar. Aquí la barrera representa el umbral mínimo
   de pedidos antes de iniciar el procesamiento en batch."""

# --- Evento de apagado ---
evento_apagado = threading.Event()
"""threading.Event: Flag booleano thread-safe. Cuando se llama a set(), todos
   los hilos que evalúan is_set() lo ven como True y pueden terminar
   graciosamente. Evita el uso de variables globales con condiciones de carrera."""

# =============================================================================
# FUNCIÓN: procesar_pedidos
# =============================================================================

def procesar_pedidos(id_procesador: int) -> None:
    """Hilo procesador que consume pedidos de cola_pedidos y actualiza el stock.

    Esta función es ejecutada por NUM_PROCESADORES hilos en paralelo.
    Cada hilo corre un bucle infinito extrayendo pedidos de la cola
    thread-safe y procesándolos.

    SINCRONIZACIÓN INTERNA:
    -----------------------
    - cola_pedidos.get(timeout=1): espera hasta 1 segundo por un pedido.
      Si no hay pedido en ese tiempo, verifica si debe apagarse.
    - lock_stock: protege la modificación de stock_productos.
    - semaforo_cola.release(): libera un slot cuando el pedido termina de
      procesarse, permitiendo aceptar nuevos pedidos en la cola.

    Parameters
    ----------
    id_procesador : int
        Identificador numérico del hilo procesador (1, 2, 3, ...).
        Se usa solo para los mensajes de log.
    """
    print(f"[PROCESADOR-{id_procesador}] Hilo procesador iniciado, esperando pedidos...")

    # -------------------------------------------------------------------------
    # BARRERA: El procesador espera en este punto hasta que todos los
    # PEDIDOS_MINIMOS_PARA_BARRERA hilos (incluyendo el hilo principal que
    # actúa como cliente) hayan llegado a wait(). En la práctica, esto
    # sincroniza el inicio de los procesadores con la llegada de los primeros
    # pedidos. Si la barrera falla por BrokenBarrierError (p.ej. timeout),
    # el procesador continúa de todas formas para no quedar bloqueado.
    # -------------------------------------------------------------------------
    try:
        print(f"[PROCESADOR-{id_procesador}] Esperando en la barrera de sincronización...")
        barrera_procesamiento.wait()
        print(f"[PROCESADOR-{id_procesador}] Barrera cruzada, comenzando procesamiento.")
    except threading.BrokenBarrierError:
        # La barrera puede romperse si el evento de apagado se activa antes
        # de que todos los hilos la alcancen.
        print(f"[PROCESADOR-{id_procesador}] Barrera rota (posible apagado anticipado). Continuando.")

    # -------------------------------------------------------------------------
    # BUCLE PRINCIPAL DEL PROCESADOR
    # Continúa mientras el evento de apagado NO esté activo, O mientras
    # queden pedidos en la cola (para no perder pedidos al apagar).
    # -------------------------------------------------------------------------
    while not evento_apagado.is_set() or not cola_pedidos.empty():
        try:
            # Extraer un pedido de la cola con timeout de 1 segundo.
            # Si la cola está vacía después del timeout, se vuelve al inicio
            # del while para re-evaluar la condición de apagado.
            pedido = cola_pedidos.get(timeout=1)
        except queue.Empty:
            # Cola vacía: ningún pedido disponible en este momento.
            # Continuar el bucle para verificar si hay que apagarse.
            continue

        # Extraer datos del pedido (el pedido es un dict Python).
        cliente  = pedido.get("cliente",  "Desconocido")
        producto = pedido.get("producto", "Desconocido")
        cantidad = pedido.get("cantidad", 0)

        print(f"[PROCESADOR-{id_procesador}] Procesando pedido de '{cliente}': "
              f"{cantidad}x '{producto}'")

        # Simular tiempo de procesamiento (p.ej. consulta a base de datos,
        # preparación del envío, etc.)
        time.sleep(1.5)

        # -----------------------------------------------------------------
        # SECCIÓN CRÍTICA: Modificar el stock de productos.
        # lock_stock garantiza que solo un procesador a la vez modifique
        # stock_productos, evitando race conditions.
        # -----------------------------------------------------------------
        with lock_stock:
            stock_actual = stock_productos.get(producto, 0)
            if stock_actual >= cantidad:
                # Hay stock suficiente: descontar unidades.
                stock_productos[producto] -= cantidad
                resultado = "PROCESADO"
                detalle   = f"Stock restante de '{producto}': {stock_productos[producto]}"
            else:
                # Stock insuficiente: el pedido no puede completarse.
                # Nota: este caso ocurre durante el procesamiento (el stock
                # pudo agotarse entre la aceptación y el procesamiento).
                resultado = "FALLIDO"
                detalle   = (f"Stock insuficiente al procesar. "
                             f"Disponible: {stock_actual}, solicitado: {cantidad}")

        print(f"[PROCESADOR-{id_procesador}] Pedido de '{cliente}' → {resultado}. {detalle}")

        # Indicar a la cola que la tarea fue completada (para join() si se usa).
        cola_pedidos.task_done()

        # Liberar un slot del semáforo: ahora el servidor puede aceptar otro
        # pedido en la cola sin rechazarlo por capacidad.
        semaforo_cola.release()

    print(f"[PROCESADOR-{id_procesador}] Hilo procesador terminando.")


# =============================================================================
# FUNCIÓN: manejar_cliente
# =============================================================================

def manejar_cliente(conn: socket.socket, addr: tuple) -> None:
    """Atiende la sesión completa de un cliente en un hilo dedicado.

    Esta función es ejecutada por un hilo separado para cada cliente
    que se conecta, permitiendo atención concurrente de múltiples clientes.

    FLUJO DE ATENCIÓN:
    ------------------
    1. Recibir los datos del cliente (JSON codificado en UTF-8).
    2. Decodificar y parsear el JSON.
    3. [MOD-02] Consultar el contador de pedidos del cliente.
    4. [MOD-02] Si el contador >= MAX_PEDIDOS_POR_CLIENTE → rechazar.
    5. Intentar adquirir un slot del semáforo (cola no llena).
    6. Si no hay slot disponible → rechazar por cola llena.
    7. Verificar stock del producto solicitado.
    8. Si stock insuficiente → rechazar y liberar slot.
    9. [MOD-02] Incrementar el contador del cliente.
    10. Encolar el pedido → confirmar al cliente.
    11. Cerrar la conexión.

    MODIFICACIÓN 2 — DETALLES DE IMPLEMENTACIÓN:
    ---------------------------------------------
    La verificación del límite y el incremento del contador se realizan
    DENTRO del mismo bloque with lock_contadores para garantizar atomicidad.
    Si se hicieran en pasos separados (leer → decidir → incrementar), dos
    hilos podrían pasar la verificación simultáneamente antes de que ninguno
    incrementara el contador, aceptando más pedidos del límite.

    Parameters
    ----------
    conn : socket.socket
        Socket de la conexión establecida con el cliente.
    addr : tuple
        Tupla (ip, puerto) de la dirección del cliente.
    """
    print(f"\n[SERVIDOR] Nueva conexión aceptada desde {addr}")

    try:
        # =====================================================================
        # PASO 1: Recibir datos del cliente
        # =====================================================================
        datos_raw = conn.recv(BUFFER_SIZE)
        if not datos_raw:
            # El cliente cerró la conexión sin enviar datos.
            print(f"[SERVIDOR] {addr} cerró la conexión sin enviar datos.")
            return

        # =====================================================================
        # PASO 2: Decodificar y parsear el JSON
        # =====================================================================
        try:
            datos_texto = datos_raw.decode(ENCODING)
            pedido      = json.loads(datos_texto)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            # Datos corruptos o protocolo incorrecto: rechazar con error.
            print(f"[SERVIDOR] Error al decodificar datos de {addr}: {e}")
            error_resp = {"tipo": "error",
                          "mensaje": "Formato de mensaje inválido",
                          "estado": "rechazado"}
            conn.sendall(json.dumps(error_resp).encode(ENCODING))
            return

        # Extraer campos del pedido recibido.
        tipo_msg  = pedido.get("tipo",     "")
        cliente   = pedido.get("cliente",  "Desconocido")
        producto  = pedido.get("producto", "")
        cantidad  = pedido.get("cantidad", 0)

        print(f"[SERVIDOR] Pedido recibido de '{cliente}' en {addr}: "
              f"{cantidad}x '{producto}'")

        # Validar que sea un pedido (tipo correcto).
        if tipo_msg != "pedido":
            error_resp = {"tipo": "error",
                          "mensaje": f"Tipo de mensaje no reconocido: '{tipo_msg}'",
                          "estado": "rechazado"}
            conn.sendall(json.dumps(error_resp).encode(ENCODING))
            return

        # =====================================================================
        # PASO 3 y 4 — MODIFICACIÓN 2: Verificar límite de pedidos por cliente
        # =====================================================================
        # Se adquiere lock_contadores para que la lectura y la decisión de
        # aceptar/rechazar sean atómicas respecto a otros hilos que atiendan
        # al mismo cliente simultáneamente.
        #
        # ESCENARIO DE RACE CONDITION SIN LOCK:
        # - Hilo A lee contador["Alice"] = 2 (límite es 3) → decide aceptar.
        # - Hilo B lee contador["Alice"] = 2 (límite es 3) → decide aceptar.
        # - Hilo A incrementa → contador["Alice"] = 3 ✓
        # - Hilo B incrementa → contador["Alice"] = 4 ✗ (superó el límite)
        # Con lock_contadores, este escenario es imposible.
        # =====================================================================
        with lock_contadores:
            # Obtener el contador actual del cliente (0 si es la primera vez).
            contador_actual = contadores_por_cliente.get(cliente, 0)

            print(f"[SERVIDOR] Cliente '{cliente}' lleva {contador_actual} "
                  f"pedido(s) aceptado(s) de un máximo de {MAX_PEDIDOS_POR_CLIENTE}.")

            if contador_actual >= MAX_PEDIDOS_POR_CLIENTE:
                # ─────────────────────────────────────────────────────────────
                # RECHAZO POR LÍMITE DE PEDIDOS
                # ─────────────────────────────────────────────────────────────
                # El cliente ya agotó su cuota de pedidos. Se envía el mensaje
                # de error estándar definido en la especificación y se retorna
                # SIN encolar nada y SIN tocar el semáforo ni el stock.
                #
                # Nótese que el lock_contadores se libera automáticamente al
                # salir del bloque with, aunque hayamos retornado desde dentro
                # del with (el finally del context manager lo garantiza).
                # ─────────────────────────────────────────────────────────────
                print(f"[SERVIDOR] ⚠️  LÍMITE ALCANZADO para '{cliente}'. "
                      f"Pedido de '{producto}' RECHAZADO.")

                error_resp = {
                    "tipo":    "error",
                    "mensaje": "Límite de pedidos alcanzado",
                    "estado":  "rechazado"
                }
                conn.sendall(json.dumps(error_resp).encode(ENCODING))
                return  # El with lock_contadores se libera aquí antes de retornar.

            # Si llegamos aquí, el cliente aún tiene cuota disponible.
            # NO incrementamos el contador todavía: primero verificamos el
            # semáforo y el stock. Solo incrementamos si el pedido será
            # efectivamente aceptado (para no penalizar intentos fallidos
            # por cola llena o stock insuficiente en el contador).
            #
            # NOTA DE DISEÑO: Se podría incrementar aquí y decrementar si
            # falla, pero eso complica el flujo. La decisión de diseño es
            # incrementar solo en el momento de éxito real.

        # =====================================================================
        # PASO 5: Intentar reservar un slot en la cola (semáforo)
        # =====================================================================
        # acquire(blocking=False) devuelve False inmediatamente si el semáforo
        # está en 0 (cola llena), en lugar de bloquear el hilo.
        # =====================================================================
        slot_adquirido = semaforo_cola.acquire(blocking=False)
        if not slot_adquirido:
            print(f"[SERVIDOR] Cola llena. Pedido de '{cliente}' para "
                  f"'{producto}' RECHAZADO por capacidad.")
            error_resp = {"tipo":    "error",
                          "mensaje": "Servidor ocupado, intente más tarde",
                          "estado":  "rechazado"}
            conn.sendall(json.dumps(error_resp).encode(ENCODING))
            return

        # =====================================================================
        # PASO 6: Verificar disponibilidad de stock (lectura previa)
        # =====================================================================
        # Se verifica el stock ANTES de encolar para evitar encolar pedidos
        # que sabemos que van a fallar. Sin embargo, el stock puede cambiar
        # entre esta verificación y el procesamiento real (TOCTOU), por eso
        # el procesador también verifica el stock al procesar.
        # =====================================================================
        with lock_stock:
            stock_disponible = stock_productos.get(producto, -1)

        if stock_disponible < 0:
            # El producto no existe en el catálogo.
            print(f"[SERVIDOR] Producto '{producto}' no existe. Pedido de "
                  f"'{cliente}' RECHAZADO.")
            semaforo_cola.release()  # Devolver el slot adquirido.
            error_resp = {"tipo":    "error",
                          "mensaje": f"Producto '{producto}' no encontrado",
                          "estado":  "rechazado"}
            conn.sendall(json.dumps(error_resp).encode(ENCODING))
            return

        if stock_disponible < cantidad:
            # Stock insuficiente en este momento.
            print(f"[SERVIDOR] Stock insuficiente de '{producto}'. "
                  f"Disponible: {stock_disponible}, solicitado: {cantidad}. "
                  f"Pedido de '{cliente}' RECHAZADO.")
            semaforo_cola.release()  # Devolver el slot adquirido.
            error_resp = {"tipo":    "error",
                          "mensaje": "Stock insuficiente",
                          "estado":  "rechazado"}
            conn.sendall(json.dumps(error_resp).encode(ENCODING))
            return

        # =====================================================================
        # PASO 9 — MODIFICACIÓN 2: Incrementar el contador del cliente
        # El pedido pasó todas las validaciones → se acepta definitivamente.
        # Ahora sí incrementamos el contador dentro de lock_contadores.
        # =====================================================================
        with lock_contadores:
            # Volver a leer el valor actual por si otro hilo lo modificó
            # mientras no teníamos el lock (entre el paso 3 y este punto).
            contadores_por_cliente[cliente] = contadores_por_cliente.get(cliente, 0) + 1
            nuevo_contador = contadores_por_cliente[cliente]

        print(f"[SERVIDOR] ✅ Contador actualizado para '{cliente}': "
              f"{nuevo_contador}/{MAX_PEDIDOS_POR_CLIENTE} pedidos aceptados.")

        # =====================================================================
        # PASO 10: Encolar el pedido y confirmar al cliente
        # =====================================================================
        cola_pedidos.put(pedido)

        print(f"[SERVIDOR] Pedido de '{cliente}' para {cantidad}x '{producto}' "
              f"encolado correctamente. Pedidos en cola: ~{cola_pedidos.qsize()}")

        confirmacion = {
            "tipo":    "confirmacion",
            "estado":  "aceptado",
            "mensaje": "Pedido encolado correctamente"
        }
        conn.sendall(json.dumps(confirmacion).encode(ENCODING))

    except ConnectionResetError:
        # El cliente cerró la conexión abruptamente.
        print(f"[SERVIDOR] {addr} reseteó la conexión abruptamente.")
    except OSError as e:
        print(f"[SERVIDOR] Error de socket con {addr}: {e}")
    finally:
        # Siempre cerrar el socket del cliente al terminar la función,
        # independientemente de si hubo excepción o no.
        conn.close()
        print(f"[SERVIDOR] Conexión con {addr} cerrada.")


# =============================================================================
# FUNCIÓN: iniciar_servidor
# =============================================================================

def iniciar_servidor() -> None:
    """Inicializa y ejecuta el servidor TCP concurrente.

    SECUENCIA DE ARRANQUE:
    ----------------------
    1. Crear el socket TCP (AF_INET = IPv4, SOCK_STREAM = TCP).
    2. Configurar SO_REUSEADDR para reutilizar el puerto inmediatamente
       después de un reinicio (evita el error "Address already in use").
    3. Enlazar el socket a (HOST, PORT).
    4. Poner el socket en modo escucha con backlog MAX_CLIENTES.
    5. Lanzar NUM_PROCESADORES hilos daemon procesadores.
    6. Bucle de aceptación de conexiones: por cada cliente, lanzar un hilo.

    HILOS DAEMON vs NO-DAEMON:
    --------------------------
    Los hilos procesadores son daemon=True, lo que significa que Python los
    terminará automáticamente cuando el hilo principal (main thread) termine.
    Los hilos de atención a clientes son daemon=False por defecto, pero como
    son de corta duración (una sesión de cliente) esto no es un problema.
    """
    # =========================================================================
    # CREAR Y CONFIGURAR EL SOCKET SERVIDOR
    # =========================================================================
    servidor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # SO_REUSEADDR: permite reutilizar el puerto incluso si está en estado
    # TIME_WAIT (sucede en los ~60s tras un cierre de conexión TCP).
    servidor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor_socket.bind((HOST, PORT))
    servidor_socket.listen(MAX_CLIENTES)

    print("=" * 65)
    print("   SERVIDOR TCP — MOD-02: LÍMITE MÁXIMO DE PEDIDOS POR CLIENTE")
    print("=" * 65)
    print(f"   Escuchando en:          {HOST}:{PORT}")
    print(f"   Capacidad máxima cola:  {CAPACIDAD_MAXIMA_COLA} pedidos")
    print(f"   Hilos procesadores:     {NUM_PROCESADORES}")
    print(f"   Pedidos mín. barrera:   {PEDIDOS_MINIMOS_PARA_BARRERA}")
    print(f"   Máx. clientes activos:  {MAX_CLIENTES}")
    print(f"   [MOD-02] Máx. pedidos por cliente: {MAX_PEDIDOS_POR_CLIENTE}")
    print("=" * 65)
    print()

    # =========================================================================
    # LANZAR HILOS PROCESADORES
    # Los hilos procesadores se arrancan antes del bucle de aceptación para
    # que estén listos cuando lleguen los primeros pedidos.
    # =========================================================================
    hilos_procesadores = []
    for i in range(1, NUM_PROCESADORES + 1):
        hilo = threading.Thread(
            target=procesar_pedidos,
            args=(i,),
            name=f"Procesador-{i}",
            daemon=True  # Termina automáticamente cuando el main thread termine.
        )
        hilo.start()
        hilos_procesadores.append(hilo)
        print(f"[SERVIDOR] Hilo procesador {i} lanzado ({hilo.name}).")

    print(f"[SERVIDOR] {NUM_PROCESADORES} procesadores activos. Esperando clientes...\n")

    # =========================================================================
    # BUCLE PRINCIPAL: ACEPTAR CONEXIONES
    # =========================================================================
    try:
        while True:
            try:
                # accept() bloquea hasta que un cliente se conecta.
                # Devuelve (conn, addr): conn = socket de la conexión,
                # addr = (ip, puerto) del cliente.
                conn, addr = servidor_socket.accept()

                # Lanzar un hilo dedicado para atender a este cliente.
                # Cada hilo ejecuta manejar_cliente(conn, addr).
                # daemon=False: si el servidor intenta cerrarse, espera a que
                # estos hilos terminen (para no cortar conexiones activas).
                hilo_cliente = threading.Thread(
                    target=manejar_cliente,
                    args=(conn, addr),
                    name=f"Cliente-{addr[1]}",
                    daemon=False
                )
                hilo_cliente.start()
                print(f"[SERVIDOR] Hilo lanzado para {addr} ({hilo_cliente.name}).")

            except OSError:
                # El socket servidor fue cerrado (p.ej. por KeyboardInterrupt).
                break

    except KeyboardInterrupt:
        print("\n[SERVIDOR] Señal de interrupción recibida (Ctrl+C).")
    finally:
        # =====================================================================
        # APAGADO GRACIOSO
        # =====================================================================
        print("[SERVIDOR] Iniciando apagado gracioso...")

        # Señalar a los procesadores que deben terminar.
        evento_apagado.set()

        # Cerrar el socket servidor para que accept() no bloquee.
        servidor_socket.close()

        # Esperar a que los procesadores terminen de procesar pedidos pendientes.
        for hilo in hilos_procesadores:
            hilo.join(timeout=10)  # Máximo 10 segundos de espera por hilo.

        # Mostrar estado final de contadores (MODIFICACIÓN 2).
        print("\n[SERVIDOR] === RESUMEN FINAL DE PEDIDOS POR CLIENTE ===")
        if contadores_por_cliente:
            for nombre, total in contadores_por_cliente.items():
                print(f"   '{nombre}': {total} pedido(s) aceptado(s)")
        else:
            print("   No se registraron pedidos.")

        print("[SERVIDOR] Servidor detenido correctamente.")


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================

if __name__ == "__main__":
    """Ejecutar el servidor cuando el script se lanza directamente.

    La guardia __name__ == "__main__" evita que iniciar_servidor() se llame
    si este módulo es importado por otro script (p.ej. en tests).
    """
    iniciar_servidor()
