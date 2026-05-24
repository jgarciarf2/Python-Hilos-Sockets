"""
==============================================================================
MODIFICACIÓN 9: Número DINÁMICO de procesadores según la carga
==============================================================================

Archivo : servidor.py
Carpeta : mod09_procesadores_dinamicos/
Autores : Programación Concurrente — Ejemplo de hilos y sockets con Python

------------------------------------------------------------------------------
DESCRIPCIÓN GENERAL
------------------------------------------------------------------------------
Este servidor TCP multicliente utiliza una arquitectura de cola de pedidos con
un grupo de hilos *procesadores* cuyo tamaño varía dinámicamente en tiempo de
ejecución según la carga actual de la cola.

La idea central es:
  • Un hilo *productor* acepta conexiones de clientes y deposita sus pedidos
    en una cola compartida.
  • Un conjunto variable de hilos *procesadores* consumen pedidos de esa cola,
    actualizan el stock y devuelven la respuesta al cliente.
  • Un hilo *monitor de carga* examina periódicamente el tamaño de la cola y
    decide si es necesario crear nuevos procesadores o retirar los existentes.

------------------------------------------------------------------------------
CAMBIOS RESPECTO A LA VERSIÓN BASE (comparativa exhaustiva)
------------------------------------------------------------------------------

1. NUM_PROCESADORES_INICIAL = 2
   - Antes: NUM_PROCESADORES = 3 (fijo, nunca cambiaba).
   - Ahora:  Se arranca con 2 procesadores. Este número PUEDE crecer hasta
             NUM_PROCESADORES_MAX = 6 o bajar hasta NUM_PROCESADORES_MIN = 1.

2. NUM_PROCESADORES_MAX = 6
   - Nueva constante. Límite superior de procesadores simultáneos.
   - Evita que el sistema cree hilos sin control bajo picos de carga.

3. NUM_PROCESADORES_MIN = 1
   - Nueva constante. Límite inferior de procesadores simultáneos.
   - Garantiza siempre al menos un procesador activo aunque la cola esté vacía.

4. UMBRAL_ESCALAR = 5
   - Nueva constante. Si la cola tiene MÁS de 5 pedidos pendientes, se
     considera que los procesadores actuales no dan abasto → escalar.

5. UMBRAL_REDUCIR = 2
   - Nueva constante. Si la cola tiene MENOS de 2 pedidos pendientes, los
     procesadores actuales están ociosos → reducir.

6. procesadores_activos (int) + lock_procesadores (threading.Lock)
   - Nueva variable compartida que lleva la cuenta de cuántos procesadores
     están en ejecución en un instante dado.
   - Cualquier lectura o escritura se realiza dentro de `with lock_procesadores`
     para evitar condiciones de carrera.

7. lista_eventos_terminar (list[threading.Event])
   - Nueva lista. Cada procesador tiene su propio threading.Event que, cuando
     se activa (.set()), le indica a ese hilo que debe finalizar su bucle de
     trabajo de forma controlada (sin matarlo abruptamente).

8. hilo monitor_carga
   - Nuevo hilo demonio que se ejecuta cada 3 segundos (INTERVALO_MONITOR).
   - Lógica:
       * Si len(cola_pedidos) > UMBRAL_ESCALAR
         y procesadores_activos < NUM_PROCESADORES_MAX:
             → crear nuevo procesador, incrementar procesadores_activos.
       * Si len(cola_pedidos) < UMBRAL_REDUCIR
         y procesadores_activos > NUM_PROCESADORES_MIN:
             → marcar el último Event como .set() para que ese procesador
               termine al finalizar su pedido actual.
   - Imprime: 'Monitor: cola tiene X pedidos, ajustando a Y procesadores'

9. Barrera adaptada al número INICIAL de procesadores
   - Antes: threading.Barrier(NUM_PROCESADORES) con valor estático.
   - Ahora:  threading.Barrier(NUM_PROCESADORES_INICIAL) porque la barrera
             se crea al inicio con los procesadores que existen en ese momento.
             Los procesadores creados dinámicamente NO participan en la barrera
             (su rol es solo aliviar la cola, no sincronizar el lote).

------------------------------------------------------------------------------
PRIMITIVAS DE SINCRONIZACIÓN UTILIZADAS
------------------------------------------------------------------------------
  • threading.Lock          → proteger stock_productos y procesadores_activos
  • threading.Semaphore     → controlar la capacidad máxima de la cola
  • threading.Barrier       → sincronizar los procesadores INICIALES cada N pedidos
  • threading.Event         → señalizar a un procesador que debe terminar
                              + evento global para apagado del servidor
  • queue.Queue             → cola FIFO thread-safe de pedidos

------------------------------------------------------------------------------
PROTOCOLO DE COMUNICACIÓN (JSON sobre TCP)
------------------------------------------------------------------------------
  Petición  : {"producto": "<nombre>", "cantidad": <int>}
  Respuesta : {"estado": "ok"|"error"|"sin_stock", "mensaje": "<texto>",
               "stock_restante": <int>|null}

==============================================================================
"""

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTACIONES
# ─────────────────────────────────────────────────────────────────────────────
import socket       # Comunicación TCP cliente-servidor
import threading    # Hilos, Lock, Semaphore, Barrier, Event
import queue        # Cola FIFO thread-safe para los pedidos
import json         # Serialización/deserialización del protocolo
import time         # time.sleep() en el monitor de carga
import logging      # Registro de eventos con nivel y timestamp

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN DEL LOGGER
# ─────────────────────────────────────────────────────────────────────────────
# Usamos el módulo logging en lugar de print() para tener control de niveles
# (DEBUG, INFO, WARNING, ERROR) y formato uniforme con timestamps.
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(threadName)-22s] %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("servidor")

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTES DE RED
# ─────────────────────────────────────────────────────────────────────────────
HOST        = "127.0.0.1"   # Dirección de escucha (loopback → solo local)
PORT        = 65000          # Puerto TCP elegido (>1024 no requiere root)
ENCODING    = "utf-8"        # Codificación de bytes ↔ cadenas
BUFFER_SIZE = 4096           # Bytes máximos leídos por recv() en una llamada

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTES DE CONCURRENCIA — ORIGINALES
# ─────────────────────────────────────────────────────────────────────────────
CAPACIDAD_MAXIMA_COLA       = 10   # Máximo de pedidos pendientes en cola
MAX_CLIENTES                = 5    # Máximo de clientes conectados simultáneos
PEDIDOS_MINIMOS_PARA_BARRERA = 5   # Cada cuántos pedidos se activa la barrera

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTES DE CONCURRENCIA — NUEVAS (Modificación 9)
# ─────────────────────────────────────────────────────────────────────────────

# Número de procesadores con los que arranca el servidor.
# Reemplaza la antigua constante fija NUM_PROCESADORES = 3.
NUM_PROCESADORES_INICIAL = 2

# Techo: no se puede crear más de este número de procesadores aunque la cola
# esté desbordada. Protege contra creación ilimitada de hilos bajo carga
# sostenida (cada hilo consume memoria de pila del SO).
NUM_PROCESADORES_MAX = 6

# Suelo: aunque la cola esté completamente vacía siempre habrá al menos un
# procesador listo para atender el siguiente pedido sin latencia extra.
NUM_PROCESADORES_MIN = 1

# Umbral de escalado: si la cola supera este número de pedidos pendientes se
# crea un nuevo procesador para aliviar la presión.
UMBRAL_ESCALAR = 5

# Umbral de reducción: si la cola tiene menos pedidos que este número los
# procesadores están mayormente ociosos → eliminar uno para liberar recursos.
UMBRAL_REDUCIR = 2

# Frecuencia de revisión del monitor de carga (segundos).
INTERVALO_MONITOR = 3

# ─────────────────────────────────────────────────────────────────────────────
# INVENTARIO COMPARTIDO
# ─────────────────────────────────────────────────────────────────────────────
# Diccionario mutable: múltiples hilos pueden leer y escribir.
# Toda modificación debe hacerse dentro de `with lock_stock`.
stock_productos = {
    "Laptop"    : 10,
    "Mouse"     : 25,
    "Teclado"   : 20,
    "Monitor"   :  8,
    "Auriculares": 15,
    "USB"       : 30,
    "Cargador"  : 18,
    "Webcam"    : 12,
}

# ─────────────────────────────────────────────────────────────────────────────
# PRIMITIVAS DE SINCRONIZACIÓN
# ─────────────────────────────────────────────────────────────────────────────

# Lock para stock_productos
# Garantiza exclusión mutua: solo un hilo a la vez puede leer/modificar stock.
lock_stock = threading.Lock()

# Semáforo de capacidad de la cola
# El productor llama acquire() antes de encolar (bloqueándose si está llena)
# y el procesador llama release() al desencolar (liberando un hueco).
sem_capacidad_cola = threading.Semaphore(CAPACIDAD_MAXIMA_COLA)

# Barrera de sincronización entre los procesadores INICIALES
# Se activa cada PEDIDOS_MINIMOS_PARA_BARRERA pedidos procesados; todos los
# procesadores iniciales deben llegar al punto de encuentro antes de continuar.
# CAMBIO MOD9: el número de partes es NUM_PROCESADORES_INICIAL (antes era
# NUM_PROCESADORES). Los procesadores dinámicos NO entran en esta barrera.
barrera_procesadores = threading.Barrier(NUM_PROCESADORES_INICIAL)

# Evento global de apagado del servidor
# Cuando se activa (.set()), todos los hilos deben terminar su trabajo y salir.
evento_apagado = threading.Event()

# ─────────────────────────────────────────────────────────────────────────────
# COLA DE PEDIDOS
# ─────────────────────────────────────────────────────────────────────────────
# queue.Queue es intrínsecamente thread-safe (usa su propio lock interno).
# Almacena tuplas: (datos_pedido_dict, socket_cliente, addr_cliente)
cola_pedidos = queue.Queue()

# ─────────────────────────────────────────────────────────────────────────────
# ESTADO DINÁMICO DE PROCESADORES (Modificación 9)
# ─────────────────────────────────────────────────────────────────────────────

# Contador de procesadores vivos en un instante dado.
# DEBE leerse/escribirse exclusivamente dentro de `with lock_procesadores`.
procesadores_activos = 0

# Lock dedicado para proteger procesadores_activos.
# Lo separamos de lock_stock para evitar contención innecesaria.
lock_procesadores = threading.Lock()

# Lista de eventos de terminación, uno por procesador.
# lista_eventos_terminar[i] es el threading.Event del i-ésimo procesador.
# Cuando el monitor llama a lista_eventos_terminar[i].set(), el procesador i
# termina su bucle de trabajo de manera ordenada.
lista_eventos_terminar = []

# Lock para proteger la lista_eventos_terminar (append / pop / indexado).
lock_lista_eventos = threading.Lock()

# Contador global de pedidos procesados (para la lógica de barrera).
pedidos_procesados_total = 0
lock_contador_pedidos    = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# FUNCIÓN AUXILIAR: enviar respuesta JSON
# ─────────────────────────────────────────────────────────────────────────────
def enviar_respuesta(conn: socket.socket, datos: dict) -> None:
    """
    Serializa `datos` como JSON y lo envía por el socket `conn`.

    Parámetros
    ----------
    conn  : socket.socket
        Socket conectado al cliente destinatario.
    datos : dict
        Diccionario con los campos de la respuesta.

    Excepciones
    -----------
    Captura OSError (socket cerrado) e imprime advertencia; no propaga.
    """
    try:
        mensaje = json.dumps(datos, ensure_ascii=False)
        conn.sendall(mensaje.encode(ENCODING))
    except OSError as e:
        log.warning("No se pudo enviar respuesta: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# FUNCIÓN: procesar_pedido (lógica de negocio)
# ─────────────────────────────────────────────────────────────────────────────
def procesar_pedido(pedido: dict, conn: socket.socket) -> None:
    """
    Aplica la lógica de negocio al pedido y envía la respuesta al cliente.

    Esta función es llamada dentro de hilo_procesador. Toda operación sobre
    stock_productos se realiza dentro del lock_stock para garantizar
    consistencia bajo concurrencia.

    Parámetros
    ----------
    pedido : dict
        Diccionario con claves "producto" (str) y "cantidad" (int).
    conn   : socket.socket
        Socket del cliente que realizó la solicitud.

    Estados de respuesta posibles
    ------------------------------
    "ok"        → pedido atendido, stock actualizado.
    "sin_stock" → el producto existe pero no hay suficientes unidades.
    "error"     → producto desconocido o datos malformados.
    """
    producto  = pedido.get("producto", "")
    cantidad  = pedido.get("cantidad", 0)

    with lock_stock:
        # Verificar existencia del producto en el inventario
        if producto not in stock_productos:
            enviar_respuesta(conn, {
                "estado" : "error",
                "mensaje": f"Producto '{producto}' no encontrado en el catálogo.",
                "stock_restante": None,
            })
            return

        stock_actual = stock_productos[producto]

        if stock_actual < cantidad:
            # No hay suficiente stock para satisfacer el pedido
            enviar_respuesta(conn, {
                "estado" : "sin_stock",
                "mensaje": (f"Stock insuficiente para '{producto}'. "
                            f"Disponibles: {stock_actual}, solicitados: {cantidad}."),
                "stock_restante": stock_actual,
            })
            return

        # Descontar el stock y confirmar el pedido
        stock_productos[producto] -= cantidad
        stock_restante = stock_productos[producto]

    # Respuesta fuera del lock (el envío de bytes no necesita protección de stock)
    enviar_respuesta(conn, {
        "estado" : "ok",
        "mensaje": (f"Pedido de {cantidad}x '{producto}' confirmado. "
                    f"Stock restante: {stock_restante}."),
        "stock_restante": stock_restante,
    })
    log.info("Pedido confirmado: %dx %s (stock restante: %d)", cantidad, producto, stock_restante)


# ─────────────────────────────────────────────────────────────────────────────
# FUNCIÓN: crear_procesador  (Modificación 9 — función auxiliar nueva)
# ─────────────────────────────────────────────────────────────────────────────
def crear_procesador(es_inicial: bool = False) -> threading.Thread:
    """
    Crea, registra y lanza un nuevo hilo procesador.

    Esta función centraliza la creación de hilos para no repetir código en el
    arranque y en el monitor de carga.

    Parámetros
    ----------
    es_inicial : bool
        Si es True, el procesador participará en la barrera de sincronización.
        Los procesadores creados dinámicamente tienen es_inicial=False y no
        participan en la barrera (la barrera ya fue construida con el número
        inicial de partes y no se puede redimensionar en tiempo de ejecución).

    Retorna
    -------
    threading.Thread
        El hilo ya iniciado.
    """
    global procesadores_activos

    # Crear el evento de terminación exclusivo de este procesador.
    # Cuando el monitor llame a evento.set(), el procesador terminará su
    # iteración actual y saldrá del bucle while de forma limpia.
    evento_terminar = threading.Event()

    # Registrar el evento en la lista compartida ANTES de iniciar el hilo,
    # para evitar una ventana de tiempo en la que el monitor intente señalizar
    # un evento que todavía no está en la lista.
    with lock_lista_eventos:
        lista_eventos_terminar.append(evento_terminar)
        indice = len(lista_eventos_terminar) - 1  # Posición de este procesador

    # Incrementar el contador ANTES de iniciar el hilo (el hilo verá el valor
    # actualizado desde su primer ciclo).
    with lock_procesadores:
        procesadores_activos += 1
        numero = procesadores_activos  # Nombre descriptivo para el hilo

    # Nombre que aparecerá en el log (diferencia iniciales de dinámicos)
    tipo    = "Inicial" if es_inicial else "Dinamico"
    nombre  = f"Procesador-{tipo}-{numero}"

    hilo = threading.Thread(
        target=hilo_procesador,
        args=(evento_terminar, es_inicial),
        name=nombre,
        daemon=True,   # Muere automáticamente cuando el proceso principal termina
    )
    hilo.start()
    log.info("Nuevo procesador creado: %s (total activos: %d)", nombre, numero)
    return hilo


# ─────────────────────────────────────────────────────────────────────────────
# HILO: hilo_procesador
# ─────────────────────────────────────────────────────────────────────────────
def hilo_procesador(evento_terminar: threading.Event, participa_barrera: bool) -> None:
    """
    Hilo consumidor de la cola de pedidos.

    Bucle principal:
      1. Esperar a que haya un pedido en la cola (bloqueante con timeout).
      2. Si evento_terminar está activado y la cola está vacía → salir.
      3. Desencolar pedido, liberar hueco en semáforo, procesar, cerrar socket.
      4. Si participa_barrera y se completó un lote de N pedidos → sincronizar
         con los demás procesadores iniciales en la barrera.

    Parámetros
    ----------
    evento_terminar   : threading.Event
        Evento propio de este hilo. Cuando está activado, el hilo termina
        al finalizar el pedido actual (o inmediatamente si la cola está vacía).
    participa_barrera : bool
        True  → es un procesador inicial y usa barrera_procesadores.
        False → procesador dinámico, no usa la barrera.

    NOTA IMPORTANTE sobre la barrera
    ---------------------------------
    La barrera se construye con parties=NUM_PROCESADORES_INICIAL. Si un
    procesador dinámico intentara llamar a barrera_procesadores.wait(), el
    barrier nunca se satisfaría a menos que todos los parties (iniciales)
    también lleguen. Por eso los dinámicos tienen participa_barrera=False.
    """
    global pedidos_procesados_total, procesadores_activos

    log.debug("Procesador iniciado. Participa en barrera: %s", participa_barrera)

    while not evento_apagado.is_set():

        # ── Intentar obtener un pedido de la cola ──────────────────────────
        # Usamos timeout=1 para revisar periódicamente las condiciones de
        # salida sin bloquear el hilo indefinidamente.
        try:
            pedido, conn, addr = cola_pedidos.get(timeout=1)
        except queue.Empty:
            # Cola vacía: comprobar si debemos terminar
            if evento_terminar.is_set():
                log.debug("Evento de terminación activo y cola vacía → saliendo.")
                break
            # Si no, volver al inicio del bucle y esperar más pedidos
            continue

        # ── Procesar el pedido ─────────────────────────────────────────────
        log.debug("Atendiendo pedido de %s: %s", addr, pedido)
        try:
            procesar_pedido(pedido, conn)
        except Exception as e:
            log.error("Error inesperado al procesar pedido: %s", e)
            enviar_respuesta(conn, {
                "estado" : "error",
                "mensaje": "Error interno del servidor.",
                "stock_restante": None,
            })
        finally:
            # Cerrar la conexión con el cliente (sin importar si hubo error)
            conn.close()
            # Liberar el hueco en el semáforo para que el productor pueda
            # encolar el próximo pedido si estaba bloqueado.
            sem_capacidad_cola.release()

        # ── Actualizar contador de pedidos ─────────────────────────────────
        with lock_contador_pedidos:
            pedidos_procesados_total += 1
            total_local = pedidos_procesados_total

        # ── Punto de sincronización con la barrera (solo iniciales) ────────
        if participa_barrera and (total_local % PEDIDOS_MINIMOS_PARA_BARRERA == 0):
            log.info(
                "Barrera: %d pedidos completados, esperando a los demás procesadores iniciales…",
                total_local,
            )
            try:
                # Todos los hilos iniciales deben llegar aquí antes de continuar.
                # Si alguno está atascado procesando, los demás esperarán.
                barrera_procesadores.wait(timeout=10)
                log.info("Barrera liberada, reanudando procesamiento.")
            except threading.BrokenBarrierError:
                # La barrera fue rota (p. ej., por el apagado del servidor)
                log.warning("Barrera rota, continuando sin sincronización.")

        # ── Verificar si debemos terminar después de completar el pedido ───
        if evento_terminar.is_set():
            log.debug("Evento de terminación activo, saliendo tras completar pedido.")
            break

    # ── Limpieza al salir ──────────────────────────────────────────────────
    with lock_procesadores:
        procesadores_activos -= 1
        restantes = procesadores_activos
    log.info("Procesador finalizado. Procesadores activos restantes: %d", restantes)


# ─────────────────────────────────────────────────────────────────────────────
# HILO: monitor_carga  (Modificación 9 — hilo completamente nuevo)
# ─────────────────────────────────────────────────────────────────────────────
def monitor_carga() -> None:
    """
    Hilo monitor que ajusta dinámicamente el número de procesadores activos.

    Lógica de ejecución (cada INTERVALO_MONITOR segundos):
    ───────────────────────────────────────────────────────
    1. Leer el tamaño actual de la cola (cola_pedidos.qsize()).
       NOTA: qsize() no es atómicamente exacto, pero es suficientemente
       preciso para decisiones de escalado (no necesitamos precisión absoluta).

    2. Leer procesadores_activos (dentro de lock_procesadores).

    3. Si tamaño_cola > UMBRAL_ESCALAR y activos < NUM_PROCESADORES_MAX:
           → Escalar: llamar a crear_procesador(es_inicial=False).

    4. Si tamaño_cola < UMBRAL_REDUCIR y activos > NUM_PROCESADORES_MIN:
           → Reducir: activar el evento de terminación del último procesador
             registrado en lista_eventos_terminar.

    5. Imprimir mensaje informativo con el estado actual.

    El hilo termina cuando evento_apagado se activa.
    """
    log.info("Monitor de carga iniciado (intervalo: %ds).", INTERVALO_MONITOR)

    while not evento_apagado.is_set():
        # Dormir el intervalo configurado; revisar evento_apagado cada segundo
        # para responder rápidamente a un apagado del servidor.
        for _ in range(INTERVALO_MONITOR):
            if evento_apagado.is_set():
                break
            time.sleep(1)

        if evento_apagado.is_set():
            break

        # ── Leer estado actual ─────────────────────────────────────────────
        tamano_cola = cola_pedidos.qsize()

        with lock_procesadores:
            activos = procesadores_activos

        # ── Decisión de escalado o reducción ──────────────────────────────
        if tamano_cola > UMBRAL_ESCALAR and activos < NUM_PROCESADORES_MAX:
            # La cola tiene demasiados pedidos esperando → agregar un procesador
            print(
                f"Monitor: cola tiene {tamano_cola} pedidos, "
                f"ajustando a {activos + 1} procesadores"
            )
            log.info(
                "Monitor → ESCALAR: cola=%d > umbral=%d, activos=%d < max=%d",
                tamano_cola, UMBRAL_ESCALAR, activos, NUM_PROCESADORES_MAX,
            )
            # Crear un procesador dinámico (no participa en la barrera)
            crear_procesador(es_inicial=False)

        elif tamano_cola < UMBRAL_REDUCIR and activos > NUM_PROCESADORES_MIN:
            # La cola está casi vacía → terminar el procesador más nuevo
            print(
                f"Monitor: cola tiene {tamano_cola} pedidos, "
                f"ajustando a {activos - 1} procesadores"
            )
            log.info(
                "Monitor → REDUCIR: cola=%d < umbral=%d, activos=%d > min=%d",
                tamano_cola, UMBRAL_REDUCIR, activos, NUM_PROCESADORES_MIN,
            )
            # Señalizar al último procesador registrado para que termine
            with lock_lista_eventos:
                if lista_eventos_terminar:
                    # Elegir el último evento (procesador más recientemente creado)
                    evento = lista_eventos_terminar.pop()
                    evento.set()  # El procesador terminará tras su pedido actual
        else:
            # No se requiere ajuste, solo informar estado
            log.debug(
                "Monitor → sin cambios: cola=%d, activos=%d",
                tamano_cola, activos,
            )

    log.info("Monitor de carga finalizado.")


# ─────────────────────────────────────────────────────────────────────────────
# HILO: manejar_cliente (productor / receptor de pedidos)
# ─────────────────────────────────────────────────────────────────────────────
def manejar_cliente(conn: socket.socket, addr: tuple) -> None:
    """
    Hilo productor: recibe el pedido JSON del cliente y lo encola.

    Este hilo:
      1. Adquiere una ranura en el semáforo de capacidad (bloquea si cola llena).
      2. Lee y deserializa el JSON enviado por el cliente.
      3. Deposita la tupla (pedido, conn, addr) en la cola.

    Los parámetros conn y addr son los mismos que devuelve socket.accept().
    El socket se cierra más adelante por hilo_procesador, no aquí.

    Parámetros
    ----------
    conn : socket.socket
        Socket de la conexión establecida con el cliente.
    addr : tuple
        Dirección (ip, puerto) del cliente.
    """
    log.debug("Nueva conexión desde %s", addr)

    try:
        # ── Leer datos enviados por el cliente ────────────────────────────
        datos_raw = conn.recv(BUFFER_SIZE)
        if not datos_raw:
            log.warning("Cliente %s cerró la conexión sin enviar datos.", addr)
            conn.close()
            return

        # ── Deserializar el JSON ───────────────────────────────────────────
        try:
            pedido = json.loads(datos_raw.decode(ENCODING))
        except json.JSONDecodeError as e:
            log.error("JSON inválido desde %s: %s", addr, e)
            enviar_respuesta(conn, {
                "estado" : "error",
                "mensaje": "Formato de pedido inválido. Se esperaba JSON.",
                "stock_restante": None,
            })
            conn.close()
            return

        # ── Validar campos mínimos del pedido ─────────────────────────────
        if "producto" not in pedido or "cantidad" not in pedido:
            enviar_respuesta(conn, {
                "estado" : "error",
                "mensaje": "El pedido debe contener 'producto' y 'cantidad'.",
                "stock_restante": None,
            })
            conn.close()
            return

        # ── Adquirir ranura en semáforo (bloquea si cola llena) ──────────
        # Esto aplica contrapresión al cliente: si la cola está llena, el
        # cliente queda en espera hasta que un procesador libere un hueco.
        log.debug("Esperando hueco en cola para pedido de %s…", addr)
        sem_capacidad_cola.acquire()
        log.debug("Hueco adquirido, encolando pedido de %s.", addr)

        # ── Encolar el pedido ─────────────────────────────────────────────
        cola_pedidos.put((pedido, conn, addr))
        log.info("Pedido encolado: %s → %s", addr, pedido)

    except OSError as e:
        log.error("Error de red con cliente %s: %s", addr, e)
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# FUNCIÓN PRINCIPAL: iniciar_servidor
# ─────────────────────────────────────────────────────────────────────────────
def iniciar_servidor() -> None:
    """
    Punto de entrada del servidor.

    Secuencia de arranque:
    ──────────────────────
    1. Crear el socket TCP y ponerlo en escucha.
    2. Iniciar NUM_PROCESADORES_INICIAL procesadores (es_inicial=True).
    3. Iniciar el hilo monitor_carga.
    4. Aceptar conexiones en bucle infinito → delegar cada una a un hilo
       manejar_cliente (con límite MAX_CLIENTES vía semáforo).
    5. Al interrumpir con Ctrl+C, activar evento_apagado y romper la barrera
       para desbloquear cualquier procesador que esté esperando.
    """
    # ── Semáforo de clientes simultáneos ──────────────────────────────────
    # Limita cuántos hilos manejar_cliente pueden existir simultáneamente,
    # evitando consumo descontrolado de recursos bajo avalancha de conexiones.
    sem_clientes = threading.Semaphore(MAX_CLIENTES)

    # ── Socket del servidor ───────────────────────────────────────────────
    servidor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # SO_REUSEADDR permite reiniciar el servidor sin esperar TIME_WAIT del SO
    servidor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor_socket.bind((HOST, PORT))
    servidor_socket.listen(MAX_CLIENTES)
    # Timeout en accept() para poder revisar evento_apagado periódicamente
    servidor_socket.settimeout(1.0)

    log.info("Servidor escuchando en %s:%d", HOST, PORT)
    print(f"[SERVIDOR] Escuchando en {HOST}:{PORT}")
    print(f"[SERVIDOR] Iniciando con {NUM_PROCESADORES_INICIAL} procesadores "
          f"(rango dinámico: {NUM_PROCESADORES_MIN}–{NUM_PROCESADORES_MAX})")

    # ── Iniciar procesadores INICIALES ───────────────────────────────────
    # Estos participan en la barrera (es_inicial=True).
    hilos_procesadores = []
    for _ in range(NUM_PROCESADORES_INICIAL):
        hilo = crear_procesador(es_inicial=True)
        hilos_procesadores.append(hilo)

    # ── Iniciar monitor de carga (Modificación 9) ─────────────────────────
    hilo_monitor = threading.Thread(
        target=monitor_carga,
        name="Monitor-Carga",
        daemon=True,  # Terminará automáticamente con el proceso principal
    )
    hilo_monitor.start()
    log.info("Hilo monitor_carga iniciado.")

    # ── Bucle de aceptación de conexiones ─────────────────────────────────
    try:
        while not evento_apagado.is_set():
            try:
                conn, addr = servidor_socket.accept()
            except socket.timeout:
                # Timeout normal: revisar evento_apagado y reintentar
                continue

            # Intentar adquirir ranura de cliente (no bloquea: si llena → rechazar)
            if not sem_clientes.acquire(blocking=False):
                log.warning("Servidor lleno, rechazando cliente %s", addr)
                try:
                    enviar_respuesta(conn, {
                        "estado" : "error",
                        "mensaje": "Servidor ocupado. Inténtelo más tarde.",
                        "stock_restante": None,
                    })
                finally:
                    conn.close()
                continue

            # Delegar la conexión a un hilo productor
            hilo_cliente = threading.Thread(
                target=_manejar_cliente_con_semaforo,
                args=(conn, addr, sem_clientes),
                name=f"Cliente-{addr[1]}",
                daemon=True,
            )
            hilo_cliente.start()

    except KeyboardInterrupt:
        print("\n[SERVIDOR] Interrupción recibida (Ctrl+C). Apagando…")
        log.info("Apagando servidor por KeyboardInterrupt.")
    finally:
        # ── Secuencia de apagado ──────────────────────────────────────────
        evento_apagado.set()

        # Romper la barrera para desbloquear procesadores iniciales que estén
        # esperando en barrera_procesadores.wait().
        try:
            barrera_procesadores.abort()
        except Exception:
            pass

        servidor_socket.close()
        log.info("Socket del servidor cerrado.")
        print("[SERVIDOR] Servidor apagado.")


def _manejar_cliente_con_semaforo(
    conn: socket.socket, addr: tuple, sem: threading.Semaphore
) -> None:
    """
    Envuelve manejar_cliente garantizando que el semáforo se libere siempre.

    Sin esta función auxiliar, si manejar_cliente lanzara una excepción no
    capturada, la ranura del semáforo quedaría bloqueada permanentemente.

    Parámetros
    ----------
    conn : socket.socket   → Socket del cliente.
    addr : tuple           → Dirección del cliente.
    sem  : threading.Semaphore → Semáforo de clientes simultáneos a liberar.
    """
    try:
        manejar_cliente(conn, addr)
    finally:
        sem.release()


# ─────────────────────────────────────────────────────────────────────────────
# PUNTO DE ENTRADA
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    iniciar_servidor()
