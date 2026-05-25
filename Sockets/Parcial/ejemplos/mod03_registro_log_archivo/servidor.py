"""
================================================================================
ARCHIVO: servidor.py
MODIFICACIÓN 3: Guardar LOG en archivo de texto
================================================================================

DESCRIPCIÓN GENERAL:
    Servidor concurrente TCP que gestiona pedidos de productos de un almacén.
    Atiende múltiples clientes en paralelo usando hilos (threads), con control
    de acceso concurrente mediante Lock, Semaphore, Barrier y Event.

    MODIFICACIÓN 3 - NOVEDAD PRINCIPAL:
    ====================================
    En esta versión, TODOS los eventos del servidor se guardan en un archivo
    de texto llamado 'servidor_log.txt', además de mostrarse en la consola.
    Se utiliza el módulo estándar 'logging' de Python con dos handlers:
        1. FileHandler   → escribe al archivo 'servidor_log.txt'
        2. StreamHandler → imprime en la consola (stderr por defecto)

    Adicionalmente, se usa un threading.Lock (lock_log) para ilustrar el
    patrón de protección explícita, aunque 'logging' ya es thread-safe
    internamente.

SINCRONIZACIÓN USADA:
    - threading.Lock       → protege stock_productos y escrituras de log
    - threading.Semaphore  → limita el número de clientes conectados simultáneamente
    - threading.Barrier    → sincroniza procesadores antes de habilitar despachos
    - threading.Event      → señal de cierre ordenado del servidor

PROTOCOLO:
    JSON sobre TCP con los campos:
        Petición:  {"accion": "CONSULTAR"|"PEDIR", "producto": "<nombre>", "cantidad": <int>}
        Respuesta: {"estado": "OK"|"ERROR"|"STOCK", "mensaje": "<texto>", "cantidad": <int>}

AUTOR:      Programación Concurrente - Modificación 3
FECHA:      2026
PYTHON:     3.8+
================================================================================
"""

# ──────────────────────────────────────────────────────────────────────────────
# IMPORTACIONES
# ──────────────────────────────────────────────────────────────────────────────

import socket           # Comunicación TCP/IP de bajo nivel
import threading        # Hilos, Lock, Semaphore, Barrier, Event
import json             # Serialización del protocolo de mensajes
import logging          # NUEVO (MOD-3): Sistema de logs estructurado de Python
import os               # Para obtener la ruta absoluta del archivo de log
import sys              # Para acceder a stderr en StreamHandler
import time             # Para simular tiempos de procesamiento

# ──────────────────────────────────────────────────────────────────────────────
# CONSTANTES DE CONFIGURACIÓN
# ──────────────────────────────────────────────────────────────────────────────

HOST                     = "127.0.0.1"   # Dirección loopback: solo acepta conexiones locales
PORT                     = 65000          # Puerto TCP en el rango de puertos dinámicos/privados
ENCODING                 = "utf-8"        # Codificación de texto para enviar/recibir mensajes
BUFFER_SIZE              = 4096           # Bytes máximos por lectura de socket
CAPACIDAD_MAXIMA_COLA    = 10             # Máx. conexiones pendientes en la cola del SO
NUM_PROCESADORES         = 3              # Hilos "procesadores" que atienden la cola de pedidos
PEDIDOS_MINIMOS_PARA_BARRERA = 5          # Pedidos que deben acumularse antes de cruzar la barrera
MAX_CLIENTES             = 5             # Semáforo: máx. clientes atendidos simultáneamente

# ──────────────────────────────────────────────────────────────────────────────
# CONSTANTE DE LOG (MODIFICACIÓN 3)
# ──────────────────────────────────────────────────────────────────────────────

# Nombre del archivo donde se persistirán todos los logs del servidor.
# Se crea (o sobreescribe) cada vez que arranca el servidor.
# Está en el mismo directorio que este script para facilitar la localización.
LOG_FILENAME = "servidor_log.txt"

# ──────────────────────────────────────────────────────────────────────────────
# ESTADO COMPARTIDO DEL SERVIDOR
# ──────────────────────────────────────────────────────────────────────────────

# Inventario de productos disponibles. Clave: nombre del producto, Valor: cantidad.
# Este diccionario es COMPARTIDO entre todos los hilos: cualquier hilo procesador
# puede leer y modificar el stock, por eso se protege con lock_stock.
stock_productos = {
    "Laptop"     : 10,
    "Mouse"      : 25,
    "Teclado"    : 20,
    "Monitor"    : 8,
    "Auriculares": 15,
    "USB"        : 30,
    "Cargador"   : 18,
    "Webcam"     : 12,
}

# Cola de pedidos pendientes. Los hilos de cliente depositan pedidos aquí;
# los hilos procesadores los consumen. Lista usada como cola FIFO simple.
cola_pedidos = []

# Contador de pedidos totales recibidos. Sirve para la lógica de la barrera.
contador_pedidos = 0

# ──────────────────────────────────────────────────────────────────────────────
# PRIMITIVAS DE SINCRONIZACIÓN
# ──────────────────────────────────────────────────────────────────────────────

# Lock para proteger 'stock_productos' y 'cola_pedidos'.
# Sin este lock, dos hilos podrían leer el mismo stock disponible y ambos
# procesarlo, resultando en stock negativo (race condition clásico).
lock_stock = threading.Lock()

# Lock para proteger 'contador_pedidos'.
lock_contador = threading.Lock()

# ── MODIFICACIÓN 3 ────────────────────────────────────────────────────────────
# lock_log: Lock explícito para escrituras de log.
#
# PREGUNTA CLAVE: ¿Por qué usar lock_log si 'logging' ya es thread-safe?
#
# El módulo 'logging' de Python utiliza internamente un threading.RLock en
# cada Handler para serializar las escrituras. Por lo tanto, técnicamente
# NO es necesario un lock externo para usar logging.
#
# Sin embargo, usamos lock_log con dos propósitos pedagógicos/prácticos:
#
#   1. DEMOSTRACIÓN EXPLÍCITA: En un contexto académico de programación
#      concurrente, es importante mostrar el patrón de protección explícita
#      de recursos compartidos, incluso cuando la biblioteca ya lo hace
#      internamente. Ayuda a entender QUÉ problema resuelve el lock.
#
#   2. AGRUPACIÓN ATÓMICA DE MENSAJES: Si en el futuro quisiéramos escribir
#      MÚLTIPLES líneas de log como una unidad atómica indivisible (sin que
#      otro hilo intercale sus líneas en medio), necesitaríamos un lock
#      externo que 'logging' NO provee a ese nivel de granularidad.
#      Con lock_log podemos adquirirlo, escribir N líneas, y liberarlo.
#
# Ejemplo de caso real donde sería NECESARIO:
#   with lock_log:
#       logger.info("=== INICIO DE TRANSACCIÓN ===")
#       logger.info(f"Cliente: {addr}")
#       logger.info(f"Pedido:  {pedido}")
#       logger.info("=== FIN DE TRANSACCIÓN ===")
#   # Las 4 líneas anteriores aparecerán juntas sin intercalado de otros hilos.
#
lock_log = threading.Lock()
# ─────────────────────────────────────────────────────────────────────────────

# Semáforo que limita el acceso simultáneo de clientes al servidor.
# Si MAX_CLIENTES=5, un sexto cliente deberá esperar hasta que uno se desconecte.
# Esto evita sobrecargar el servidor con demasiadas conexiones paralelas.
semaforo_clientes = threading.Semaphore(MAX_CLIENTES)

# Barrera de sincronización: todos los hilos procesadores (NUM_PROCESADORES)
# deben alcanzar la barrera antes de que cualquiera pueda continuar.
# Se usa para sincronizar el inicio del procesamiento de pedidos: los
# procesadores esperan hasta que haya suficientes pedidos acumulados.
barrera_procesadores = threading.Barrier(NUM_PROCESADORES)

# Evento de cierre: cuando se activa (set()), todos los hilos saben
# que deben terminar ordenadamente. Es una señal de "apagado".
evento_cierre = threading.Event()

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN DEL SISTEMA DE LOGGING (MODIFICACIÓN 3)
# ──────────────────────────────────────────────────────────────────────────────

def configurar_logging() -> logging.Logger:
    """
    Configura y devuelve el logger principal del servidor.

    MODIFICACIÓN 3 - FUNCIÓN CLAVE:
    ================================
    Esta función es el corazón de la modificación. Crea un logger con DOS
    destinos de salida simultáneos:

        ┌─────────────────────────────────────────────────────────┐
        │                    logger "servidor"                    │
        │                                                         │
        │  ┌──────────────────┐    ┌──────────────────────────┐  │
        │  │  StreamHandler   │    │      FileHandler         │  │
        │  │  (sys.stderr /   │    │  (servidor_log.txt)      │  │
        │  │   consola)       │    │                          │  │
        │  └──────────────────┘    └──────────────────────────┘  │
        └─────────────────────────────────────────────────────────┘

    PARÁMETROS DEL LOGGER:
        - Nivel: INFO → registra INFO, WARNING, ERROR, CRITICAL
                        ignora DEBUG (demasiado verboso para producción)
        - Formato: '%(asctime)s [%(threadName)s] %(message)s'
            * %(asctime)s    → marca de tiempo (ej: 2026-05-24 12:00:00,123)
            * %(threadName)s → nombre del hilo que emitió el mensaje
                               esto es MUY útil en código concurrente para
                               distinguir qué hilo escribió cada línea
            * %(message)s    → el texto del mensaje de log

    OPCIÓN filemode='a':
        Usamos 'a' (append) en el FileHandler para que, si el servidor se
        reinicia, los logs anteriores NO se borren. Cada sesión se añade
        al final del archivo. Si se quisiera empezar limpio en cada
        arranque, se usaría filemode='w' (write/truncate).

    Returns:
        logging.Logger: Instancia del logger configurada y lista para usar.
    """

    # Obtenemos una instancia de logger con el nombre "servidor".
    # Usar nombres en lugar del logger raíz (logging.getLogger()) permite
    # tener múltiples loggers con distintas configuraciones en la misma app.
    logger = logging.getLogger("servidor")

    # Establecemos el nivel mínimo de severidad que el logger procesará.
    # Jerarquía: DEBUG < INFO < WARNING < ERROR < CRITICAL
    # Con INFO, ignoramos mensajes de depuración interna muy detallados.
    logger.setLevel(logging.INFO)

    # ── FORMATO COMPARTIDO ────────────────────────────────────────────────────
    # Definimos el formato de cada línea de log. Es el MISMO para ambos
    # handlers (consola y archivo) para mantener consistencia.
    #
    # Ejemplo de línea generada:
    #   2026-05-24 12:05:33,421 [Procesador-1] Despachando 3 unidades de Mouse
    #
    formato = logging.Formatter(
        fmt="%(asctime)s [%(threadName)s] %(message)s",
        # datefmt controla el formato de la fecha. Por defecto incluye milisegundos.
        # Si se quisiera personalizar: datefmt="%Y-%m-%d %H:%M:%S"
    )

    # ── HANDLER 1: ARCHIVO (MODIFICACIÓN 3) ───────────────────────────────────
    # FileHandler abre (o crea) el archivo 'servidor_log.txt' y escribe
    # cada mensaje de log como una nueva línea.
    #
    # PARÁMETROS:
    #   filename: ruta del archivo. Al ser relativa, se crea en el
    #             directorio de trabajo actual (donde se ejecuta el script).
    #   mode='a': append mode → no sobreescribe, añade al final.
    #   encoding: misma codificación que el protocolo para consistencia.
    archivo_handler = logging.FileHandler(
        filename=LOG_FILENAME,
        mode="a",           # 'a' = append; usar 'w' para limpiar en cada inicio
        encoding=ENCODING,
    )
    archivo_handler.setLevel(logging.INFO)    # Este handler también filtra por nivel
    archivo_handler.setFormatter(formato)     # Aplica el formato definido arriba

    # ── HANDLER 2: CONSOLA (StreamHandler) ────────────────────────────────────
    # StreamHandler escribe en un stream de texto. Por defecto usa sys.stderr,
    # pero podemos cambiarlo a sys.stdout para que aparezca en la salida estándar.
    #
    # ¿Por qué mantener la consola además del archivo?
    #   → Permite supervisión en tiempo real sin necesidad de abrir el archivo.
    #   → En entornos de producción, la consola puede ser capturada por
    #     herramientas de monitoreo (systemd journal, Docker logs, etc.).
    consola_handler = logging.StreamHandler(sys.stdout)
    consola_handler.setLevel(logging.INFO)
    consola_handler.setFormatter(formato)

    # Añadimos ambos handlers al logger. Desde este momento, cada llamada
    # a logger.info(), logger.error(), etc., enviará el mensaje a AMBOS destinos.
    logger.addHandler(archivo_handler)
    logger.addHandler(consola_handler)

    return logger


# Instancia global del logger. Se crea una sola vez al importar el módulo.
# Todos los hilos compartirán este mismo logger (que ya es thread-safe internamente).
logger = configurar_logging()


# ──────────────────────────────────────────────────────────────────────────────
# FUNCIONES AUXILIARES DE LOGGING (MODIFICACIÓN 3)
# ──────────────────────────────────────────────────────────────────────────────

def log_info(mensaje: str) -> None:
    """
    Registra un mensaje de nivel INFO de forma thread-safe.

    MODIFICACIÓN 3:
    ===============
    Envuelve logger.info() con lock_log para garantizar que, si en el futuro
    necesitamos emitir MÚLTIPLES líneas como una unidad atómica, el patrón
    ya está establecido y es consistente en todo el código.

    Para mensajes simples (una sola línea), el lock es técnicamente redundante
    porque logging ya serializa las escrituras internamente.

    Args:
        mensaje (str): Texto del mensaje a registrar.
    """
    with lock_log:
        # El contexto del hilo (%(threadName)s) es capturado automáticamente
        # por el formatter de logging, no necesitamos incluirlo manualmente.
        logger.info(mensaje)


def log_error(mensaje: str) -> None:
    """
    Registra un mensaje de nivel ERROR de forma thread-safe.

    Los mensajes ERROR indican fallos que afectan una operación específica
    pero no detienen el servidor. Se guardan tanto en consola como en archivo.

    Args:
        mensaje (str): Descripción del error ocurrido.
    """
    with lock_log:
        logger.error(mensaje)


def log_warning(mensaje: str) -> None:
    """
    Registra un mensaje de nivel WARNING de forma thread-safe.

    Los WARNING indican situaciones inesperadas pero no críticas:
    stock bajo, cliente desconectado abruptamente, etc.

    Args:
        mensaje (str): Descripción de la situación de alerta.
    """
    with lock_log:
        logger.warning(mensaje)


# ──────────────────────────────────────────────────────────────────────────────
# HILO PROCESADOR DE PEDIDOS
# ──────────────────────────────────────────────────────────────────────────────

def procesador_pedidos(id_procesador: int) -> None:
    """
    Hilo trabajador que consume pedidos de la cola y actualiza el stock.

    FUNCIONAMIENTO:
        1. Espera en la Barrera hasta que todos los procesadores estén listos.
        2. En un bucle continuo, revisa la cola de pedidos.
        3. Si hay pedidos, toma el primero (FIFO), verifica stock y actualiza.
        4. Termina cuando 'evento_cierre' se activa y la cola está vacía.

    BARRERA (threading.Barrier):
        Todos los NUM_PROCESADORES hilos deben llegar a barrera_procesadores.wait()
        antes de que cualquiera pueda continuar. Esto garantiza que el sistema
        de procesamiento está completamente inicializado antes de atender pedidos.
        Si un procesador llegara antes que los demás, podría procesar pedidos
        mientras los otros aún se están configurando.

    Args:
        id_procesador (int): Identificador numérico del procesador (1, 2, 3...).
    """
    # Damos un nombre descriptivo al hilo para que aparezca en los logs.
    # %(threadName)s en el formato mostrará este nombre.
    threading.current_thread().name = f"Procesador-{id_procesador}"

    log_info(f"Procesador-{id_procesador} iniciado. Esperando en barrera...")

    # ── BARRERA DE SINCRONIZACIÓN ─────────────────────────────────────────────
    # Todos los procesadores esperan aquí hasta que los NUM_PROCESADORES hilos
    # hayan llegado. Simula una fase de "preparación" que todos deben completar.
    try:
        barrera_procesadores.wait(timeout=30)   # Timeout de seguridad: 30 segundos
        log_info(f"Procesador-{id_procesador} cruzó la barrera. Comenzando procesamiento.")
    except threading.BrokenBarrierError:
        # La barrera se rompe si un hilo llama a barrera.abort() o si
        # se agota el timeout. En ese caso, terminamos ordenadamente.
        log_error(f"Procesador-{id_procesador}: barrera rota, terminando.")
        return

    # ── BUCLE PRINCIPAL DE PROCESAMIENTO ─────────────────────────────────────
    while not evento_cierre.is_set() or cola_pedidos:
        # Si la cola está vacía, esperamos brevemente para no consumir CPU.
        # Esto es un "polling" simple; en producción se usaría queue.Queue
        # con bloqueo nativo (put/get bloqueantes).
        with lock_stock:
            if not cola_pedidos:
                pedido = None
            else:
                # Extraemos el primer pedido de la cola (FIFO).
                # pop(0) es O(n) en listas Python; para producción usar collections.deque.
                pedido = cola_pedidos.pop(0)
                log(f"[COLA] Estado: {cola_pedidos}")

        if pedido is None:
            # No hay trabajo: dormimos brevemente y volvemos a verificar.
            time.sleep(0.05)
            continue

        # ── PROCESAMIENTO DEL PEDIDO ─────────────────────────────────────────
        # El pedido es un diccionario con: producto, cantidad, conexion, addr
        producto    = pedido.get("producto", "")
        cantidad    = pedido.get("cantidad", 0)
        conn        = pedido.get("conexion")
        addr        = pedido.get("addr")
        accion      = pedido.get("accion", "PEDIR")

        log_info(f"Procesando pedido de {addr}: {accion} {cantidad}x '{producto}'")

        # Simulamos tiempo de procesamiento (búsqueda en almacén, validación, etc.)
        time.sleep(0.1)

        # Accedemos al stock de forma protegida para verificar y actualizar.
        with lock_stock:
            if producto not in stock_productos:
                # El producto no existe en el catálogo.
                respuesta = {
                    "estado"  : "ERROR",
                    "mensaje" : f"Producto '{producto}' no encontrado en el catálogo.",
                    "cantidad": 0,
                }
                log_warning(f"Producto desconocido solicitado: '{producto}' por {addr}")

            elif accion == "CONSULTAR":
                # Solo consulta: devuelve el stock sin modificarlo.
                stock_actual = stock_productos[producto]
                respuesta = {
                    "estado"  : "STOCK",
                    "mensaje" : f"Stock de '{producto}': {stock_actual} unidades.",
                    "cantidad": stock_actual,
                }
                log_info(f"Consulta de stock: '{producto}' → {stock_actual} unidades disponibles.")

            elif stock_productos[producto] >= cantidad:
                # Hay suficiente stock: se descuenta y se confirma el pedido.
                stock_productos[producto] -= cantidad
                respuesta = {
                    "estado"  : "OK",
                    "mensaje" : (
                        f"Pedido confirmado: {cantidad}x '{producto}'. "
                        f"Stock restante: {stock_productos[producto]}."
                    ),
                    "cantidad": cantidad,
                }
                log_info(
                    f"Despacho exitoso: {cantidad}x '{producto}' para {addr}. "
                    f"Stock restante: {stock_productos[producto]}."
                )
            else:
                # Stock insuficiente: se informa cuánto hay disponible.
                stock_actual = stock_productos[producto]
                respuesta = {
                    "estado"  : "ERROR",
                    "mensaje" : (
                        f"Stock insuficiente de '{producto}'. "
                        f"Solicitado: {cantidad}, Disponible: {stock_actual}."
                    ),
                    "cantidad": stock_actual,
                }
                log_warning(
                    f"Stock insuficiente: '{producto}' solicitado {cantidad}, "
                    f"disponible {stock_actual}. Cliente: {addr}."
                )

        # ── ENVÍO DE RESPUESTA AL CLIENTE ─────────────────────────────────────
        # Serializamos la respuesta como JSON y la enviamos al socket del cliente.
        try:
            datos_respuesta = json.dumps(respuesta).encode(ENCODING)
            conn.sendall(datos_respuesta)
            log_info(f"Respuesta enviada a {addr}: estado={respuesta['estado']}")
        except (OSError, BrokenPipeError) as e:
            # El cliente puede haberse desconectado antes de recibir la respuesta.
            log_error(f"Error al enviar respuesta a {addr}: {e}")

    log_info(f"Procesador-{id_procesador} terminando. Cola vacía y cierre solicitado.")


# ──────────────────────────────────────────────────────────────────────────────
# HILO MANEJADOR DE CLIENTE
# ──────────────────────────────────────────────────────────────────────────────

def manejar_cliente(conn: socket.socket, addr: tuple) -> None:
    """
    Hilo dedicado a gestionar la conexión con un cliente individual.

    RESPONSABILIDADES:
        1. Adquirir el semáforo (controla cuántos clientes simultáneos hay).
        2. Recibir mensajes JSON del cliente en un bucle.
        3. Encolar los pedidos válidos para que los procesadores los atiendan.
        4. Liberar el semáforo y cerrar el socket al terminar.

    SEMÁFORO (threading.Semaphore):
        El semáforo semaforo_clientes se inicializa con MAX_CLIENTES=5.
        Cada hilo cliente lo adquiere (decrementa) al entrar.
        Si ya hay 5 clientes activos, el siguiente hilo bloqueará aquí
        hasta que un cliente termine y libere el semáforo (incremente).
        Esto protege los recursos del servidor de sobrecarga.

    Args:
        conn (socket.socket): Socket de la conexión con el cliente.
        addr (tuple): Dirección del cliente (IP, puerto).
    """
    # Nombre descriptivo para que aparezca en los logs del hilo.
    threading.current_thread().name = f"Cliente-{addr[1]}"

    # ── ADQUISICIÓN DEL SEMÁFORO ──────────────────────────────────────────────
    # Si el servidor ya atiende MAX_CLIENTES, este acquire() bloqueará al hilo
    # hasta que se libere un slot. Garantiza no superar la capacidad máxima.
    semaforo_clientes.acquire()
    log_info(f"Nueva conexión aceptada: {addr}. Semáforo adquirido.")

    global contador_pedidos

    try:
        # ── BUCLE DE RECEPCIÓN DE MENSAJES ────────────────────────────────────
        while not evento_cierre.is_set():
            try:
                # recv() bloquea hasta recibir datos o detectar cierre del socket.
                # Si el cliente cierra la conexión, recv() devuelve bytes vacíos b"".
                datos = conn.recv(BUFFER_SIZE)
                if not datos:
                    # El cliente cerró la conexión ordenadamente.
                    log_info(f"Cliente {addr} cerró la conexión.")
                    break

                # Deserializamos el mensaje JSON recibido.
                try:
                    mensaje = json.loads(datos.decode(ENCODING))
                except json.JSONDecodeError as e:
                    log_error(f"JSON inválido de {addr}: {e}. Datos: {datos[:100]}")
                    # Enviamos error al cliente y continuamos esperando más mensajes.
                    error_resp = json.dumps({
                        "estado": "ERROR",
                        "mensaje": "Formato JSON inválido.",
                        "cantidad": 0
                    }).encode(ENCODING)
                    conn.sendall(error_resp)
                    continue

                accion   = mensaje.get("accion", "").upper()
                producto = mensaje.get("producto", "")
                cantidad = int(mensaje.get("cantidad", 1))

                log_info(
                    f"Mensaje recibido de {addr}: "
                    f"accion={accion}, producto='{producto}', cantidad={cantidad}"
                )

                # Validación básica del mensaje.
                if accion not in ("CONSULTAR", "PEDIR"):
                    log_warning(f"Acción desconocida '{accion}' de {addr}.")
                    conn.sendall(json.dumps({
                        "estado": "ERROR",
                        "mensaje": f"Acción '{accion}' no reconocida. Use CONSULTAR o PEDIR.",
                        "cantidad": 0
                    }).encode(ENCODING))
                    continue

                # ── ENCOLADO DEL PEDIDO ───────────────────────────────────────
                # El pedido se añade a la cola para ser procesado por un procesador.
                # Incluimos la referencia al socket (conn) para que el procesador
                # pueda enviar la respuesta directamente al cliente correcto.
                with lock_stock:
                    cola_pedidos.append({
                    log(f"[COLA] Estado: {cola_pedidos}")
                        "accion"  : accion,
                        "producto": producto,
                        "cantidad": cantidad,
                        "conexion": conn,
                        "addr"    : addr,
                    })

                # Actualizamos el contador de pedidos totales (thread-safe).
                with lock_contador:
                    contador_pedidos += 1
                    log_info(
                        f"Pedido encolado. Total acumulado: {contador_pedidos}. "
                        f"Cola actual: {len(cola_pedidos)} pedidos."
                    )

                    # ── LÓGICA DE BARRERA ─────────────────────────────────────
                    # Cuando se alcanza el umbral de pedidos, notificamos.
                    # (La barrera real está en los procesadores al iniciar;
                    # aquí simplemente registramos el hito.)
                    if contador_pedidos == PEDIDOS_MINIMOS_PARA_BARRERA:
                        log_info(
                            f"🔔 Umbral de {PEDIDOS_MINIMOS_PARA_BARRERA} pedidos "
                            f"alcanzado. Los procesadores ya están activos."
                        )

            except (ConnectionResetError, BrokenPipeError) as e:
                # El cliente se desconectó abruptamente (sin cierre ordenado).
                log_error(f"Conexión interrumpida con {addr}: {e}")
                break
            except OSError as e:
                # Error genérico de socket (socket cerrado, etc.).
                log_error(f"Error de socket con {addr}: {e}")
                break

    finally:
        # ── LIBERACIÓN DE RECURSOS ────────────────────────────────────────────
        # El bloque finally garantiza que SIEMPRE se ejecuta, incluso si
        # ocurre una excepción inesperada.

        conn.close()                    # Cerramos el socket del cliente.
        semaforo_clientes.release()     # Liberamos el slot del semáforo para el siguiente cliente.
        log_info(f"Conexión con {addr} cerrada. Semáforo liberado.")


# ──────────────────────────────────────────────────────────────────────────────
# FUNCIÓN PRINCIPAL DEL SERVIDOR
# ──────────────────────────────────────────────────────────────────────────────

def iniciar_servidor() -> None:
    """
    Función principal: configura el socket, lanza los hilos y gestiona el ciclo de vida.

    FLUJO:
        1. Registra la ruta del archivo de log (MODIFICACIÓN 3).
        2. Lanza NUM_PROCESADORES hilos procesadores.
        3. Crea y configura el socket TCP servidor.
        4. Acepta conexiones en bucle, creando un hilo por cliente.
        5. Al recibir KeyboardInterrupt (Ctrl+C), activa el evento de cierre.
        6. Espera a que todos los hilos terminen (join).
        7. Imprime la ruta del archivo de log al terminar (MODIFICACIÓN 3).
    """

    # ── RUTA DEL ARCHIVO DE LOG (MODIFICACIÓN 3) ──────────────────────────────
    # Calculamos la ruta absoluta del archivo de log para informar al usuario.
    # os.path.abspath convierte la ruta relativa en absoluta usando el
    # directorio de trabajo actual (cwd) en el momento de ejecutar el script.
    ruta_log = os.path.abspath(LOG_FILENAME)

    # Separador visual al inicio del archivo de log para distinguir sesiones.
    # Esto es especialmente útil cuando usamos mode='a' (append): podemos
    # ver dónde empieza cada nueva ejecución del servidor.
    with lock_log:
        logger.info("=" * 70)
        logger.info("SERVIDOR INICIANDO - MODIFICACIÓN 3: LOG EN ARCHIVO")
        logger.info(f"Archivo de log: {ruta_log}")
        logger.info("=" * 70)

    log_info(f"Configuración: HOST={HOST}, PORT={PORT}, MAX_CLIENTES={MAX_CLIENTES}")
    log_info(f"Procesadores: {NUM_PROCESADORES}, Buffer: {BUFFER_SIZE} bytes")
    log_info(f"Productos en catálogo: {list(stock_productos.keys())}")

    # ── INICIO DE HILOS PROCESADORES ─────────────────────────────────────────
    hilos_procesadores = []
    for i in range(1, NUM_PROCESADORES + 1):
        hilo = threading.Thread(
            target=procesador_pedidos,
            args=(i,),
            name=f"Procesador-{i}",
            daemon=True,    # daemon=True: el hilo termina automáticamente si el programa principal termina
        )
        hilo.start()
        hilos_procesadores.append(hilo)
        log_info(f"Hilo Procesador-{i} lanzado.")

    # ── CREACIÓN Y CONFIGURACIÓN DEL SOCKET SERVIDOR ─────────────────────────
    # socket.AF_INET → protocolo IPv4
    # socket.SOCK_STREAM → TCP (stream orientado a conexión, confiable)
    servidor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # SO_REUSEADDR: permite reutilizar el puerto inmediatamente después de
    # cerrar el servidor, sin esperar el timeout TIME_WAIT del protocolo TCP.
    # Sin esto, al reiniciar el servidor rápidamente obtendríamos "Address already in use".
    servidor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Vinculamos el socket a la dirección y puerto configurados.
    servidor_socket.bind((HOST, PORT))

    # Ponemos el socket en modo "escucha" con una cola de CAPACIDAD_MAXIMA_COLA
    # conexiones pendientes (conexiones que el SO acepta pero el programa aún no procesó).
    servidor_socket.listen(CAPACIDAD_MAXIMA_COLA)

    # Timeout en el socket servidor: accept() bloqueará máximo 1 segundo.
    # Esto permite al bucle verificar evento_cierre periódicamente.
    servidor_socket.settimeout(1.0)

    log_info(f"Servidor escuchando en {HOST}:{PORT}. Esperando conexiones...")
    log_info(f"📄 Los logs se guardan en: {ruta_log}")
    print(f"\n[SERVIDOR] Presione Ctrl+C para detener el servidor.\n")

    hilos_clientes = []

    try:
        # ── BUCLE PRINCIPAL DE ACEPTACIÓN ────────────────────────────────────
        while not evento_cierre.is_set():
            try:
                # accept() bloquea hasta recibir una nueva conexión o timeout.
                # Con settimeout(1.0), si no llega nadie en 1 segundo, lanza
                # socket.timeout y el while vuelve a verificar evento_cierre.
                conn, addr = servidor_socket.accept()
                log_info(f"Conexión entrante de {addr[0]}:{addr[1]}")

                # Creamos un hilo dedicado para este cliente.
                hilo_cliente = threading.Thread(
                    target=manejar_cliente,
                    args=(conn, addr),
                    name=f"Cliente-{addr[1]}",
                    daemon=True,
                )
                hilo_cliente.start()
                hilos_clientes.append(hilo_cliente)

                # Limpiamos la lista de hilos terminados para no acumular referencias.
                hilos_clientes = [h for h in hilos_clientes if h.is_alive()]
                log_info(f"Clientes activos actualmente: {len(hilos_clientes)}")

            except socket.timeout:
                # Timeout normal: el bucle verifica evento_cierre y continúa.
                continue
            except OSError as e:
                # El socket fue cerrado (puede ocurrir durante el apagado).
                if not evento_cierre.is_set():
                    log_error(f"Error en accept(): {e}")
                break

    except KeyboardInterrupt:
        # El usuario presionó Ctrl+C. Iniciamos el apagado ordenado.
        log_info("KeyboardInterrupt recibido. Iniciando apagado ordenado...")

    finally:
        # ── APAGADO ORDENADO ──────────────────────────────────────────────────
        log_info("Activando evento de cierre para todos los hilos...")
        evento_cierre.set()     # Señal a todos los hilos de que deben terminar

        # Cerramos el socket servidor para que accept() falle inmediatamente
        # si todavía está bloqueando en algún hilo.
        servidor_socket.close()
        log_info("Socket servidor cerrado.")

        # Esperamos a que todos los hilos clientes terminen (máx. 5 segundos cada uno).
        for hilo in hilos_clientes:
            if hilo.is_alive():
                hilo.join(timeout=5)
                log_info(f"Hilo {hilo.name} finalizado.")

        # Esperamos a que los procesadores terminen de procesar la cola restante.
        for hilo in hilos_procesadores:
            if hilo.is_alive():
                hilo.join(timeout=10)
                log_info(f"Hilo {hilo.name} finalizado.")

        # ── MODIFICACIÓN 3: RESUMEN FINAL DE LOG ─────────────────────────────
        with lock_log:
            logger.info("=" * 70)
            logger.info("SERVIDOR DETENIDO CORRECTAMENTE")
            logger.info(f"Total de pedidos procesados: {contador_pedidos}")
            logger.info(f"Stock final: {dict(stock_productos)}")
            logger.info(f"Archivo de log guardado en: {ruta_log}")
            logger.info("=" * 70)

        # Forzamos el vaciado (flush) de todos los handlers para garantizar
        # que todas las líneas de log se escribieron al disco antes de salir.
        logging.shutdown()

        # ── MENSAJE FINAL AL OPERADOR (MODIFICACIÓN 3) ───────────────────────
        # Imprimimos la ruta del archivo de log en la consola para que el
        # operador sepa dónde encontrar el historial completo de la sesión.
        print(f"\n{'='*60}")
        print(f"  SERVIDOR DETENIDO")
        print(f"  Pedidos totales procesados: {contador_pedidos}")
        print(f"  📄 Archivo de log guardado en:")
        print(f"     {ruta_log}")
        print(f"{'='*60}\n")


# ──────────────────────────────────────────────────────────────────────────────
# PUNTO DE ENTRADA
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    """
    Punto de entrada del script. Solo se ejecuta si se corre directamente
    (no si se importa como módulo desde otro archivo).

    MODIFICACIÓN 3:
        Al iniciar, el logging ya está configurado con FileHandler + StreamHandler.
        No es necesaria ninguna configuración adicional en el main; todo está
        encapsulado en configurar_logging() y la variable global 'logger'.
    """
    iniciar_servidor()
