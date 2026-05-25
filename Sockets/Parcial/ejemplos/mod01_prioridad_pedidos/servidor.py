"""
================================================================================
MODIFICACIÓN 1: SISTEMA DE PRIORIDAD EN LOS PEDIDOS — servidor.py
================================================================================

PROPÓSITO GENERAL:
    Este servidor implementa un sistema de gestión de pedidos con PRIORIDAD.
    A diferencia del servidor original (donde los pedidos se procesaban en orden
    de llegada — FIFO), aquí los pedidos tienen un campo 'prioridad' (1, 2 o 3),
    y la cola interna se mantiene ORDENADA para que siempre se procese primero
    el pedido de MAYOR prioridad (valor numérico más bajo = más urgente).

PROBLEMA QUE RESUELVE:
    En un sistema real de comercio electrónico, no todos los pedidos son iguales.
    Un pedido urgente de un cliente VIP (prioridad 1) no debería esperar detrás
    de pedidos ordinarios (prioridad 3). Este módulo implementa una cola de
    prioridad para garantizar que los pedidos críticos se atiendan primero,
    independientemente del orden en que llegaron al servidor.

DIFERENCIAS CLAVE RESPECTO AL SERVIDOR ORIGINAL:
    1. La cola 'cola_pedidos' ya no es una lista FIFO simple.
       Ahora se inserta cada pedido en la posición correcta según su prioridad,
       manteniendo el invariante: menor número = mayor urgencia.
    2. El protocolo JSON incluye el campo 'prioridad' en cada pedido.
    3. Los logs del servidor muestran la prioridad de cada pedido recibido,
       encolado y procesado.
    4. La función de inserción ordenada 'insertar_pedido_ordenado()' reemplaza
       la llamada simple a 'cola_pedidos.append()'.

ESTRUCTURA DE CONCURRENCIA (igual que el original):
    ┌─────────────────────────────────────────────────────────────────┐
    │  Hilo principal: arranca el socket TCP y acepta conexiones      │
    │  ├── Por cada cliente: hilo 'manejar_cliente()'                 │
    │  │       Lee el pedido JSON, valida, inserta en cola ordenada   │
    │  └── NUM_PROCESADORES hilos 'procesador_pedidos()'              │
    │          Toman siempre el pedido de mayor prioridad (índice 0)  │
    └─────────────────────────────────────────────────────────────────┘

PRIMITIVAS DE SINCRONIZACIÓN USADAS:
    - threading.Lock          → protege la cola y el stock de condiciones de carrera
    - threading.Semaphore     → controla la capacidad máxima de la cola (backpressure)
    - threading.Barrier       → sincroniza procesadores cada N pedidos completados
    - threading.Event         → señal de apagado limpio del servidor

PROTOCOLO JSON (petición del cliente → servidor):
    {
        "tipo":      "pedido",
        "producto":  "Laptop",
        "cantidad":  2,
        "prioridad": 1        ← NUEVO CAMPO (1=alta, 2=media, 3=baja)
    }

PROTOCOLO JSON (respuesta del servidor → cliente):
    {
        "estado":   "ok" | "error",
        "mensaje":  "Descripción del resultado",
        "prioridad": 1        ← NUEVO: se devuelve la prioridad para confirmación
    }

AUTOR:      Programación Concurrente — Modificación 1
FECHA:      2026
VERSIÓN:    1.0
================================================================================
"""

# ---------------------------------------------------------------------------
# IMPORTACIONES
# ---------------------------------------------------------------------------
import socket       # API de sockets TCP/IP para comunicación entre procesos
import threading    # Módulo de hilos POSIX para concurrencia en Python
import json         # Serialización/deserialización de mensajes en formato JSON
import time         # Funciones de tiempo para simular procesamiento y logs
import logging      # Sistema de registro estructurado con niveles y timestamps
import sys          # Para acceder a parámetros del sistema (salida de error, etc.)

# ---------------------------------------------------------------------------
# CONFIGURACIÓN DEL SISTEMA DE LOGGING
# ---------------------------------------------------------------------------
# Se configura el logger ANTES de cualquier otra cosa para que todos los
# mensajes (incluyendo errores de inicialización) queden registrados.
#
# FORMATO: [TIMESTAMP] [NIVEL] [NOMBRE_HILO] — Mensaje
# Incluir el nombre del hilo es CRÍTICO en sistemas concurrentes: permite
# rastrear qué hilo generó cada línea de log y detectar condiciones de carrera.
logging.basicConfig(
    level=logging.DEBUG,           # Capturar TODOS los niveles (DEBUG, INFO, WARNING, ERROR)
    format="%(asctime)s [%(levelname)-8s] [%(threadName)-20s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout)   # Salida por consola (stdout)
    ]
)
logger = logging.getLogger("ServidorPrioridad")

# ---------------------------------------------------------------------------
# CONSTANTES DE CONFIGURACIÓN DEL SERVIDOR
# ---------------------------------------------------------------------------
# Estas constantes definen el comportamiento del servidor y se colocan
# en la sección superior del archivo para facilitar su ajuste sin tener
# que buscar valores dispersos en el código.

HOST = "127.0.0.1"
"""str: Dirección IPv4 de loopback. El servidor solo acepta conexiones locales.
En producción se usaría "0.0.0.0" para escuchar en todas las interfaces."""

PORT = 65000
"""int: Puerto TCP elegido en el rango de puertos efímeros (1024–65535).
Debe coincidir exactamente con el puerto configurado en cliente.py."""

ENCODING = "utf-8"
"""str: Codificación de caracteres para serializar/deserializar mensajes JSON.
UTF-8 es el estándar universal y soporta caracteres especiales (tildes, etc.)."""

BUFFER_SIZE = 4096
"""int: Tamaño del buffer de recepción en bytes (4 KB).
Suficiente para mensajes JSON típicos. Si un mensaje supera este tamaño,
se recibirá fragmentado y habría que implementar delimitadores de mensaje."""

CAPACIDAD_MAXIMA_COLA = 10
"""int: Máximo número de pedidos que pueden estar en cola simultáneamente.
Actúa como mecanismo de backpressure: si la cola está llena, el servidor
rechaza nuevos pedidos en lugar de acumularlos indefinidamente (evita OOM)."""

NUM_PROCESADORES = 3
"""int: Número de hilos procesadores que consumen pedidos de la cola.
Representa los 'trabajadores' o 'workers' del sistema."""

PEDIDOS_MINIMOS_PARA_BARRERA = 5
"""int: Número de pedidos que deben completarse antes de sincronizar todos
los procesadores en la barrera. Simula un 'checkpoint' de lote."""

MAX_CLIENTES = 5
"""int: Máximo de conexiones en la cola de espera del socket servidor (backlog).
Cuando llegan más conexiones simultáneas, el SO las encola hasta este límite."""

TIEMPO_PROCESAMIENTO_BASE = 1.5
"""float: Segundos base que tarda en 'procesarse' un pedido (simulación).
En un sistema real sería el tiempo de consulta a BD, pago, logística, etc."""

# ---------------------------------------------------------------------------
# STOCK DE PRODUCTOS
# ---------------------------------------------------------------------------
# Diccionario que mapea nombre de producto → cantidad disponible en almacén.
# MODIFICACIÓN 1: Este diccionario no cambia respecto al original; la
# prioridad afecta el ORDEN de procesamiento, no el stock en sí.
stock_productos = {
    "Laptop":      10,
    "Mouse":       25,
    "Teclado":     20,
    "Monitor":     8,
    "Auriculares": 15,
    "USB":         30,
    "Cargador":    18,
    "Webcam":      12,
}
"""dict[str, int]: Stock inicial de productos disponibles para venta.
Las cantidades se decrementan a medida que se procesan pedidos exitosos."""

# ---------------------------------------------------------------------------
# COLA DE PEDIDOS CON PRIORIDAD
# ---------------------------------------------------------------------------
# CAMBIO CLAVE RESPECTO AL ORIGINAL:
#   Original:   cola_pedidos = []  → lista FIFO simple (append + pop(0))
#   Modificación 1: cola_pedidos = []  → lista ORDENADA por prioridad
#
# La lista sigue siendo una lista de Python, pero ahora se mantiene
# ordenada: el elemento en el índice 0 es siempre el de MAYOR prioridad
# (menor valor numérico). Esto se logra con la función insertar_pedido_ordenado().
#
# ¿Por qué no usar heapq o PriorityQueue?
#   - heapq: válido pero requiere manejo de tuplas y desempate manual.
#   - PriorityQueue: encapsula el lock interno, pero perdemos control explícito.
#   - Lista ordenada: más transparente para fines educativos; muestra claramente
#     el invariante de prioridad y el funcionamiento del lock.
cola_pedidos = []
"""list[dict]: Cola de pedidos ordenada por campo 'prioridad' (ascendente).
El índice 0 siempre contiene el pedido de mayor urgencia.
Cada elemento es un dict con: tipo, producto, cantidad, prioridad, id_cliente."""

# ---------------------------------------------------------------------------
# PRIMITIVAS DE SINCRONIZACIÓN
# ---------------------------------------------------------------------------

lock_cola = threading.Lock()
"""threading.Lock: Mutex que protege el acceso a 'cola_pedidos' y 'stock_productos'.

¿POR QUÉ ES NECESARIO?
    La cola y el stock son recursos compartidos entre MÚLTIPLES hilos:
    - Los hilos 'manejar_cliente' INSERTAN pedidos en la cola.
    - Los hilos 'procesador_pedidos' EXTRAEN pedidos de la cola y MODIFICAN el stock.
    Sin un mutex, dos hilos podrían leer/escribir simultáneamente causando:
    - Pedidos perdidos (race condition al insertar)
    - Stock negativo (dos procesadores leen el mismo stock y ambos decrementan)
    - Corrupción de la lista (modificaciones concurrentes sin sincronización)

¿POR QUÉ UN SOLO LOCK PARA COLA Y STOCK?
    La operación "extraer pedido de cola + verificar stock + decrementar stock"
    debe ser ATÓMICA. Si usáramos locks separados, un hilo podría extraer el
    pedido, liberar el lock_cola, y antes de adquirir lock_stock otro hilo
    podría ver el stock incorrecto. Un único lock garantiza atomicidad completa."""

semaforo_cola = threading.Semaphore(CAPACIDAD_MAXIMA_COLA)
"""threading.Semaphore: Controla la capacidad máxima de la cola (backpressure).

¿POR QUÉ UN SEMÁFORO Y NO SOLO EL LOCK?
    El Lock controla el ACCESO EXCLUSIVO pero no la CAPACIDAD.
    El Semáforo tiene un contador interno inicializado a CAPACIDAD_MAXIMA_COLA.
    - Cuando un cliente quiere encolar un pedido: acquire() → contador−1.
      Si el contador llega a 0, el hilo se BLOQUEA hasta que haya espacio.
    - Cuando un procesador extrae un pedido: release() → contador+1.
    Esto implementa backpressure: cuando la cola está llena, los clientes nuevos
    reciben un rechazo inmediato (timeout=0) en lugar de esperar indefinidamente.

IMPORTANTE: El semáforo se usa con acquire(blocking=False) para evitar
bloquear el hilo del cliente. Si la cola está llena → rechazo instantáneo."""

barrera_procesadores = threading.Barrier(NUM_PROCESADORES)
"""threading.Barrier: Punto de sincronización entre los procesadores.

¿QUÉ ES UNA BARRERA?
    Una barrera hace que todos los hilos que lleguen a ella ESPEREN hasta que
    TODOS (NUM_PROCESADORES en total) hayan llegado. Solo entonces continúan.

¿PARA QUÉ SE USA AQUÍ?
    Cada vez que un procesador completa PEDIDOS_MINIMOS_PARA_BARRERA pedidos,
    espera en la barrera a que los otros procesadores también completen ese
    número. Esto simula un 'checkpoint de lote': antes de continuar, el sistema
    verifica que todos los procesadores están al día (útil para, por ejemplo,
    sincronizar logs de lote, generar reportes, o hacer flush de transacciones).

NOTA: En el código original la barrera funciona igual; la modificación no
la altera porque la prioridad afecta el ORDEN, no la sincronización por lotes."""

evento_apagado = threading.Event()
"""threading.Event: Señal de apagado limpio del servidor.

¿CÓMO FUNCIONA?
    Un threading.Event es un flag booleano thread-safe:
    - Inicialmente está en False (no señalado).
    - Cuando se quiere apagar el servidor: evento_apagado.set() → True.
    - Los hilos procesadores comprueban evento_apagado.is_set() en su bucle
      principal y terminan limpiamente cuando detectan la señal.
    Esto evita matar los hilos abruptamente (que podría dejar transacciones
    a medias o corromper el estado del sistema)."""

# Contador de pedidos procesados por cada procesador (para la barrera)
# NOTA: Usar una lista en lugar de un int simple porque las listas son mutables
# y Python las pasa por referencia, lo que permite modificarlas desde funciones.
# Alternativa más limpia: threading.local() o un array indexado por hilo.
contador_pedidos_procesados = [0] * NUM_PROCESADORES
"""list[int]: Contador por procesador de pedidos completados exitosamente.
Usado para determinar cuándo activar la barrera de sincronización."""

# Lock para el contador de pedidos (operación de incremento no es atómica en Python)
lock_contador = threading.Lock()
"""threading.Lock: Protege los accesos al contador_pedidos_procesados."""

# ---------------------------------------------------------------------------
# FUNCIÓN AUXILIAR: INSERCIÓN ORDENADA POR PRIORIDAD
# ---------------------------------------------------------------------------

def insertar_pedido_ordenado(pedido: dict) -> None:
    """
    Inserta un pedido en la cola manteniendo el orden ascendente por prioridad.

    MODIFICACIÓN CENTRAL de este módulo:
        En el servidor original se usaba: cola_pedidos.append(pedido)
        Aquí se reemplaza por esta función que mantiene la cola ORDENADA.

    ALGORITMO:
        Se recorre la cola de izquierda a derecha buscando la primera posición
        donde la prioridad del pedido a insertar sea MENOR O IGUAL que la del
        pedido en esa posición. Se inserta justo ahí.
        Esto garantiza que:
        - Pedidos con prioridad 1 quedan al inicio de la cola.
        - Pedidos con prioridad 3 quedan al final.
        - Pedidos de la misma prioridad se mantienen en orden de llegada (FIFO
          dentro de la misma prioridad — comportamiento justo/fair).

    COMPLEJIDAD:
        O(n) en tiempo (búsqueda lineal + inserción en lista).
        Para CAPACIDAD_MAXIMA_COLA = 10, esto es despreciable.
        Si la cola fuera muy grande (miles de elementos), se usaría bisect.insort()
        de la librería estándar, que hace búsqueda binaria → O(log n).

    PRECONDICIÓN:
        Esta función DEBE llamarse dentro de una sección crítica protegida
        por lock_cola, ya que modifica cola_pedidos (recurso compartido).

    Args:
        pedido (dict): Diccionario del pedido con al menos el campo 'prioridad'
                       (int: 1=alta, 2=media, 3=baja).

    Returns:
        None

    Ejemplo:
        >>> # Cola antes: [p1(pri=1), p2(pri=2)]
        >>> insertar_pedido_ordenado({"prioridad": 1, "producto": "Mouse", ...})
        >>> # Cola después: [p1(pri=1), nuevo(pri=1), p2(pri=2)]
        >>> # El nuevo pedido de prioridad 1 se insertó DESPUÉS del otro pri=1
        >>> # (FIFO dentro de la misma prioridad) y ANTES del de prioridad 2.
    """
    prioridad_nueva = pedido["prioridad"]

    # Buscar la posición de inserción correcta
    # Se busca la primera posición i donde cola_pedidos[i]["prioridad"] > prioridad_nueva
    # Esa es la posición donde el nuevo pedido debe insertarse para mantener el orden.
    posicion = len(cola_pedidos)  # Por defecto va al final (menor prioridad)

    for i, pedido_existente in enumerate(cola_pedidos):
        if pedido_existente["prioridad"] > prioridad_nueva:
            # Encontramos un pedido de menor urgencia → insertar antes
            posicion = i
            break  # No necesitamos seguir buscando

    # list.insert(i, x) inserta x en la posición i, desplazando el resto a la derecha
    # Esto es O(n) por el desplazamiento, pero n ≤ CAPACIDAD_MAXIMA_COLA = 10
    cola_pedidos.insert(posicion, pedido)

    logger.debug(
        f"[COLA] Pedido insertado en posición {posicion}/{len(cola_pedidos)-1} "
        f"| Prioridad: {prioridad_nueva} | Producto: {pedido['producto']} "
        f"| Cola actual: {[(p['producto'], p['prioridad']) for p in cola_pedidos]}"
    )


# ---------------------------------------------------------------------------
# FUNCIÓN: PROCESADOR DE PEDIDOS (hilo worker)
# ---------------------------------------------------------------------------

def procesador_pedidos(id_procesador: int) -> None:
    """
    Hilo worker que extrae y procesa pedidos de la cola de prioridad.

    MODIFICACIÓN RESPECTO AL ORIGINAL:
        Original:   pedido = cola_pedidos.pop(0)   → siempre el más antiguo (FIFO)
        Modificación 1: pedido = cola_pedidos.pop(0)  → IGUAL, pero ahora el índice 0
                        es el de MAYOR prioridad gracias a la cola ordenada.
        El código de extracción es idéntico, pero la SEMÁNTICA cambió: ya no es
        FIFO sino PRIORITY QUEUE. El ordenamiento se garantiza en la inserción.

    FLUJO DE EJECUCIÓN:
        1. Esperar a que haya pedidos en la cola (bucle de polling con sleep).
        2. Adquirir lock_cola.
        3. Extraer el pedido del índice 0 (mayor prioridad).
        4. Liberar lock_cola.
        5. Liberar una unidad del semáforo (hay un hueco libre en la cola).
        6. Simular procesamiento (time.sleep).
        7. Actualizar stock con lock_cola.
        8. Incrementar contador y verificar si se debe activar la barrera.
        9. Volver al paso 1.

    BARRERA:
        Cada vez que este procesador completa PEDIDOS_MINIMOS_PARA_BARRERA
        pedidos exitosos, espera en la barrera a que los otros procesadores
        también completen ese número antes de continuar.

    Args:
        id_procesador (int): Identificador único del procesador (0, 1, 2, ...).
                             Usado en logs y para indexar contador_pedidos_procesados.
    """
    logger.info(f"[PROCESADOR-{id_procesador}] Iniciado y listo para procesar pedidos.")

    # Contador local de pedidos procesados por ESTE procesador en este ciclo de barrera
    pedidos_en_ciclo_actual = 0

    while not evento_apagado.is_set():
        # ── FASE 1: VERIFICAR SI HAY PEDIDOS EN LA COLA ──────────────────────
        # Usamos polling con una pequeña pausa para no consumir 100% de CPU.
        # Alternativa más eficiente: threading.Condition (notify/wait), pero
        # el polling es más simple y suficiente para esta escala.
        with lock_cola:
            hay_pedido = len(cola_pedidos) > 0
            if hay_pedido:
                # EXTRACCIÓN DEL PEDIDO DE MAYOR PRIORIDAD
                # CLAVE: pop(0) extrae el PRIMER elemento de la lista.
                # Como la lista está ORDENADA por prioridad (menor número = mayor
                # urgencia = índice más bajo), pop(0) siempre da el pedido más urgente.
                # En el original, pop(0) también extraía el primero, pero era FIFO puro.
                # Aquí es PRIORITY QUEUE: la ordenación se hizo al insertar.
                pedido = cola_pedidos.pop(0)
                logger.debug(f"[COLA] Estado: {cola_pedidos}")
            else:
                pedido = None

        if pedido is None:
            # No hay pedidos: dormir brevemente para no quemar CPU en polling
            time.sleep(0.05)  # 50ms de espera antes de volver a revisar
            continue

        # ── FASE 2: LIBERAR ESPACIO EN EL SEMÁFORO ───────────────────────────
        # Al extraer un pedido de la cola, hay un hueco disponible.
        # Notificar al semáforo para que un cliente bloqueado (si lo hubiera)
        # pueda continuar y encolar su pedido.
        semaforo_cola.release()

        # ── FASE 3: LOG CON PRIORIDAD (NUEVO EN MODIFICACIÓN 1) ──────────────
        # El log ahora muestra explícitamente la prioridad del pedido,
        # lo que permite verificar que la cola de prioridad funciona correctamente.
        # En el original, este log no mostraba prioridad.
        prioridad_str = {1: "ALTA", 2: "MEDIA", 3: "BAJA"}.get(
            pedido.get("prioridad", 0), "DESCONOCIDA"
        )
        logger.info(
            f"[PROCESADOR-{id_procesador}] ► Procesando pedido "
            f"| Prioridad: {pedido['prioridad']} ({prioridad_str}) "
            f"| Producto: {pedido['producto']} "
            f"| Cantidad: {pedido['cantidad']} "
            f"| Cliente: {pedido.get('id_cliente', 'N/A')}"
        )

        # ── FASE 4: SIMULAR PROCESAMIENTO ────────────────────────────────────
        # En un sistema real aquí iría: consulta a BD, cargo a tarjeta, etc.
        # Pedidos de alta prioridad podrían procesarse más rápido si se quisiera
        # (por ejemplo: tiempo = BASE / prioridad), pero en este módulo el tiempo
        # es fijo para mantener la modificación enfocada solo en el ORDEN.
        tiempo_procesamiento = TIEMPO_PROCESAMIENTO_BASE
        logger.debug(
            f"[PROCESADOR-{id_procesador}] Simulando procesamiento "
            f"({tiempo_procesamiento}s)..."
        )
        time.sleep(tiempo_procesamiento)

        # ── FASE 5: ACTUALIZAR STOCK ──────────────────────────────────────────
        # La actualización del stock debe hacerse dentro del lock_cola porque:
        # 1. stock_productos es un recurso compartido entre todos los procesadores.
        # 2. La operación "leer stock → verificar → decrementar" no es atómica.
        # Sin el lock, dos procesadores podrían leer stock=1, ambos decrementar,
        # y quedar con stock=-1 (venta de producto inexistente).
        exito = False
        mensaje_resultado = ""

        with lock_cola:
            producto = pedido["producto"]
            cantidad = pedido["cantidad"]

            if producto not in stock_productos:
                mensaje_resultado = f"Producto '{producto}' no existe en el catálogo."
                logger.warning(
                    f"[PROCESADOR-{id_procesador}] ✗ Producto desconocido: {producto}"
                )
            elif stock_productos[producto] < cantidad:
                mensaje_resultado = (
                    f"Stock insuficiente para '{producto}': "
                    f"disponible={stock_productos[producto]}, pedido={cantidad}."
                )
                logger.warning(
                    f"[PROCESADOR-{id_procesador}] ✗ Stock insuficiente "
                    f"| {producto}: {stock_productos[producto]} < {cantidad}"
                )
            else:
                # Decrementar stock de forma segura (dentro del lock)
                stock_productos[producto] -= cantidad
                exito = True
                mensaje_resultado = (
                    f"Pedido procesado exitosamente. "
                    f"Stock restante de '{producto}': {stock_productos[producto]}."
                )
                logger.info(
                    f"[PROCESADOR-{id_procesador}] ✓ Pedido completado "
                    f"| Prioridad: {pedido['prioridad']} ({prioridad_str}) "
                    f"| {producto} x{cantidad} "
                    f"| Stock restante: {stock_productos[producto]}"
                )

        # ── FASE 6: BARRERA DE SINCRONIZACIÓN ────────────────────────────────
        if exito:
            pedidos_en_ciclo_actual += 1

            # Verificar si este procesador completó su cuota del ciclo actual
            if pedidos_en_ciclo_actual >= PEDIDOS_MINIMOS_PARA_BARRERA:
                logger.info(
                    f"[PROCESADOR-{id_procesador}] 🔀 Llegando a la barrera "
                    f"(completó {pedidos_en_ciclo_actual} pedidos exitosos). "
                    f"Esperando a otros procesadores..."
                )
                try:
                    # wait() bloquea hasta que NUM_PROCESADORES hilos lleguen aquí.
                    # El parámetro timeout evita bloqueo infinito si un procesador
                    # se cae (robustez ante fallos).
                    barrera_procesadores.wait(timeout=30.0)
                    logger.info(
                        f"[PROCESADOR-{id_procesador}] 🔓 Barrera superada. "
                        f"Todos los procesadores sincronizados. Iniciando nuevo ciclo."
                    )
                    pedidos_en_ciclo_actual = 0  # Reiniciar contador del ciclo
                except threading.BrokenBarrierError:
                    # La barrera se rompe si se llama a barrera.abort() o si
                    # un hilo que estaba esperando fue interrumpido.
                    logger.error(
                        f"[PROCESADOR-{id_procesador}] ⚠ Barrera rota. "
                        f"Continuando sin sincronización."
                    )
                    pedidos_en_ciclo_actual = 0  # Reiniciar para evitar bucle de error

    logger.info(f"[PROCESADOR-{id_procesador}] Señal de apagado recibida. Finalizando.")


# ---------------------------------------------------------------------------
# FUNCIÓN: MANEJADOR DE CLIENTE (hilo por conexión)
# ---------------------------------------------------------------------------

def manejar_cliente(conn: socket.socket, addr: tuple, id_cliente: int) -> None:
    """
    Gestiona la comunicación con un cliente conectado.

    Este hilo se crea por cada cliente que se conecta al servidor.
    Lee el pedido JSON, lo valida, y si la cola tiene espacio, llama a
    insertar_pedido_ordenado() para agregarlo en la posición correcta
    según su prioridad.

    MODIFICACIÓN RESPECTO AL ORIGINAL:
        1. Se extrae y valida el campo 'prioridad' del JSON recibido.
        2. El log de recepción incluye la prioridad del pedido.
        3. Se llama a insertar_pedido_ordenado() en lugar de cola_pedidos.append().
        4. La respuesta JSON incluye el campo 'prioridad' para confirmación.

    PROTOCOLO DE COMUNICACIÓN:
        Cliente → Servidor:
            {"tipo": "pedido", "producto": "Laptop", "cantidad": 2, "prioridad": 1}
        Servidor → Cliente:
            {"estado": "ok", "mensaje": "Pedido encolado.", "prioridad": 1}
            o
            {"estado": "error", "mensaje": "Motivo del error.", "prioridad": 1}

    Args:
        conn  (socket.socket): Socket de conexión con el cliente específico.
        addr  (tuple):         Dirección del cliente (ip, puerto).
        id_cliente (int):      Identificador secuencial del cliente.
    """
    logger.info(f"[CLIENTE-{id_cliente}] Conexión aceptada desde {addr[0]}:{addr[1]}")

    try:
        # ── PASO 1: RECIBIR DATOS DEL CLIENTE ─────────────────────────────────
        # recv() devuelve bytes. El argumento es el tamaño máximo del buffer.
        # Si el cliente envía más de BUFFER_SIZE bytes, el mensaje se fragmenta.
        # Para este protocolo de mensajes pequeños (< 1 KB), es suficiente.
        datos_crudos = conn.recv(BUFFER_SIZE)

        if not datos_crudos:
            # El cliente cerró la conexión sin enviar datos (posible error de red)
            logger.warning(f"[CLIENTE-{id_cliente}] Conexión cerrada sin datos.")
            return

        # ── PASO 2: DESERIALIZAR JSON ──────────────────────────────────────────
        try:
            mensaje = json.loads(datos_crudos.decode(ENCODING))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"[CLIENTE-{id_cliente}] JSON inválido: {e}")
            respuesta = {"estado": "error", "mensaje": f"Formato JSON inválido: {e}"}
            conn.sendall(json.dumps(respuesta).encode(ENCODING))
            return

        # ── PASO 3: VALIDAR TIPO DE MENSAJE ───────────────────────────────────
        if mensaje.get("tipo") != "pedido":
            logger.warning(
                f"[CLIENTE-{id_cliente}] Tipo de mensaje desconocido: "
                f"{mensaje.get('tipo')}"
            )
            respuesta = {
                "estado": "error",
                "mensaje": "Tipo de mensaje no reconocido. Use 'tipo': 'pedido'."
            }
            conn.sendall(json.dumps(respuesta).encode(ENCODING))
            return

        # ── PASO 4: EXTRAER Y VALIDAR CAMPOS (INCLUYE 'prioridad') ───────────
        # MODIFICACIÓN 1: Se añade validación del campo 'prioridad'.
        # El original no tenía este campo; aquí es OBLIGATORIO.

        producto  = mensaje.get("producto")
        cantidad  = mensaje.get("cantidad")
        prioridad = mensaje.get("prioridad")  # ← NUEVO CAMPO

        # Validar que los campos requeridos existen
        if not producto or cantidad is None or prioridad is None:
            logger.warning(
                f"[CLIENTE-{id_cliente}] Campos faltantes en el pedido: {mensaje}"
            )
            respuesta = {
                "estado": "error",
                "mensaje": "Campos requeridos: 'producto', 'cantidad', 'prioridad'."
            }
            conn.sendall(json.dumps(respuesta).encode(ENCODING))
            return

        # Validar tipos de datos
        if not isinstance(cantidad, int) or cantidad <= 0:
            respuesta = {
                "estado": "error",
                "mensaje": "'cantidad' debe ser un entero positivo."
            }
            conn.sendall(json.dumps(respuesta).encode(ENCODING))
            return

        # VALIDACIÓN DE PRIORIDAD: Solo acepta 1, 2 o 3
        # 1 = Alta (más urgente), 2 = Media, 3 = Baja (menos urgente)
        if prioridad not in (1, 2, 3):
            logger.warning(
                f"[CLIENTE-{id_cliente}] Prioridad inválida: {prioridad}"
            )
            respuesta = {
                "estado": "error",
                "mensaje": "'prioridad' debe ser 1 (alta), 2 (media) o 3 (baja).",
                "prioridad": prioridad
            }
            conn.sendall(json.dumps(respuesta).encode(ENCODING))
            return

        # Mapear prioridad numérica a texto para logs más legibles
        prioridad_texto = {1: "ALTA", 2: "MEDIA", 3: "BAJA"}[prioridad]

        logger.info(
            f"[CLIENTE-{id_cliente}] Pedido recibido "
            f"| Prioridad: {prioridad} ({prioridad_texto}) "
            f"| Producto: {producto} "
            f"| Cantidad: {cantidad}"
        )

        # ── PASO 5: VERIFICAR CAPACIDAD DE LA COLA (SEMÁFORO) ────────────────
        # acquire(blocking=False) intenta decrementar el semáforo sin bloquear.
        # Si el contador es 0 (cola llena), devuelve False inmediatamente.
        # Esto implementa "fail-fast": rechazar pedido sin hacer esperar al cliente.
        if not semaforo_cola.acquire(blocking=False):
            logger.warning(
                f"[CLIENTE-{id_cliente}] Cola llena ({CAPACIDAD_MAXIMA_COLA} "
                f"pedidos). Pedido rechazado | Prioridad: {prioridad} ({prioridad_texto})"
            )
            respuesta = {
                "estado": "error",
                "mensaje": (
                    f"Servidor ocupado. Cola llena "
                    f"({CAPACIDAD_MAXIMA_COLA} pedidos máx.). Intente más tarde."
                ),
                "prioridad": prioridad
            }
            conn.sendall(json.dumps(respuesta).encode(ENCODING))
            return

        # ── PASO 6: ENCOLAR EL PEDIDO CON PRIORIDAD ───────────────────────────
        # MODIFICACIÓN CENTRAL: En lugar de cola_pedidos.append(pedido),
        logger.debug(f"[COLA] Estado: {cola_pedidos}")
        # usamos insertar_pedido_ordenado() que mantiene el invariante de orden.
        pedido = {
            "tipo":       "pedido",
            "producto":   producto,
            "cantidad":   cantidad,
            "prioridad":  prioridad,   # ← NUEVO: guardamos la prioridad en el pedido
            "id_cliente": id_cliente,  # Para trazabilidad en logs
        }

        with lock_cola:
            # insertar_pedido_ordenado() hace cola_pedidos.insert(pos, pedido)
            # donde pos mantiene el orden ascendente por 'prioridad'.
            # ANTES (original):    cola_pedidos.append(pedido)  → al final siempre
            logger.debug(f"[COLA] Estado: {cola_pedidos}")
            # AHORA (mod 1):       insertar_pedido_ordenado(pedido)  → posición correcta
            insertar_pedido_ordenado(pedido)

        logger.info(
            f"[CLIENTE-{id_cliente}] ✓ Pedido encolado "
            f"| Prioridad: {prioridad} ({prioridad_texto}) "
            f"| {producto} x{cantidad} "
            f"| Pedidos en cola: {len(cola_pedidos)}/{CAPACIDAD_MAXIMA_COLA}"
        )

        # ── PASO 7: ENVIAR CONFIRMACIÓN AL CLIENTE ────────────────────────────
        # MODIFICACIÓN 1: La respuesta incluye 'prioridad' para que el cliente
        # pueda confirmar que su pedido fue registrado con la prioridad correcta.
        respuesta = {
            "estado":    "ok",
            "mensaje":   (
                f"Pedido de '{producto}' x{cantidad} encolado con prioridad "
                f"{prioridad} ({prioridad_texto}). Será procesado en orden."
            ),
            "prioridad": prioridad   # ← NUEVO: devuelto para confirmación
        }
        conn.sendall(json.dumps(respuesta).encode(ENCODING))

    except ConnectionResetError:
        # El cliente cerró la conexión abruptamente (Ctrl+C, caída de red, etc.)
        logger.warning(f"[CLIENTE-{id_cliente}] Conexión restablecida abruptamente.")
    except OSError as e:
        logger.error(f"[CLIENTE-{id_cliente}] Error de socket: {e}")
    except Exception as e:
        logger.critical(
            f"[CLIENTE-{id_cliente}] Error inesperado: {type(e).__name__}: {e}",
            exc_info=True   # Incluir traceback completo en el log
        )
    finally:
        # SIEMPRE cerrar el socket del cliente, incluso si ocurrió una excepción.
        # No hacerlo causaría file descriptor leak.
        try:
            conn.close()
            logger.debug(f"[CLIENTE-{id_cliente}] Socket cerrado.")
        except OSError:
            pass  # El socket ya estaba cerrado


# ---------------------------------------------------------------------------
# FUNCIÓN PRINCIPAL: INICIO DEL SERVIDOR
# ---------------------------------------------------------------------------

def iniciar_servidor() -> None:
    """
    Inicializa y arranca el servidor TCP con cola de prioridad.

    FLUJO:
        1. Crear socket TCP (AF_INET, SOCK_STREAM).
        2. Configurar SO_REUSEADDR para reusar el puerto inmediatamente tras reinicios.
        3. Hacer bind() al HOST:PORT.
        4. Hacer listen() con backlog MAX_CLIENTES.
        5. Arrancar los hilos procesadores en background (daemon=True).
        6. Entrar en el bucle de aceptación de conexiones.
        7. Por cada conexión: crear hilo manejar_cliente() y lanzarlo.
        8. Capturar KeyboardInterrupt para apagado limpio.
    """
    logger.info("=" * 70)
    logger.info("  SERVIDOR DE PEDIDOS CON PRIORIDAD — MODIFICACIÓN 1")
    logger.info("=" * 70)
    logger.info(f"  Host:               {HOST}:{PORT}")
    logger.info(f"  Capacidad de cola:  {CAPACIDAD_MAXIMA_COLA} pedidos")
    logger.info(f"  Procesadores:       {NUM_PROCESADORES} hilos")
    logger.info(f"  Barrera cada:       {PEDIDOS_MINIMOS_PARA_BARRERA} pedidos por procesador")
    logger.info(f"  Stock inicial:      {stock_productos}")
    logger.info("=" * 70)
    logger.info("  PRIORIDAD: 1=ALTA (primero), 2=MEDIA, 3=BAJA (último)")
    logger.info("=" * 70)

    # ── PASO 1: ARRANCAR HILOS PROCESADORES ───────────────────────────────────
    # Los procesadores se inician ANTES de aceptar conexiones para estar listos.
    # daemon=True: estos hilos terminan automáticamente cuando el hilo principal muere.
    procesadores = []
    for i in range(NUM_PROCESADORES):
        hilo = threading.Thread(
            target=procesador_pedidos,
            args=(i,),
            name=f"Procesador-{i}",
            daemon=True   # Hilo daemon: muere con el proceso principal
        )
        hilo.start()
        procesadores.append(hilo)
        logger.debug(f"Hilo procesador {i} iniciado.")

    # ── PASO 2: CREAR Y CONFIGURAR EL SOCKET SERVIDOR ─────────────────────────
    # AF_INET:     Familia de direcciones IPv4
    # SOCK_STREAM: Protocolo TCP (orientado a conexión, confiable, ordenado)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as servidor_socket:

        # SO_REUSEADDR: Permite reusar inmediatamente el puerto tras un reinicio.
        # Sin esto, el SO mantiene el puerto en estado TIME_WAIT durante ~60s
        # y el servidor no podría reiniciarse rápidamente tras un fallo.
        servidor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # bind(): asociar el socket a la dirección y puerto específicos
        servidor_socket.bind((HOST, PORT))

        # listen(): poner el socket en modo pasivo (espera conexiones)
        # MAX_CLIENTES = backlog: máximo de conexiones pendientes en la cola del SO
        servidor_socket.listen(MAX_CLIENTES)

        logger.info(f"✓ Servidor escuchando en {HOST}:{PORT}")
        logger.info("  Presione Ctrl+C para detener el servidor.\n")

        # Contador de clientes para identificación en logs
        id_cliente_counter = 0

        try:
            while True:
                # ── PASO 3: ACEPTAR CONEXIONES ────────────────────────────────
                # accept() es una llamada BLOQUEANTE: suspende el hilo principal
                # hasta que un cliente se conecta. Devuelve (socket, dirección).
                try:
                    conn, addr = servidor_socket.accept()
                except OSError:
                    # Ocurre al cerrar el socket durante KeyboardInterrupt
                    break

                id_cliente_counter += 1
                id_actual = id_cliente_counter

                # ── PASO 4: CREAR HILO POR CLIENTE ────────────────────────────
                # Cada cliente tiene su propio hilo para no bloquear al servidor
                # mientras se procesa la solicitud de ese cliente.
                # daemon=True: el hilo muere si el servidor termina abruptamente.
                hilo_cliente = threading.Thread(
                    target=manejar_cliente,
                    args=(conn, addr, id_actual),
                    name=f"Cliente-{id_actual}",
                    daemon=True
                )
                hilo_cliente.start()
                logger.debug(
                    f"Hilo para Cliente-{id_actual} lanzado "
                    f"({threading.active_count()} hilos activos total)."
                )

        except KeyboardInterrupt:
            # ── PASO 5: APAGADO LIMPIO ─────────────────────────────────────────
            logger.info("\n[SERVIDOR] Ctrl+C detectado. Iniciando apagado limpio...")

            # Señalar a todos los procesadores que deben terminar
            evento_apagado.set()

            # Esperar a que los procesadores terminen (máximo 5 segundos cada uno)
            for i, hilo in enumerate(procesadores):
                hilo.join(timeout=5.0)
                if hilo.is_alive():
                    logger.warning(f"[SERVIDOR] Procesador-{i} no terminó a tiempo.")
                else:
                    logger.info(f"[SERVIDOR] Procesador-{i} terminado correctamente.")

            logger.info("[SERVIDOR] Apagado completado. ¡Hasta luego!")


# ---------------------------------------------------------------------------
# PUNTO DE ENTRADA
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Llamar a la función principal que arranca el servidor.
    # La protección 'if __name__ == "__main__"' evita que el servidor se inicie
    # si este módulo es importado por otro (por ejemplo, en pruebas unitarias).
    iniciar_servidor()
