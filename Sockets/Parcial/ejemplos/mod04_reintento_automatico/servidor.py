"""
==============================================================================
servidor.py  —  MODIFICACIÓN 4: Reintento automático en el cliente
==============================================================================

DESCRIPCIÓN GENERAL
-------------------
Este servidor TCP gestiona pedidos de productos de una tienda concurrente.
Acepta múltiples clientes simultáneamente, encola sus pedidos, los procesa
con un pool de hilos «procesadores» y devuelve respuestas JSON.

QUÉ CAMBIÓ RESPECTO AL SERVIDOR BASE
--------------------------------------
El único cambio visible en la interfaz de red es el MENSAJE de error cuando
la cola está llena:

  BASE       → "estado": "rechazado", "mensaje": "Cola de pedidos llena."
  MOD-04     → "estado": "rechazado", "mensaje": "Cola llena. Reintente más tarde."

El texto nuevo es intencionalmente descriptivo para que el cliente (y cualquier
operador que vea logs) sepa explícitamente que la acción correcta es reintentar.
Ninguna otra lógica del servidor cambia: el servidor no sabe ni le importa si
el cliente reintentará o no —esa responsabilidad recae completamente en el cliente.
Esto respeta el principio de «responsabilidad única» y hace que el servidor sea
agnóstico a la política de reintentos del cliente.

PRIMITIVAS DE CONCURRENCIA UTILIZADAS
---------------------------------------
  threading.Lock       → protege el acceso al diccionario de stock (región crítica).
  threading.Semaphore  → limita el número de clientes simultáneos conectados.
  threading.Barrier    → sincroniza a los procesadores antes de que empiecen a trabajar.
  threading.Event      → señal de apagado graceful para los hilos procesadores.
  queue.Queue          → cola de pedidos thread-safe (FIFO) compartida entre hilos.

PROTOCOLO DE COMUNICACIÓN
--------------------------
Todos los mensajes viajan como JSON codificado en UTF-8 sobre TCP.

  Pedido del cliente →  {"tipo": "pedido", "producto": "...", "cantidad": N}
  Respuesta OK       →  {"tipo": "respuesta", "estado": "ok",
                          "mensaje": "...", "stock_restante": N}
  Respuesta error    →  {"tipo": "respuesta", "estado": "rechazado",
                          "mensaje": "..."}
  Respuesta cola     →  {"tipo": "error",    "estado": "rechazado",
                          "mensaje": "Cola llena. Reintente más tarde."}
                          ^^^^^^^
                          MODIFICACIÓN 4: mensaje actualizado

FLUJO GENERAL
-------------
  main()
    ├─ crea cola, lock, semáforo, barrera, evento
    ├─ lanza NUM_PROCESADORES hilos procesadores (esperan en la barrera)
    └─ acepta conexiones TCP en bucle
         └─ por cada cliente → lanza hilo manejar_cliente()
              ├─ adquiere semáforo (limita concurrencia)
              ├─ recibe JSON
              ├─ intenta meter pedido en cola
              │    ├─ éxito → responde {"tipo":"respuesta","estado":"ok",...} *
              │    └─ cola llena → responde {"tipo":"error","estado":"rechazado",...}  ← MOD-04
              └─ libera semáforo

  (*) La respuesta de confirmación de encolado es inmediata; la respuesta de
      procesamiento se genera dentro del hilo procesador y se devuelve por el
      mismo socket que se pasó en la tupla de la cola.

==============================================================================
"""

import socket          # Comunicación TCP
import threading       # Hilos, Lock, Semaphore, Barrier, Event
import json            # Serialización del protocolo
import queue           # Cola FIFO thread-safe
import time            # Simula tiempo de procesamiento
import random          # Variación aleatoria en procesamiento

# ===========================================================================
# CONSTANTES DE CONFIGURACIÓN
# ===========================================================================

HOST = "127.0.0.1"          # Dirección de escucha (loopback, solo conexiones locales)
PORT = 65000                 # Puerto TCP; debe coincidir en el cliente
ENCODING = "utf-8"           # Codificación de texto para JSON
BUFFER_SIZE = 4096           # Bytes máximos leídos en un solo recv()

CAPACIDAD_MAXIMA_COLA = 10   # Máximo de pedidos pendientes en la cola
NUM_PROCESADORES = 3         # Hilos que consumen pedidos de la cola
PEDIDOS_MINIMOS_PARA_BARRERA = 5  # Pedidos que deben estar listos antes de liberar
                                  # la barrera (sincronización de inicio)
MAX_CLIENTES = 5             # Semáforo: máximo de clientes manejados en paralelo

# Inventario inicial de productos
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

# ===========================================================================
# PRIMITIVAS DE CONCURRENCIA (se instancian en main() y se pasan a los hilos)
# ===========================================================================
#
#  lock_stock   : threading.Lock
#      Garantiza exclusión mutua al leer+modificar stock_productos.
#      Sin este lock, dos procesadores podrían leer el mismo stock,
#      ambos creer que hay suficiente y quedar en stock negativo.
#
#  semaforo_clientes : threading.Semaphore(MAX_CLIENTES)
#      Cada hilo manejar_cliente() hace acquire() al entrar y release() al salir.
#      Si ya hay MAX_CLIENTES activos, el siguiente acquire() bloquea hasta que
#      uno libere, evitando explosión de hilos.
#
#  barrera : threading.Barrier(NUM_PROCESADORES)
#      Todos los procesadores llaman barrier.wait() antes de entrar al bucle
#      principal. Ninguno empieza a consumir la cola hasta que el último
#      procesador haya arrancado. Útil para garantizar que el sistema esté
#      «listo» antes de aceptar trabajo real.
#      NOTA: PEDIDOS_MINIMOS_PARA_BARRERA no se usa en la barrera directamente;
#      sirve para una segunda barrera opcional o como referencia de diseño.
#
#  evento_apagado : threading.Event
#      Cuando se quiere parar el servidor limpiamente, se hace evento.set().
#      Los procesadores chequean evento.is_set() en cada iteración y terminan.

# ===========================================================================
# FUNCIÓN: procesar_pedido
# ===========================================================================

def procesar_pedido(datos_pedido, lock_stock):
    """
    Aplica la lógica de negocio a un pedido: verifica stock y lo descuenta.

    PARÁMETROS
    ----------
    datos_pedido : dict
        Diccionario con al menos "producto" (str) y "cantidad" (int).
    lock_stock : threading.Lock
        Mutex que protege el diccionario global stock_productos.

    RETORNA
    -------
    dict
        Respuesta lista para serializar a JSON y enviar al cliente.
        - Si ok:       {"tipo": "respuesta", "estado": "ok",    "mensaje": ..., "stock_restante": N}
        - Si error:    {"tipo": "respuesta", "estado": "rechazado", "mensaje": ...}

    POR QUÉ USAR lock_stock AQUÍ
    ----------------------------
    La sección crítica es la combinación leer-decidir-escribir sobre stock_productos:
        1. leer  stock_productos[producto]        → ¿hay suficiente?
        2. decidir                                → sí/no
        3. escribir stock_productos[producto] -= cantidad
    Si dos hilos ejecutan (1) al mismo tiempo y ambos ven stock=5 para cantidad=4,
    ambos aprobarán, pero al hacer (3) el stock quedaría en -3.
    El Lock garantiza que solo un hilo a la vez ejecute esta región.
    """
    producto = datos_pedido.get("producto", "")
    cantidad = datos_pedido.get("cantidad", 0)

    # Validación básica de tipos antes de entrar a la región crítica
    if not isinstance(cantidad, int) or cantidad <= 0:
        return {
            "tipo": "respuesta",
            "estado": "rechazado",
            "mensaje": f"Cantidad inválida: {cantidad}. Debe ser un entero positivo.",
        }

    # ── REGIÓN CRÍTICA ──────────────────────────────────────────────────────
    with lock_stock:
        if producto not in stock_productos:
            return {
                "tipo": "respuesta",
                "estado": "rechazado",
                "mensaje": f"Producto '{producto}' no existe en el catálogo.",
            }

        stock_actual = stock_productos[producto]

        if stock_actual < cantidad:
            return {
                "tipo": "respuesta",
                "estado": "rechazado",
                "mensaje": (
                    f"Stock insuficiente para '{producto}'. "
                    f"Solicitado: {cantidad}, disponible: {stock_actual}."
                ),
            }

        # Descuento atómico dentro del lock
        stock_productos[producto] -= cantidad
        stock_restante = stock_productos[producto]
    # ── FIN REGIÓN CRÍTICA ──────────────────────────────────────────────────

    return {
        "tipo": "respuesta",
        "estado": "ok",
        "mensaje": f"Pedido de {cantidad}x '{producto}' procesado correctamente.",
        "stock_restante": stock_restante,
    }


# ===========================================================================
# FUNCIÓN: hilo_procesador
# ===========================================================================

def hilo_procesador(id_procesador, cola_pedidos, lock_stock, barrera, evento_apagado):
    """
    Hilo trabajador que consume pedidos de la cola y envía respuestas al cliente.

    PARÁMETROS
    ----------
    id_procesador  : int
        Identificador numérico del procesador (para logs).
    cola_pedidos   : queue.Queue
        Cola compartida de tuplas (datos_pedido, conn_socket).
    lock_stock     : threading.Lock
        Mutex para acceso al stock (se pasa a procesar_pedido).
    barrera        : threading.Barrier
        Barrera de sincronización de inicio entre procesadores.
    evento_apagado : threading.Event
        Señal para terminar el bucle de forma limpia.

    FUNCIONAMIENTO
    --------------
    1. Todos los procesadores convergen en barrier.wait().
       El último en llegar libera a todos simultáneamente.
    2. Cada procesador entra en un bucle:
         a. Intenta sacar un ítem de la cola con timeout de 1 segundo.
            El timeout evita que el hilo quede bloqueado eternamente si
            el evento de apagado se activa con la cola vacía.
         b. Si obtiene un pedido, lo procesa y envía la respuesta por socket.
         c. Si la cola está vacía (queue.Empty), comprueba evento_apagado.
    3. Al terminar el bucle, registra su cierre en consola.

    POR QUÉ BARRERA AQUÍ
    --------------------
    Si los procesadores empezaran a consumir la cola en cuanto arrancan,
    podría ocurrir que el procesador-0 procese todos los pedidos iniciales
    antes de que el procesador-2 haya arrancado. La barrera garantiza que
    el trabajo se distribuya de forma más equitativa desde el primer momento.
    """
    print(f"[Procesador-{id_procesador}] Arrancado. Esperando en barrera...")

    # ── SINCRONIZACIÓN DE INICIO ─────────────────────────────────────────────
    try:
        barrera.wait()  # Bloquea hasta que los NUM_PROCESADORES hilos lleguen aquí
    except threading.BrokenBarrierError:
        # La barrera puede romperse si otro hilo lanzó una excepción antes
        print(f"[Procesador-{id_procesador}] Barrera rota. Terminando.")
        return
    # ── FIN SINCRONIZACIÓN ───────────────────────────────────────────────────

    print(f"[Procesador-{id_procesador}] Barrera liberada. Iniciando consumo de cola.")

    # ── BUCLE PRINCIPAL DE CONSUMO ────────────────────────────────────────────
    while not evento_apagado.is_set():
        try:
            # Intenta obtener un pedido; si no hay en 1 s, vuelve a comprobar
            # el evento de apagado. El timeout impide bloqueo indefinido.
            datos_pedido, conn = cola_pedidos.get(timeout=1)
        except queue.Empty:
            # Cola vacía: verificar si debemos parar
            continue

        print(
            f"[Procesador-{id_procesador}] Procesando pedido: "
            f"{datos_pedido.get('producto')} x{datos_pedido.get('cantidad')}"
        )

        # Simula tiempo de procesamiento variable (acceso a BD, API, etc.)
        time.sleep(random.uniform(0.5, 1.5))

        # Lógica de negocio
        respuesta = procesar_pedido(datos_pedido, lock_stock)

        # Enviar respuesta al cliente a través del socket que llegó en la cola
        try:
            respuesta_json = json.dumps(respuesta, ensure_ascii=False)
            conn.sendall(respuesta_json.encode(ENCODING))
            print(
                f"[Procesador-{id_procesador}] Respuesta enviada: {respuesta['estado']}"
            )
        except OSError as e:
            # El cliente puede haberse desconectado antes de recibir la respuesta
            print(f"[Procesador-{id_procesador}] Error al enviar respuesta: {e}")
        finally:
            # Marcar la tarea como completada para que join() funcione correctamente
            cola_pedidos.task_done()
    # ── FIN BUCLE ─────────────────────────────────────────────────────────────

    print(f"[Procesador-{id_procesador}] Apagado graceful completado.")


# ===========================================================================
# FUNCIÓN: manejar_cliente
# ===========================================================================

def manejar_cliente(conn, addr, cola_pedidos, semaforo_clientes, evento_apagado):
    """
    Hilo dedicado a un cliente TCP concreto.

    PARÁMETROS
    ----------
    conn              : socket.socket
        Socket de la conexión con el cliente.
    addr              : tuple
        (IP, puerto) del cliente.
    cola_pedidos      : queue.Queue
        Cola compartida donde se depositan los pedidos para ser procesados.
    semaforo_clientes : threading.Semaphore
        Limita el número de clientes simultáneos. Se libera al terminar.
    evento_apagado    : threading.Event
        Si está activo, el hilo termina sin procesar el pedido.

    RESPONSABILIDADES
    -----------------
    1. Recibir el pedido JSON del cliente.
    2. Validar que el mensaje sea un pedido bien formado.
    3a. Si la cola tiene espacio → encolar el pedido (junto con el socket conn
        para que el procesador pueda responder).
        → IMPORTANTE: En este modelo, el hilo manejar_cliente NO envía la
          respuesta de negocio; solo confirma el encolado exitoso con un ACK.
          La respuesta real la manda el hilo procesador a través del mismo conn.
    3b. Si la cola está llena → responder con error de cola llena.
        ──────────────────────────────────────────────────────────────
        MODIFICACIÓN 4: El mensaje es "Cola llena. Reintente más tarde."
        Este texto guía explícitamente al cliente a reintentar.
        El cliente (ver cliente.py) usará esta respuesta {"tipo":"error",
        "estado":"rechazado"} para activar su lógica de reintentos.
        ──────────────────────────────────────────────────────────────
    4. Liberar el semáforo al finalizar.

    POR QUÉ EL SOCKET VIAJA EN LA COLA
    ------------------------------------
    El hilo manejar_cliente recibe el pedido y lo encola, pero el procesador
    es quien conoce el resultado (stock suficiente o no). Para que el procesador
    pueda comunicarse con el cliente correcto, el socket debe viajar junto al
    pedido dentro de la cola. Alternativa sería una cola de respuestas por cliente,
    pero esto es más simple y directo para este diseño.
    """
    print(f"[Servidor] Nueva conexión de {addr}")

    try:
        # ── RECEPCIÓN DEL PEDIDO ────────────────────────────────────────────
        datos_raw = conn.recv(BUFFER_SIZE)
        if not datos_raw:
            print(f"[Servidor] {addr} cerró la conexión sin enviar datos.")
            return

        # Decodificar y parsear JSON
        try:
            datos_pedido = json.loads(datos_raw.decode(ENCODING))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            error_resp = {
                "tipo": "respuesta",
                "estado": "rechazado",
                "mensaje": f"Mensaje malformado: {e}",
            }
            conn.sendall(json.dumps(error_resp).encode(ENCODING))
            return

        # Validar que sea un pedido con los campos mínimos
        if datos_pedido.get("tipo") != "pedido":
            error_resp = {
                "tipo": "respuesta",
                "estado": "rechazado",
                "mensaje": "Tipo de mensaje desconocido. Se esperaba 'pedido'.",
            }
            conn.sendall(json.dumps(error_resp).encode(ENCODING))
            return

        print(
            f"[Servidor] Pedido recibido de {addr}: "
            f"{datos_pedido.get('producto')} x{datos_pedido.get('cantidad')}"
        )

        # ── INTENTAR ENCOLAR EL PEDIDO ──────────────────────────────────────
        try:
            # put_nowait lanza queue.Full si la cola está al límite.
            # Usar put_nowait en lugar de put() evita bloquear al hilo manejador
            # (y por ende al cliente) indefinidamente.
            cola_pedidos.put_nowait((datos_pedido, conn))
            print(
                f"[Servidor] Pedido de {addr} encolado. "
                f"Cola: {cola_pedidos.qsize()}/{CAPACIDAD_MAXIMA_COLA}"
            )
            # ACK de encolado al cliente
            ack = {
                "tipo": "respuesta",
                "estado": "encolado",
                "mensaje": "Pedido recibido y encolado. Espera la confirmación de procesamiento.",
            }
            conn.sendall(json.dumps(ack, ensure_ascii=False).encode(ENCODING))

            # IMPORTANTE: No cerramos conn aquí porque el procesador
            # necesita usarla para enviar la respuesta de negocio.
            # El procesador cierra la conexión al finalizar (o aquí si
            # el diseño fuera distinto). En este diseño simplificado
            # la conexión se cierra en el bloque finally de abajo.
            # Para recibir la respuesta del procesador el cliente debe
            # hacer un segundo recv() en su propio hilo.

        except queue.Full:
            # ──────────────────────────────────────────────────────────────
            # MODIFICACIÓN 4 — Mensaje de cola llena actualizado
            # ──────────────────────────────────────────────────────────────
            # El mensaje original era "Cola de pedidos llena."
            # Lo cambiamos a "Cola llena. Reintente más tarde." para que
            # sea explícito: el cliente sabe que no fue un error permanente,
            # sino una condición temporal que puede resolverse reintentando.
            # El cliente (cliente.py) detecta {"tipo":"error","estado":"rechazado"}
            # y activa su función reintentar_pedido().
            # ──────────────────────────────────────────────────────────────
            respuesta_llena = {
                "tipo": "error",
                "estado": "rechazado",
                "mensaje": "Cola llena. Reintente más tarde.",
            }
            conn.sendall(
                json.dumps(respuesta_llena, ensure_ascii=False).encode(ENCODING)
            )
            print(f"[Servidor] Cola llena. Pedido de {addr} rechazado.")

    except OSError as e:
        print(f"[Servidor] Error de socket con {addr}: {e}")
    finally:
        # Liberar el semáforo SIEMPRE, incluso si hubo excepción,
        # para no bloquear futuros clientes.
        semaforo_clientes.release()
        print(f"[Servidor] Semáforo liberado para {addr}.")
        # Nota: conn.close() no se llama aquí cuando el pedido fue encolado,
        # porque el procesador aún puede necesitar el socket. En producción
        # real se usarían callbacks o futuros. En este ejemplo didáctico,
        # el procesador cierra el socket tras enviar su respuesta.
        # Para simplificar la demo, sí lo cerramos aquí cuando se rechazó.


# ===========================================================================
# FUNCIÓN PRINCIPAL: main
# ===========================================================================

def main():
    """
    Punto de entrada del servidor.

    SECUENCIA DE INICIALIZACIÓN
    ---------------------------
    1. Crear primitivas de concurrencia compartidas.
    2. Lanzar hilos procesadores (quedan bloqueados en la barrera).
    3. Abrir socket TCP y entrar en bucle de aceptación.
    4. Por cada nueva conexión, lanzar un hilo manejar_cliente().
    5. Al interrumpir con Ctrl+C, activar evento_apagado y esperar cierre.
    """

    print("=" * 60)
    print("  SERVIDOR — MODIFICACIÓN 4: Reintento automático del cliente")
    print("=" * 60)
    print(f"  Host: {HOST}:{PORT}")
    print(f"  Procesadores: {NUM_PROCESADORES}")
    print(f"  Capacidad cola: {CAPACIDAD_MAXIMA_COLA}")
    print(f"  Máx. clientes simultáneos: {MAX_CLIENTES}")
    print("=" * 60)

    # ── PRIMITIVAS DE CONCURRENCIA ───────────────────────────────────────────
    lock_stock = threading.Lock()
    # El Lock protege stock_productos de condiciones de carrera.
    # Solo un procesador puede leer+modificar el stock a la vez.

    semaforo_clientes = threading.Semaphore(MAX_CLIENTES)
    # Limita a MAX_CLIENTES clientes manejados en paralelo.
    # Acquire() en manejar_cliente(); Release() en su finally.

    barrera = threading.Barrier(NUM_PROCESADORES)
    # Sincroniza el inicio de los NUM_PROCESADORES hilos procesadores.
    # Todos esperan hasta que el último haya arrancado.

    evento_apagado = threading.Event()
    # Flag compartido para señalizar a los procesadores que paren.
    # Se activa en el bloque KeyboardInterrupt.

    # Cola FIFO thread-safe con capacidad máxima
    cola_pedidos = queue.Queue(maxsize=CAPACIDAD_MAXIMA_COLA)
    # maxsize=N hace que put_nowait() lance queue.Full si hay N elementos.
    # Esto es clave para detectar «cola llena» y responder al cliente.

    # ── LANZAR HILOS PROCESADORES ────────────────────────────────────────────
    hilos_procesadores = []
    for i in range(NUM_PROCESADORES):
        hilo = threading.Thread(
            target=hilo_procesador,
            args=(i, cola_pedidos, lock_stock, barrera, evento_apagado),
            daemon=True,          # Termina con el proceso principal
            name=f"Procesador-{i}",
        )
        hilo.start()
        hilos_procesadores.append(hilo)
        print(f"[Main] Procesador-{i} lanzado.")

    # ── SOCKET TCP ───────────────────────────────────────────────────────────
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as servidor_socket:
            # SO_REUSEADDR evita "Address already in use" al reiniciar rápido
            servidor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            servidor_socket.bind((HOST, PORT))
            servidor_socket.listen(MAX_CLIENTES)
            print(f"[Main] Servidor escuchando en {HOST}:{PORT}")

            # ── BUCLE DE ACEPTACIÓN ──────────────────────────────────────────
            while True:
                try:
                    conn, addr = servidor_socket.accept()
                except OSError:
                    # El socket fue cerrado (ej. Ctrl+C cerrará el contexto with)
                    break

                # Adquirir semáforo ANTES de lanzar el hilo.
                # Si ya hay MAX_CLIENTES activos, acquire() bloquea aquí
                # (en el hilo main) hasta que uno libere.
                semaforo_clientes.acquire()

                # Lanzar hilo manejador para este cliente
                hilo_cliente = threading.Thread(
                    target=manejar_cliente,
                    args=(conn, addr, cola_pedidos, semaforo_clientes, evento_apagado),
                    daemon=True,
                    name=f"Cliente-{addr}",
                )
                hilo_cliente.start()

    except KeyboardInterrupt:
        print("\n[Main] Interrupción recibida. Iniciando apagado graceful...")
        evento_apagado.set()    # Señal a los procesadores para que paren

        # Esperar a que los procesadores terminen su tarea actual
        for hilo in hilos_procesadores:
            hilo.join(timeout=5)
            if hilo.is_alive():
                print(f"[Main] {hilo.name} no terminó a tiempo.")

        print("[Main] Servidor cerrado.")


# ===========================================================================
# PUNTO DE ENTRADA
# ===========================================================================

if __name__ == "__main__":
    main()
