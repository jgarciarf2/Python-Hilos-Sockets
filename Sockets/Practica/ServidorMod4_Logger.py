"""
MODIFICACIÓN 4: Agregar hilo Logger dedicado
============================================
CAMBIO PRINCIPAL:
  - ANTES: Todos los hilos escriben directamente con print() o a un archivo,
    lo que puede causar mensajes entremezclados en salida concurrente.
  - AHORA:  Un hilo LOGGER dedicado es el ÚNICO que escribe al archivo de log.
    Los demás hilos solo ponen mensajes en una cola de log (queue.Queue).
    El logger consume esa cola y escribe ordenadamente.

POR QUÉ ES UN PATRÓN IMPORTANTE:
  - Patrón "Single Writer" → evita condiciones de carrera en I/O.
  - Si varios hilos escriben a un archivo sin sincronización, las líneas
    pueden mezclarse (interleaving de caracteres).
  - Con la cola de log, cada mensaje es una unidad atómica: se pone completo
    en la cola y se escribe completo en el archivo.
  - Es el mismo patrón que usan frameworks como Python logging con handlers.

NUEVO SINCRONIZADOR:
  - queue.Queue para log_queue (thread-safe por diseño).
  - El hilo logger hace log_queue.get() bloqueante → duerme hasta que
    haya un mensaje, sin consumir CPU.

RESTO: semáforo, lock, barrera, sockets igual al taller base.
"""

import socket
import threading
import queue
import random
import time
from datetime import datetime

# ── Configuración ──────────────────────────────────────────────────────────────
HOST             = 'localhost'
PORT             = 12345
MAX_COLA         = 10
NUM_PROCESADORES = 5
MIN_PEDIDOS      = 5
LOG_FILE         = 'central_pedidos.log'   # archivo de log

# ── Stock compartido ───────────────────────────────────────────────────────────
stock = {
    'productoA': 50,
    'productoB': 30,
    'productoC': 20
}

# ── Cola de pedidos ────────────────────────────────────────────────────────────
pedidos = []

# ── Cola de log ───────────────────────────────────────────────────────────────
# *** NUEVO *** Cola thread-safe para mensajes de log
# Cualquier hilo puede hacer log_queue.put("mensaje") de forma segura.
# Solo el hilo Logger consume esta cola y escribe al archivo.
log_queue = queue.Queue()

# ── Sincronizadores ────────────────────────────────────────────────────────────
semaforo_cola      = threading.Semaphore(MAX_COLA)
lock_pedidos       = threading.Lock()
barrera_procesadores = threading.Barrier(NUM_PROCESADORES)

# ── Estadísticas ───────────────────────────────────────────────────────────────
stats = {"recibidos": 0, "procesados": 0, "rechazados": 0}
lock_stats = threading.Lock()


# ── FUNCIÓN DE LOG (usada por todos los hilos) ─────────────────────────────────
def log(origen, mensaje):
    """
    Encola un mensaje de log. NO escribe directamente.
    Cualquier hilo llama a log() y continúa inmediatamente.
    El hilo Logger es quien realmente escribe (asíncrono).

    Args:
        origen  (str): nombre del hilo/componente que origina el log.
        mensaje (str): texto del mensaje.
    """
    ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]   # timestamp con ms
    entrada = f"[{ts}] [{origen}] {mensaje}"
    print(entrada)            # también imprimir en consola
    log_queue.put(entrada)    # encolar para escritura a archivo


# ── HILO LOGGER ────────────────────────────────────────────────────────────────
def hilo_logger():
    """
    *** NUEVO HILO ***
    Único responsable de escribir al archivo de log.

    PATRÓN:
      1. log_queue.get() → bloquea hasta que haya un mensaje (sin busy-wait).
      2. Escribe el mensaje al archivo con flush() inmediato.
      3. log_queue.task_done() → señala que el mensaje fue procesado.
      4. Repite indefinidamente.

    VENTAJA DE CONCURRENCIA:
      - No hay Lock para el archivo porque solo este hilo escribe.
      - Los demás hilos nunca bloquean esperando I/O de disco.
      - Si el disco es lento, los mensajes se acumulan en la cola
        sin bloquear al servidor.
    """
    log("LOGGER", f"Iniciado. Escribiendo en: {LOG_FILE}")
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"Sesión iniciada: {datetime.now()}\n")
        f.write(f"{'='*60}\n")

        while True:
            try:
                # get() bloqueante con timeout para poder apagar limpiamente
                entrada = log_queue.get(timeout=1)
                f.write(entrada + '\n')
                f.flush()   # escribir a disco inmediatamente
                log_queue.task_done()
            except queue.Empty:
                continue   # timeout → verificar si debe seguir corriendo


# ── HILO CLIENTE ───────────────────────────────────────────────────────────────
def manejar_cliente(conn, addr):
    """
    Igual al taller base, pero usando log() en lugar de print() directo.
    """
    nombre_cliente = f"Cliente-{addr[1]}"
    log("SERVIDOR", f"{nombre_cliente} conectado desde {addr}")

    try:
        info = (f"Productos disponibles: {list(stock.keys())} | "
                f"Cantidades: {list(stock.values())}\n")
        conn.sendall(info.encode('utf-8'))

        while True:
            datos = conn.recv(1024).decode('utf-8').strip()
            if not datos:
                break

            try:
                producto, cantidad = datos.split(',')
                cantidad = int(cantidad.strip())
                producto = producto.strip()
            except ValueError:
                conn.sendall("❌ Formato incorrecto. Usa: producto,cantidad\n".encode('utf-8'))
                continue

            with lock_pedidos:
                if producto not in stock:
                    msg = f"❌ Producto '{producto}' no existe."
                    conn.sendall((msg + "\n").encode('utf-8'))
                    log(nombre_cliente, f"Rechazado: {msg}")
                    with lock_stats: stats["rechazados"] += 1
                    continue

                if stock[producto] < cantidad:
                    msg = (f"❌ Stock insuficiente de '{producto}' "
                           f"(disponible: {stock[producto]}).")
                    conn.sendall((msg + "\n").encode('utf-8'))
                    log(nombre_cliente, f"Rechazado: {msg}")
                    with lock_stats: stats["rechazados"] += 1
                    continue

                if semaforo_cola._value == 0:
                    conn.sendall("⏳ Cola llena. Tu pedido espera...\n".encode('utf-8'))
                    log(nombre_cliente, "Cola llena, cliente espera...")

            semaforo_cola.acquire()

            with lock_pedidos:
                stock[producto] -= cantidad
                pedidos.append((producto, cantidad, nombre_cliente))
                with lock_stats: stats["recibidos"] += 1
                log("COLA", f"+Pedido: {producto} x{cantidad} de {nombre_cliente} "
                            f"| Cola: {len(pedidos)}/{MAX_COLA}")

            conn.sendall(f"✅ Pedido de {producto} x{cantidad} encolado.\n".encode('utf-8'))

    except Exception as e:
        log("SERVIDOR", f"Error con {nombre_cliente}: {e}")
    finally:
        conn.close()
        log("SERVIDOR", f"{nombre_cliente} desconectado.")


# ── HILOS PROCESADORES ─────────────────────────────────────────────────────────
def procesar_pedidos(procesador_id):
    """
    Igual al taller base pero usando log() para registrar cada acción.
    """
    nombre = f"PROCESADOR-{procesador_id}"
    log(nombre, "Listo. Esperando en barrera...")
    barrera_procesadores.wait()
    log(nombre, "¡Barrera cruzada! Iniciando procesamiento.")

    while True:
        with lock_pedidos:
            hay_suficientes = len(pedidos) >= MIN_PEDIDOS

        if not hay_suficientes:
            time.sleep(0.5)
            continue

        with lock_pedidos:
            if not pedidos:
                continue
            pedido = pedidos.pop(0)

        producto, cantidad, cliente = pedido
        log(nombre, f"Procesando: {producto} x{cantidad} de {cliente}")

        tiempo = random.randint(1, 5)
        time.sleep(tiempo)

        log(nombre, f"✅ Despachado: {producto} x{cantidad} ({tiempo}s)")

        with lock_stats:
            stats["procesados"] += 1

        semaforo_cola.release()


# ── SERVIDOR PRINCIPAL ─────────────────────────────────────────────────────────
def iniciar_servidor():
    # *** NUEVO: lanzar hilo logger PRIMERO ***
    threading.Thread(
        target=hilo_logger,
        daemon=True,
        name="Logger"
    ).start()

    log("SERVIDOR", "Logger iniciado.")

    for i in range(1, NUM_PROCESADORES + 1):
        threading.Thread(
            target=procesar_pedidos,
            args=(i,),
            daemon=True
        ).start()

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(10)

    log("SERVIDOR", f"Central activa en {HOST}:{PORT}")
    log("SERVIDOR", f"Stock inicial: {stock}")
    log("SERVIDOR", "Esperando clientes...")

    try:
        while True:
            conn, addr = srv.accept()
            threading.Thread(
                target=manejar_cliente,
                args=(conn, addr),
                daemon=True
            ).start()
    except KeyboardInterrupt:
        log("SERVIDOR", "Apagando...")
        log("SERVIDOR", f"Estadísticas finales: {stats}")
        srv.close()


if __name__ == '__main__':
    iniciar_servidor()
