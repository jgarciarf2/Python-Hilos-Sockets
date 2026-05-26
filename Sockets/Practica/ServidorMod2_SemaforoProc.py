"""
MODIFICACIÓN 2: Cambiar el rol del Semáforo
============================================
CAMBIO PRINCIPAL:
  - ANTES: El semáforo limitaba cuántos PEDIDOS podían estar en la cola
    al mismo tiempo (capacidad máxima de la cola).
  - AHORA:  El semáforo limita cuántos PROCESADORES pueden trabajar
    simultáneamente (máximo MAX_PROCESADORES_ACTIVOS a la vez).

POR QUÉ ES DIFERENTE:
  - Semáforo como límite de cola: controla RECURSOS DE DATOS (espacio).
  - Semáforo como límite de trabajadores: controla RECURSOS DE CÓMPUTO
    (cuántos hilos pueden ejecutar una sección crítica al mismo tiempo).
  - Este segundo uso es el patrón clásico de "pool de trabajadores"
    (worker pool) donde el semáforo actúa como contador de permisos.

EJEMPLO CONCRETO:
  - MAX_PROCESADORES_ACTIVOS = 2 → aunque haya 5 procesadores,
    solo 2 pueden estar despachando pedidos al mismo tiempo.
  - Los otros 3 bloquean en semaforo_activos.acquire() hasta que
    uno de los 2 activos termine y haga release().

RESTO: cola ilimitada (queue.Queue), Lock, Barrier igual al taller base.
"""

import socket
import threading
import queue
import random
import time

# ── Configuración ──────────────────────────────────────────────────────────────
HOST                    = 'localhost'
PORT                    = 12345
NUM_PROCESADORES        = 5    # hilos procesadores totales
MAX_PROCESADORES_ACTIVOS = 2   # *** máximo procesando simultáneamente ***
MIN_PEDIDOS             = 5    # para la barrera

# ── Stock compartido ───────────────────────────────────────────────────────────
stock = {
    'productoA': 50,
    'productoB': 30,
    'productoC': 20
}

# ── Cola compartida (thread-safe por defecto) ──────────────────────────────────
# Usamos queue.Queue en lugar de lista manual: ya incluye sincronización interna.
# No necesitamos Lock para la cola, pero sí para el stock.
cola_pedidos = queue.Queue()   # sin límite de tamaño en este escenario

# ── Sincronizadores ────────────────────────────────────────────────────────────

# Lock: protege únicamente el diccionario stock{}
lock_stock = threading.Lock()

# Barrera: igual al taller base — los 5 procesadores esperan entre sí
barrera_procesadores = threading.Barrier(NUM_PROCESADORES)

# *** NUEVO ROL DEL SEMÁFORO ***
# Limita cuántos procesadores pueden estar ACTIVOS al mismo tiempo.
# acquire() → el procesador "pide permiso" para trabajar
# release() → el procesador "devuelve el permiso" al terminar
semaforo_activos = threading.Semaphore(MAX_PROCESADORES_ACTIVOS)

# ── Estadísticas ───────────────────────────────────────────────────────────────
stats = {"recibidos": 0, "procesados": 0, "rechazados": 0}
lock_stats = threading.Lock()


# ── HILO CLIENTE ───────────────────────────────────────────────────────────────
def manejar_cliente(conn, addr):
    """
    Recibe pedidos del cliente y los encola en cola_pedidos.
    En esta modificación el cliente NO interactúa con el semáforo;
    el semáforo ahora vive en el lado del procesador.
    """
    nombre_cliente = f"Cliente-{addr[1]}"
    print(f"[SERVIDOR] {nombre_cliente} conectado desde {addr}")

    try:
        with lock_stock:
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

            with lock_stock:
                if producto not in stock:
                    conn.sendall(f"❌ Producto '{producto}' no existe.\n".encode('utf-8'))
                    with lock_stats: stats["rechazados"] += 1
                    continue

                if stock[producto] < cantidad:
                    conn.sendall(
                        f"❌ Stock insuficiente de '{producto}' "
                        f"(disponible: {stock[producto]}).\n".encode('utf-8'))
                    with lock_stats: stats["rechazados"] += 1
                    continue

                # Reservar stock y encolar
                stock[producto] -= cantidad
                cola_pedidos.put((producto, cantidad, nombre_cliente))
                with lock_stats: stats["recibidos"] += 1
                print(f"[COLA] +Pedido: {producto} x{cantidad} de {nombre_cliente} "
                      f"| Cola aprox: {cola_pedidos.qsize()}")

            conn.sendall(f"✅ Pedido de {producto} x{cantidad} encolado.\n".encode('utf-8'))

    except Exception as e:
        print(f"[SERVIDOR] Error con {nombre_cliente}: {e}")
    finally:
        conn.close()
        print(f"[SERVIDOR] {nombre_cliente} desconectado.")


# ── HILOS PROCESADORES ─────────────────────────────────────────────────────────
def procesar_pedidos(procesador_id):
    """
    Procesador interno.

    FLUJO CON SEMÁFORO COMO CONTROL DE CONCURRENCIA:
      1. Barrera → todos los procesadores arrancan juntos (igual taller base).
      2. El procesador espera que haya ≥ MIN_PEDIDOS en la cola.
      3. *** NUEVO *** semaforo_activos.acquire() → pide "permiso" para trabajar.
         Si ya hay MAX_PROCESADORES_ACTIVOS trabajando, este hilo se bloquea aquí.
      4. Procesa el pedido.
      5. semaforo_activos.release() → devuelve el permiso, desbloquea a uno en espera.

    COMPARACIÓN CON TALLER BASE:
      - Base: semáforo controlaba espacio en cola (se adquiría al ENCOLAR).
      - Mod2: semáforo controla trabajadores activos (se adquiere al PROCESAR).
    """
    print(f"[PROCESADOR-{procesador_id}] Listo. Esperando en barrera...")
    barrera_procesadores.wait()
    print(f"[PROCESADOR-{procesador_id}] ¡Barrera cruzada! Iniciando.")

    while True:
        # Esperar suficientes pedidos acumulados
        if cola_pedidos.qsize() < MIN_PEDIDOS:
            time.sleep(0.5)
            continue

        # *** CLAVE: acquire del semáforo de trabajadores activos ***
        # Si ya hay MAX_PROCESADORES_ACTIVOS trabajando, este bloquea aquí
        semaforo_activos.acquire()
        print(f"[PROCESADOR-{procesador_id}] 🔒 Permiso obtenido "
              f"(activos ≤ {MAX_PROCESADORES_ACTIVOS})")

        try:
            # Tomar pedido de la cola (bloqueante si está vacía)
            try:
                pedido = cola_pedidos.get(timeout=1)
            except queue.Empty:
                continue   # si no hay pedido, liberar permiso y reintentar

            producto, cantidad, cliente = pedido
            print(f"[PROCESADOR-{procesador_id}] Procesando: "
                  f"{producto} x{cantidad} de {cliente}")

            tiempo = random.randint(1, 5)
            time.sleep(tiempo)

            print(f"[PROCESADOR-{procesador_id}] ✅ Despachado: "
                  f"{producto} x{cantidad} ({tiempo}s)")

            with lock_stats:
                stats["procesados"] += 1

            cola_pedidos.task_done()

        finally:
            # *** CLAVE: siempre liberar el permiso, incluso si hubo error ***
            semaforo_activos.release()
            print(f"[PROCESADOR-{procesador_id}] 🔓 Permiso liberado")


# ── SERVIDOR PRINCIPAL ─────────────────────────────────────────────────────────
def iniciar_servidor():
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

    print(f"[SERVIDOR] Central de pedidos activa en {HOST}:{PORT}")
    print(f"[SERVIDOR] Stock inicial: {stock}")
    print(f"[SERVIDOR] Procesadores totales: {NUM_PROCESADORES} | "
          f"Máx. activos simultáneos: {MAX_PROCESADORES_ACTIVOS}")
    print(f"[SERVIDOR] Esperando clientes...\n")

    try:
        while True:
            conn, addr = srv.accept()
            threading.Thread(
                target=manejar_cliente,
                args=(conn, addr),
                daemon=True
            ).start()
    except KeyboardInterrupt:
        print("\n[SERVIDOR] Apagando...")
        print(f"[SERVIDOR] Estadísticas finales: {stats}")
        srv.close()


if __name__ == '__main__':
    iniciar_servidor()
