"""
MODIFICACIÓN 1: Reemplazar Barrier por Event
=============================================
CAMBIO PRINCIPAL:
  - ANTES: threading.Barrier(5) hacía que los 5 procesadores esperaran
    entre sí hasta que todos estuvieran listos.
  - AHORA:  threading.Event() actúa como una "señal de disparo":
    un hilo SUPERVISOR acumula pedidos y cuando llega a MIN_PEDIDOS
    dispara evento.set(), desbloqueando a TODOS los procesadores al mismo tiempo.

POR QUÉ ES DIFERENTE:
  - La Barrier sincroniza N hilos entre sí (todos esperan a todos).
  - El Event sincroniza N hilos contra UNA condición externa (esperar señal).
  - Event es más flexible: puede resetearse con evento.clear() para
    volver a bloquear procesadores si la cola se vacía.

RESTO DEL SISTEMA: igual al taller base (semáforo, lock, cola, sockets).
"""

import socket
import threading
import random
import time

# ── Configuración ──────────────────────────────────────────────────────────────
HOST             = 'localhost'
PORT             = 12345
MAX_COLA         = 10   # capacidad máxima de la cola de pedidos
NUM_PROCESADORES = 5    # hilos procesadores internos
MIN_PEDIDOS      = 5    # pedidos mínimos para disparar el Event

# ── Stock compartido ───────────────────────────────────────────────────────────
stock = {
    'productoA': 50,
    'productoB': 30,
    'productoC': 20
}

# ── Cola compartida ────────────────────────────────────────────────────────────
pedidos = []   # lista de tuplas (producto, cantidad, cliente)

# ── Sincronizadores ────────────────────────────────────────────────────────────

# Semáforo: igual que el taller base — limita pedidos simultáneos en cola
semaforo_cola = threading.Semaphore(MAX_COLA)

# Lock: protege pedidos[] y stock{}
lock_pedidos = threading.Lock()

# *** NUEVO *** Event: reemplaza la Barrier
# - evento.wait() → bloquea hasta que alguien llame evento.set()
# - evento.set()  → desbloquea a TODOS los que estén en wait()
# - evento.clear()→ vuelve a bloquear (útil si queremos pausar y reanudar)
evento_inicio = threading.Event()

# ── Estadísticas ───────────────────────────────────────────────────────────────
stats = {"recibidos": 0, "procesados": 0, "rechazados": 0}
lock_stats = threading.Lock()


# ── HILO SUPERVISOR ────────────────────────────────────────────────────────────
def supervisor():
    """
    Hilo adicional que monitorea la cola cada 0.5 s.
    Cuando detecta MIN_PEDIDOS acumulados, dispara evento_inicio.set()
    para desbloquear a todos los procesadores simultáneamente.

    DIFERENCIA CON BARRIER:
      - La Barrier bloqueaba a los propios procesadores entre sí.
      - Aquí un hilo externo (supervisor) decide CUÁNDO arrancar,
        basándose en una condición del negocio (cantidad de pedidos).
    """
    print("[SUPERVISOR] Monitoreando cola. Esperando MIN_PEDIDOS...")
    while True:
        with lock_pedidos:
            total = len(pedidos)

        if total >= MIN_PEDIDOS and not evento_inicio.is_set():
            print(f"[SUPERVISOR] ✅ {total} pedidos acumulados → disparando Event.")
            evento_inicio.set()   # desbloquea todos los procesadores

        elif total == 0 and evento_inicio.is_set():
            # Opcional: resetear el evento si la cola se vació
            # (así los procesadores vuelven a esperar la próxima ráfaga)
            print("[SUPERVISOR] Cola vacía → reseteando Event.")
            evento_inicio.clear()

        time.sleep(0.5)   # poll cada medio segundo


# ── HILO CLIENTE ───────────────────────────────────────────────────────────────
def manejar_cliente(conn, addr):
    """
    Gestiona la comunicación con un cliente remoto.
    Recibe pedidos vía socket y los encola si hay stock y espacio en cola.
    """
    nombre_cliente = f"Cliente-{addr[1]}"
    print(f"[SERVIDOR] {nombre_cliente} conectado desde {addr}")

    try:
        info = (f"Productos disponibles: {list(stock.keys())} | "
                f"Cantidades: {list(stock.values())}\n")
        conn.sendall(info.encode('utf-8'))

        while True:
            datos = conn.recv(1024).decode('utf-8').strip()
            if not datos:
                break

            # Parsear "producto,cantidad"
            try:
                producto, cantidad = datos.split(',')
                cantidad = int(cantidad.strip())
                producto = producto.strip()
            except ValueError:
                conn.sendall("❌ Formato incorrecto. Usa: producto,cantidad\n".encode('utf-8'))
                continue

            # Verificar stock y estado de cola
            with lock_pedidos:
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

                if semaforo_cola._value == 0:
                    conn.sendall("⏳ Cola llena. Tu pedido espera...\n".encode('utf-8'))

            # Acquire semáforo FUERA del lock (evita deadlock)
            semaforo_cola.acquire()

            with lock_pedidos:
                stock[producto] -= cantidad
                pedidos.append((producto, cantidad, nombre_cliente))
                with lock_stats: stats["recibidos"] += 1
                print(f"[COLA] +Pedido: {producto} x{cantidad} de {nombre_cliente} "
                      f"| Cola: {len(pedidos)}/{MAX_COLA}")

            conn.sendall(f"✅ Pedido de {producto} x{cantidad} encolado.\n".encode('utf-8'))

    except Exception as e:
        print(f"[SERVIDOR] Error con {nombre_cliente}: {e}")
    finally:
        conn.close()
        print(f"[SERVIDOR] {nombre_cliente} desconectado.")


# ── HILOS PROCESADORES ─────────────────────────────────────────────────────────
def procesar_pedidos(procesador_id):
    """
    Procesador interno que despacha pedidos.

    FLUJO CON EVENT (diferente a Barrier):
      1. evento_inicio.wait() → el procesador duerme hasta que el
         SUPERVISOR dispare la señal (MIN_PEDIDOS acumulados).
      2. Una vez desbloqueado, procesa pedidos hasta que la cola se vacíe.
      3. Si el supervisor resetea el evento, vuelve a esperar en wait().

    Con Barrier todos los procesadores se esperaban entre sí;
    con Event todos esperan una señal externa independientemente.
    """
    print(f"[PROCESADOR-{procesador_id}] Listo. Esperando señal Event...")

    while True:
        # *** CLAVE *** Esperar la señal del supervisor
        evento_inicio.wait()
        print(f"[PROCESADOR-{procesador_id}] ¡Event recibido! Procesando...")

        # Intentar tomar un pedido de la cola
        with lock_pedidos:
            if not pedidos:
                time.sleep(0.3)
                continue
            pedido = pedidos.pop(0)   # FIFO

        producto, cantidad, cliente = pedido
        print(f"[PROCESADOR-{procesador_id}] Procesando: {producto} x{cantidad} de {cliente}")

        tiempo = random.randint(1, 5)
        time.sleep(tiempo)

        print(f"[PROCESADOR-{procesador_id}] ✅ Despachado: {producto} x{cantidad} ({tiempo}s)")

        with lock_stats:
            stats["procesados"] += 1

        # Liberar espacio en cola (corresponde al acquire de manejar_cliente)
        semaforo_cola.release()


# ── SERVIDOR PRINCIPAL ─────────────────────────────────────────────────────────
def iniciar_servidor():
    # Lanzar hilo supervisor (el que dispara el Event)
    threading.Thread(target=supervisor, daemon=True, name="Supervisor").start()

    # Lanzar procesadores (esperarán el Event)
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
