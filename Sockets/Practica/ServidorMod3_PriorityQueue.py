"""
MODIFICACIÓN 3: Cola con prioridad aleatoria y reordenamiento explícito
=======================================================================
CAMBIO PRINCIPAL:
  - Cada pedido recibe una prioridad ALEATORIA (1, 2 o 3) al momento
    de ser encolado, independientemente del producto solicitado.
  - La cola es una lista manual (igual que el taller base) pero después
    de cada inserción se ordena completa por prioridad con sort().
  - Los procesadores siempre sacan el pedido de menor número (mayor urgencia).

POR QUÉ ES DIFERENTE AL TALLER BASE:
  - Base:  pedidos.append(tupla) → FIFO estricto, sin orden.
  - Mod3:  pedidos.append(tupla) + pedidos.sort() → la cola se reordena
    cada vez que llega un pedido nuevo. Si llega uno con prioridad 1
    y hay 10 con prioridad 3 esperando, el nuevo queda de primero.

ESTRUCTURA DE LA TUPLA:
  (prioridad, timestamp, producto, cantidad, cliente)
  - prioridad:  entero aleatorio entre 1 y 3 (1=urgente, 3=bajo)
  - timestamp:  time.time() como desempate FIFO dentro de igual prioridad
  - producto, cantidad, cliente: igual al taller base

SINCRONIZADORES: semáforo, lock, barrera — igual al taller base.
"""

import socket
import threading
import random
import time

# ── Configuración ──────────────────────────────────────────────────────────────
HOST             = 'localhost'
PORT             = 12345
MAX_COLA         = 10
NUM_PROCESADORES = 5
MIN_PEDIDOS      = 5

# ── Stock compartido ───────────────────────────────────────────────────────────
stock = {
    'productoA': 50,
    'productoB': 30,
    'productoC': 20
}

# ── Cola compartida con prioridad ──────────────────────────────────────────────
# Lista de tuplas: (prioridad, timestamp, producto, cantidad, cliente)
# Se ordena por prioridad (y timestamp como desempate) después de cada inserción.
pedidos = []

# ── Sincronizadores ────────────────────────────────────────────────────────────

# Semáforo: limita pedidos simultáneos en cola (igual al taller base)
semaforo_cola = threading.Semaphore(MAX_COLA)

# Lock: protege pedidos[] y stock{} (igual al taller base)
lock_pedidos = threading.Lock()

# Barrera: los 5 procesadores esperan entre sí (igual al taller base)
barrera_procesadores = threading.Barrier(NUM_PROCESADORES)

# ── Estadísticas ───────────────────────────────────────────────────────────────
stats = {"recibidos": 0, "procesados": 0, "rechazados": 0}
lock_stats = threading.Lock()


# ── HILO CLIENTE ───────────────────────────────────────────────────────────────
def manejar_cliente(conn, addr):
    """
    Recibe pedidos del cliente y los inserta en la cola con prioridad aleatoria.

    DIFERENCIA CON TALLER BASE — solo estas dos líneas cambian:
      1. prioridad = random.randint(1, 3)   ← asignar prioridad al azar
      2. pedidos.sort(key=lambda x: (x[0], x[1]))  ← reordenar toda la cola

    Todo lo demás (semáforo, lock, stock, socket) es idéntico al base.
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

            # Verificar stock y espacio en cola
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

                # *** CAMBIO 1: asignar prioridad aleatoria al pedido ***
                # 1 = urgente, 2 = medio, 3 = bajo
                prioridad = random.randint(1, 3)

                # Timestamp como desempate: si dos pedidos tienen igual prioridad,
                # el que llegó primero (menor timestamp) va primero → FIFO dentro
                # de la misma prioridad.
                timestamp = time.time()

                # Insertar tupla con prioridad
                pedidos.append((prioridad, timestamp, producto, cantidad, nombre_cliente))

                # *** CAMBIO 2: reordenar toda la cola por prioridad ***
                # sort() es estable y ordena por el primer elemento (prioridad),
                # usando timestamp como desempate automático (segundo elemento).
                # Complejidad: O(n log n) — aceptable para colas pequeñas.
                pedidos.sort(key=lambda x: (x[0], x[1]))

                with lock_stats: stats["recibidos"] += 1

                # Mostrar estado actual de la cola tras reordenar
                resumen_cola = [(p[0], p[2]) for p in pedidos]  # (prioridad, producto)
                print(f"[COLA] +Pedido: {producto} x{cantidad} | "
                      f"Prioridad asignada: {prioridad} | "
                      f"Cola reordenada: {resumen_cola}")

            conn.sendall(
                f"✅ Pedido de {producto} x{cantidad} encolado "
                f"(prioridad {prioridad}).\n".encode('utf-8'))

    except Exception as e:
        print(f"[SERVIDOR] Error con {nombre_cliente}: {e}")
    finally:
        conn.close()
        print(f"[SERVIDOR] {nombre_cliente} desconectado.")


# ── HILOS PROCESADORES ─────────────────────────────────────────────────────────
def procesar_pedidos(procesador_id):
    """
    Procesador interno que siempre despacha el pedido de MAYOR PRIORIDAD.

    DIFERENCIA CON TALLER BASE:
      - Base: pedidos.pop(0) → saca el más antiguo (FIFO puro).
      - Mod3: pedidos.pop(0) → saca el primero de la lista, que gracias
              al sort() ya es el de mayor prioridad (menor número).

    El procesador NO necesita buscar el de mayor prioridad; el sort()
    en manejar_cliente garantiza que siempre esté en la posición 0.
    """
    print(f"[PROCESADOR-{procesador_id}] Listo. Esperando en barrera...")
    barrera_procesadores.wait()
    print(f"[PROCESADOR-{procesador_id}] ¡Barrera cruzada! Iniciando procesamiento.")

    while True:
        # Esperar suficientes pedidos acumulados antes de procesar
        with lock_pedidos:
            hay_suficientes = len(pedidos) >= MIN_PEDIDOS

        if not hay_suficientes:
            time.sleep(0.5)
            continue

        # Tomar el pedido de mayor prioridad (posición 0 tras el sort)
        with lock_pedidos:
            if not pedidos:
                continue
            pedido = pedidos.pop(0)   # siempre el de menor número = mayor urgencia

        prioridad, timestamp, producto, cantidad, cliente = pedido
        print(f"[PROCESADOR-{procesador_id}] Procesando (prioridad {prioridad}): "
              f"{producto} x{cantidad} de {cliente}")

        # Simular tiempo de despacho (1-5 segundos)
        tiempo = random.randint(1, 5)
        time.sleep(tiempo)

        print(f"[PROCESADOR-{procesador_id}] ✅ Despachado: "
              f"{producto} x{cantidad} ({tiempo}s) [prioridad {prioridad}]")

        with lock_stats:
            stats["procesados"] += 1

        # Liberar espacio en cola (corresponde al acquire de manejar_cliente)
        semaforo_cola.release()


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

    print(f"[SERVIDOR] Central de pedidos con PRIORIDAD ALEATORIA en {HOST}:{PORT}")
    print(f"[SERVIDOR] Stock inicial: {stock}")
    print(f"[SERVIDOR] Prioridades: 1=urgente | 2=medio | 3=bajo (asignadas al azar)")
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