"""
SERVIDOR WORKER — Instancia reutilizable (A o B)
=================================================
Este servidor es idéntico al servidor_pedidos.py pero:
  1. Recibe su PUERTO como argumento → puede ser A o B
  2. Expone un PUERTO DE ESTADO → el balanceador lo consulta
  3. El puerto de estado responde SOLO con el número de pedidos en cola

CÓMO EJECUTAR:
  python servidor_worker.py 12345 A    → Servidor A
  python servidor_worker.py 12346 B    → Servidor B

PUERTOS:
  Servidor A: pedidos=12345, estado=12347
  Servidor B: pedidos=12346, estado=12348
"""

import socket
import threading
import random
import time
import sys
from datetime import datetime

# ── argumentos de línea de comandos ───────────
# sys.argv[1] = puerto de pedidos
# sys.argv[2] = nombre (A o B)
if len(sys.argv) < 3:
    print("Uso: python servidor_worker.py PUERTO NOMBRE")
    print("Ejemplo: python servidor_worker.py 12345 A")
    sys.exit(1)

PORT   = int(sys.argv[1])
NOMBRE = sys.argv[2]

# puerto de estado = puerto de pedidos + 2
# A: pedidos=12345, estado=12347
# B: pedidos=12346, estado=12348
PORT_ESTADO = PORT + 2

HOST             = 'localhost'
MAX_COLA         = 10
NUM_PROCESADORES = 3    # menos procesadores que el original → más diferencia de carga
MIN_PEDIDOS      = 3

# ── estructuras compartidas ────────────────────
stock = {'productoA': 50, 'productoB': 30, 'productoC': 20}
pedidos      = []
lock_pedidos = threading.Lock()
semaforo_cola = threading.Semaphore(MAX_COLA)
barrera       = threading.Barrier(NUM_PROCESADORES)
stats         = {"recibidos": 0, "procesados": 0}
lock_stats    = threading.Lock()


# ─────────────────────────────────────────────
# PUERTO DE ESTADO — responde la carga al balanceador
# ─────────────────────────────────────────────

def servidor_estado():
    """
    Servidor liviano que solo responde cuántos pedidos hay en cola.
    El balanceador lo consulta para decidir a quién enviar el cliente.

    DECISIÓN: hilo daemon separado del hilo principal
    → no interfiere con el procesamiento de pedidos
    → responde incluso cuando el servidor está muy ocupado
    """
    srv_estado = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv_estado.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv_estado.bind((HOST, PORT_ESTADO))
    srv_estado.listen(10)
    print(f"[{NOMBRE}] Puerto de estado activo en {PORT_ESTADO}")

    while True:
        try:
            conn, _ = srv_estado.accept()
            # leer carga dentro del lock → valor confiable
            with lock_pedidos:
                carga = len(pedidos)
            # responder solo el número → el balanceador lo parsea con int()
            conn.sendall(str(carga).encode())
            conn.close()
        except:
            break


# ─────────────────────────────────────────────
# HILO CLIENTE
# ─────────────────────────────────────────────

def manejar_cliente(conn, addr):
    nombre_cliente = f"Cliente-{addr[1]}"
    print(f"[{NOMBRE}] {nombre_cliente} conectado")

    try:
        info = f"[Servidor {NOMBRE}] Productos: {list(stock.keys())}\n"
        conn.sendall(info.encode())

        while True:
            datos = conn.recv(1024).decode().strip()
            if not datos:
                break

            # detectar comandos
            if datos.startswith('/'):
                if datos.lower() == '/stock':
                    with lock_pedidos:
                        respuesta = f"📦 Stock [{NOMBRE}]: {stock} | Cola: {len(pedidos)}/{MAX_COLA}\n"
                    conn.sendall(respuesta.encode())
                else:
                    conn.sendall(f"⚠️  Comando desconocido.\n".encode())
                continue

            # parsear pedido
            try:
                producto, cantidad = datos.split(',')
                cantidad = int(cantidad.strip())
                producto = producto.strip()
            except ValueError:
                conn.sendall("❌ Formato: producto,cantidad\n".encode())
                continue

            # verificar stock
            with lock_pedidos:
                if producto not in stock or stock[producto] < cantidad:
                    conn.sendall(f"❌ Sin stock de '{producto}'.\n".encode())
                    continue
                if semaforo_cola._value == 0:
                    conn.sendall("⏳ Cola llena. Esperando...\n".encode())

            semaforo_cola.acquire()

            with lock_pedidos:
                stock[producto] -= cantidad
                pedidos.append((producto, cantidad, nombre_cliente))
                with lock_stats: stats["recibidos"] += 1
                print(f"[{NOMBRE}] +Pedido: {producto} x{cantidad} | Cola: {len(pedidos)}/{MAX_COLA}")

            conn.sendall(f"✅ [{NOMBRE}] Pedido {producto} x{cantidad} encolado.\n".encode())

    except Exception as e:
        print(f"[{NOMBRE}] Error con {nombre_cliente}: {e}")
    finally:
        conn.close()


# ─────────────────────────────────────────────
# PROCESADORES
# ─────────────────────────────────────────────

def procesar_pedidos(pid):
    print(f"[{NOMBRE}] Procesador-{pid} esperando en barrera...")
    barrera.wait()
    print(f"[{NOMBRE}] Procesador-{pid} listo.")

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
        tiempo = random.randint(1, 5)
        time.sleep(tiempo)

        print(f"[{NOMBRE}] Procesador-{pid} ✅ {producto} x{cantidad} de {cliente} ({tiempo}s)")
        with lock_stats: stats["procesados"] += 1
        semaforo_cola.release()


# ─────────────────────────────────────────────
# INICIO
# ─────────────────────────────────────────────

def iniciar_worker():
    # hilo de estado → responde carga al balanceador
    threading.Thread(target=servidor_estado, daemon=True).start()

    # procesadores internos
    for i in range(1, NUM_PROCESADORES + 1):
        threading.Thread(target=procesar_pedidos, args=(i,), daemon=True).start()

    # socket principal de pedidos
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(10)

    print(f"[{NOMBRE}] Worker activo en puerto {PORT}")
    print(f"[{NOMBRE}] Stock: {stock}\n")

    try:
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=manejar_cliente, args=(conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        print(f"\n[{NOMBRE}] Cerrando. Stats: {stats}")
        srv.close()


if __name__ == '__main__':
    iniciar_worker()
