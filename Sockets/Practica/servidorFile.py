"""
SERVIDOR — Central de Pedidos Distribuida
==========================================
Gestiona pedidos de clientes remotos usando:
  - Sockets TCP    → comunicación con clientes
  - Threading      → un hilo por cliente + 5 procesadores
  - Semaphore(10)  → limita capacidad máxima de la cola
  - Lock           → protege acceso a pedidos[] y stock{}
  - Barrier(5)     → sincroniza los 5 procesadores al inicio

FLUJO:
  1. Cliente conecta → servidor lanza hilo manejar_cliente
  2. manejar_cliente recibe pedidos → los encola si hay stock
  3. semaforo.acquire() se queda tomado mientras pedido está en cola
  4. 5 procesadores esperan en barrera hasta que todos estén listos
  5. Cada procesador espera que haya >= 5 pedidos antes de procesar
  6. Al procesar → semaforo.release() libera espacio en la cola

PROTOCOLO CLIENTE → SERVIDOR:
  Formato mensaje: "producto,cantidad"
  Ejemplo:         "productoA,2"
"""

import socket
import threading
import random
import time
from datetime import datetime   # para registrar la hora exacta de cada pedido

# ── configuración ──────────────────────────────
HOST              = 'localhost'
PORT              = 12345
MAX_COLA          = 10    # máximo de pedidos simultáneos en cola
NUM_PROCESADORES  = 5     # hilos procesadores internos
MIN_PEDIDOS       = 5     # mínimo de pedidos antes de procesar

# ── stock compartido ───────────────────────────
# diccionario: producto → cantidad disponible
stock = {
    'productoA': 50,
    'productoB': 30,
    'productoC': 20
}

# ── cola compartida de pedidos ─────────────────
# cada pedido es una tupla: (producto, cantidad, nombre_cliente)
pedidos = []

# ── herramientas de sincronización ────────────

# Semáforo: limita pedidos simultáneos en cola
# acquire() al AGREGAR → ocupa espacio
# release() al PROCESAR → libera espacio
semaforo_cola = threading.Semaphore(MAX_COLA)

# Lock: protege pedidos[] y stock{} contra acceso simultáneo
lock_pedidos = threading.Lock()

# Barrera: los 5 procesadores esperan hasta que todos estén listos
barrera_procesadores = threading.Barrier(NUM_PROCESADORES)

# ── estadísticas ───────────────────────────────
stats = {"recibidos": 0, "procesados": 0, "rechazados": 0}
lock_stats = threading.Lock()

# lock exclusivo para escritura al archivo de log
# sin esto, dos procesadores podrían escribir al mismo tiempo
# y las líneas quedarían mezcladas en el archivo
lock_log    = threading.Lock()
LOG_ARCHIVO = "pedidos.log"

# crear el archivo limpio al arrancar el servidor
with open(LOG_ARCHIVO, 'w') as f:
    f.write(f"=== LOG CENTRAL DE PEDIDOS — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n\n")


# ─────────────────────────────────────────────
# HILO CLIENTE — uno por cada cliente conectado
# ─────────────────────────────────────────────

def manejar_cliente(conn, addr):
    """
    Gestiona la comunicación con un cliente.
    Recibe pedidos y los encola si hay stock disponible.
    El semáforo se adquiere aquí y se libera en procesar_pedidos.
    """
    nombre_cliente = f"Cliente-{addr[1]}"   # identificar por puerto
    print(f"[SERVIDOR] {nombre_cliente} conectado desde {addr}")

    try:
        # informar al cliente qué productos hay
        info = f"Productos disponibles: {list(stock.keys())}\n"
        conn.sendall(info.encode())

        while True:
            datos = conn.recv(1024).decode().strip()
            if not datos:
                break

            # ── parsear mensaje ──
            try:
                producto, cantidad = datos.split(',')
                cantidad = int(cantidad.strip())
                producto = producto.strip()
            except ValueError:
                conn.sendall("❌ Formato incorrecto. Usa: producto,cantidad\n".encode())
                continue

            # ── verificar stock y encolar ──
            with lock_pedidos:
                if producto not in stock:
                    conn.sendall(f"❌ Producto '{producto}' no existe.\n".encode())
                    with lock_stats: stats["rechazados"] += 1
                    continue

                if stock[producto] < cantidad:
                    conn.sendall(f"❌ Stock insuficiente de '{producto}' (disponible: {stock[producto]}).\n".encode())
                    with lock_stats: stats["rechazados"] += 1
                    continue

                # verificar si la cola tiene espacio (sin bloquear el lock)
                # semaforo._value indica espacios disponibles
                if semaforo_cola._value == 0:
                    conn.sendall("⏳ Cola llena. Tu pedido espera...\n".encode())

            # acquire FUERA del lock para no bloquear otros hilos
            # si la cola está llena, este hilo espera aquí
            semaforo_cola.acquire()

            # agregar pedido a la cola (protegido por lock)
            with lock_pedidos:
                stock[producto] -= cantidad          # reservar stock
                pedidos.append((producto, cantidad, nombre_cliente))
                with lock_stats: stats["recibidos"] += 1
                print(f"[COLA] +Pedido: {producto} x{cantidad} de {nombre_cliente} | Cola: {len(pedidos)}/{MAX_COLA}")

            conn.sendall(f"✅ Pedido de {producto} x{cantidad} encolado.\n".encode())

    except Exception as e:
        print(f"[SERVIDOR] Error con {nombre_cliente}: {e}")
    finally:
        conn.close()
        print(f"[SERVIDOR] {nombre_cliente} desconectado.")


# ─────────────────────────────────────────────
# HILOS PROCESADORES — 5 hilos internos del servidor
# ─────────────────────────────────────────────

def procesar_pedidos(procesador_id):
    """
    Procesador interno que despacha pedidos de la cola.
    
    SINCRONIZACIÓN:
      1. Barrera al inicio → todos los procesadores arrancan juntos
      2. Espera hasta len(pedidos) >= MIN_PEDIDOS antes de procesar
      3. semaforo.release() al terminar cada pedido → libera espacio en cola
    """
    print(f"[PROCESADOR-{procesador_id}] Listo. Esperando en barrera...")

    # PASO 1: barrera — ningún procesador trabaja hasta que los 5 estén listos
    barrera_procesadores.wait()
    print(f"[PROCESADOR-{procesador_id}] ¡Barrera cruzada! Iniciando procesamiento.")

    while True:

        # PASO 2: esperar que haya suficientes pedidos acumulados
        # sleep(0.5) evita busy waiting → no consume CPU innecesariamente
        with lock_pedidos:
            hay_suficientes = len(pedidos) >= MIN_PEDIDOS

        if not hay_suficientes:
            time.sleep(0.5)    # espera sin bloquear el lock
            continue

        # PASO 3: tomar un pedido de la cola
        with lock_pedidos:
            if not pedidos:    # doble verificación: otro procesador pudo vaciarlo
                continue
            pedido = pedidos.pop(0)   # FIFO: primero en entrar, primero en salir

        producto, cantidad, cliente = pedido
        print(f"[PROCESADOR-{procesador_id}] Procesando: {producto} x{cantidad} de {cliente}")

        # PASO 4: simular tiempo de procesamiento (1-5 segundos)
        tiempo = random.randint(1, 5)
        time.sleep(tiempo)

        # PASO 5: pedido completado
        print(f"[PROCESADOR-{procesador_id}] ✅ Despachado: {producto} x{cantidad} ({tiempo}s)")

        with lock_stats:
            stats["procesados"] += 1

        # PASO 6: escribir en el log
        # with lock_log garantiza que solo 1 procesador escribe a la vez
        # 'a' = append → agrega al final sin borrar lo anterior
        # si usáramos 'w' cada procesador borraría el archivo completo
        with lock_log:
            with open(LOG_ARCHIVO, 'a') as f:
                hora  = datetime.now().strftime('%H:%M:%S')
                linea = (
                    f"[{hora}] PROCESADOR-{procesador_id} | "
                    f"Producto: {producto} | "
                    f"Cantidad: {cantidad} | "
                    f"Cliente: {cliente} | "
                    f"Tiempo: {tiempo}s\n"
                )
                f.write(linea)

        # PASO 6: liberar espacio en la cola → permite nuevo pedido
        # este release corresponde al acquire que hizo manejar_cliente
        semaforo_cola.release()


# ─────────────────────────────────────────────
# LOOP PRINCIPAL DEL SERVIDOR
# ─────────────────────────────────────────────

def iniciar_servidor():
    """
    Arranca los procesadores internos y acepta conexiones de clientes.
    """
    # lanzar los 5 procesadores ANTES de aceptar clientes
    # así están en la barrera cuando lleguen los primeros pedidos
    for i in range(1, NUM_PROCESADORES + 1):
        threading.Thread(
            target=procesar_pedidos,
            args=(i,),
            daemon=True    # mueren si el servidor principal termina
        ).start()

    # configurar socket del servidor
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
            # un hilo por cliente → atiende múltiples clientes simultáneamente
            threading.Thread(
                target=manejar_cliente,
                args=(conn, addr),
                daemon=True
            ).start()
    except KeyboardInterrupt:
        print("\n[SERVIDOR] Apagando servidor...")
        print(f"[SERVIDOR] Estadísticas finales: {stats}")

        # mostrar el log completo al cerrar
        print(f"\n[SERVIDOR] Contenido de {LOG_ARCHIVO}:")
        with open(LOG_ARCHIVO, 'r') as f:
            print(f.read())

        srv.close()


if __name__ == '__main__':
    iniciar_servidor()
