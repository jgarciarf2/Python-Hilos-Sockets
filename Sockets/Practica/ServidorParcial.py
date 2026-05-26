"""
Descripción: Desarrollar un sistema distribuido en Python que simule una central de pedidos, en la cual clientes remotos
(conectados vía socket) realizan pedidos y un conjunto de procesadores internos (hilos del servidor) quienes se encargan
de despachar. El sistema debe controlar el acceso, sincronización y procesamiento usando hilos, bloqueos, semáforos,
barreras y sockets. El servidor debe tener una lista de productos disponibles en stock con sus respectivas cantidades

Restricciones:
o Sockets TCP/IP:
• Los clientes (ejecuciones independientes) se conectan al servidor vía sockets.
• Cada cliente puede enviar una cantidad de "pedidos" (número aleatorio entre 1 y 5) como mensajes al
servidor (indicando adicionalmente sobre que producto está haciendo el pedido).
o Hilos:
• El servidor debe crear un hilo para gestionar cada cliente conectado (operadores procesadores). Además,
estos “operadores procesadores” se tardar un tiempo aleatorio de 1 a 5 segundos en procesar cada pedido.
o Cola compartida de pedidos:
• El servidor debe tener la capacidad de encolar los pedidos que van llegando desde los clientes.
o Semáforo (Semaphore):
• Se usa para limitar el número de pedidos que pueden estar simultáneamente en la cola (simulando capacidad
máxima del sistema).
o Bloqueo (Lock):
• Se debe usar para proteger el acceso a la cola de pedidos y evitar conflictos entre los hilos.
o Barrera (Barrier):
• Antes de comenzar el procesamiento, los "procesadores" deben sincronizarse con una barrera (por ejemplo,
que esperen a que haya al menos 5 pedidos acumulados antes de empezar a atenderlos).
"""
import socket
import threading
import random
import time

# configuración
HOST              = 'localhost'
PORT              = 12345
MAX_COLA          = 10    # máximo de pedidos simultáneos en cola
NUM_PROCESADORES  = 5     # hilos procesadores internos
MIN_PEDIDOS       = 5     # mínimo de pedidos antes de procesar

# stock compartido
# diccionario: producto → cantidad disponible
stock = {
    'productoA': 50,
    'productoB': 30,
    'productoC': 20
}

# cola compartida de pedidos
# cada pedido es una tupla: (producto, cantidad, nombre_cliente)
pedidos = []

# herramientas de sincronización

# Semáforo: limita pedidos simultáneos en cola
# acquire() al AGREGAR → ocupa espacio
# release() al PROCESAR → libera espacio
semaforo_cola = threading.Semaphore(MAX_COLA)

# Lock: protege pedidos[] y stock{} contra acceso simultáneo
lock_pedidos = threading.Lock()

# Barrera: los 5 procesadores esperan hasta que todos estén listos
barrera_procesadores = threading.Barrier(NUM_PROCESADORES)

# estadísticas
stats = {"recibidos": 0, "procesados": 0, "rechazados": 0}
lock_stats = threading.Lock()


# HILO CLIENTE — uno por cada cliente conectado

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
        info = f"Productos disponibles: {list(stock.keys())} | Cantidades: {list(stock.values())}\n"
        conn.sendall(info.encode('utf-8'))

        while True:
            datos = conn.recv(1024).decode('utf-8').strip()
            if not datos:
                break

            # parsear mensaje
            try:
                producto, cantidad = datos.split(',')
                cantidad = int(cantidad.strip())
                producto = producto.strip()
            except ValueError:
                conn.sendall("❌ Formato incorrecto. Usa: producto,cantidad\n".encode('utf-8'))
                continue

            # verificar stock y encolar
            with lock_pedidos:
                if producto not in stock:
                    conn.sendall(f"❌ Producto '{producto}' no existe.\n".encode('utf-8'))
                    with lock_stats: stats["rechazados"] += 1
                    continue

                if stock[producto] < cantidad:
                    conn.sendall(f"❌ Stock insuficiente de '{producto}' (disponible: {stock[producto]}).\n".encode('utf-8'))
                    with lock_stats: stats["rechazados"] += 1
                    continue

                # verificar si la cola tiene espacio (sin bloquear el lock)
                # semaforo._value indica espacios disponibles
                if semaforo_cola._value == 0:
                    conn.sendall("⏳ Cola llena. Tu pedido espera...\n".encode('utf-8'))

            # acquire FUERA del lock para no bloquear otros hilos
            # si la cola está llena, este hilo espera aquí
            semaforo_cola.acquire()

            # agregar pedido a la cola (protegido por lock)
            with lock_pedidos:
                stock[producto] -= cantidad          # reservar stock
                pedidos.append((producto, cantidad, nombre_cliente))
                with lock_stats: stats["recibidos"] += 1
                print(f"[COLA] +Pedido: {producto} x{cantidad} de {nombre_cliente} | Cola: {len(pedidos)}/{MAX_COLA}")

            conn.sendall(f"✅ Pedido de {producto} x{cantidad} encolado.\n".encode('utf-8'))

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
    # lanzar los 5 procesadores antes de aceptar clientes
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
        srv.close()


if __name__ == '__main__':
    iniciar_servidor()