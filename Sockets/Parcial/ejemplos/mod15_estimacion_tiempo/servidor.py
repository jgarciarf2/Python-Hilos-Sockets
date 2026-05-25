"""
================================================================================
SERVIDOR - MODIFICACIÓN 15: NOTIFICACIÓN DE TIEMPO ESTIMADO DE ESPERA
================================================================================
DIFERENCIA PRINCIPAL:
    - Cuando un pedido es ingresado con éxito a la cola, el servidor calcula de forma
      dinámica y thread-safe una estimación de tiempo de procesamiento restante:
      `tiempo_estimado = (len(cola_pedidos) * TIEMPO_PROMEDIO_PROCESAMIENTO) / NUM_PROCESADORES`.
    - Este tiempo estimado se devuelve en la respuesta JSON al cliente.
    - Esto mejora enormemente la experiencia y demuestra cómo un sistema concurrente
      puede dar feedback del estado de sus colas internas a los clientes externos.
================================================================================
"""

import socket
import threading
import time
import random
import json

HOST = "127.0.0.1"
PORT = 65000
ENCODING = "utf-8"
BUFFER_SIZE = 4096
CAPACIDAD_MAXIMA_COLA = 10
NUM_PROCESADORES = 3
PEDIDOS_MINIMOS_PARA_BARRERA = 5
MAX_CLIENTES = 5
TIEMPO_PROMEDIO_PROCESAMIENTO = 3.0  # en segundos

stock_productos = {
    "Laptop": 10, "Mouse": 25, "Teclado": 20, "Monitor": 8
}

cola_pedidos = []
lock_cola = threading.Lock()
lock_stock = threading.Lock()
semaforo_capacidad = threading.Semaphore(CAPACIDAD_MAXIMA_COLA)
barrera_procesadores = threading.Barrier(NUM_PROCESADORES + 1)
evento_barrera_liberada = threading.Event()
contador_pedidos_totales = 0
evento_servidor_activo = threading.Event()
evento_servidor_activo.set()

def log(mensaje):
    nombre_hilo = threading.current_thread().name
    hora_actual = time.strftime("%H:%M:%S")
    print(f"[{hora_actual}] [{nombre_hilo}] {mensaje}")

def mostrar_stock():
    with lock_stock:
        print("\n        STOCK ACTUAL DE PRODUCTOS")
        for producto, cantidad in stock_productos.items():
            print(f"  {producto:<15} → {cantidad} unidades")

def obtener_lista_productos():
    with lock_stock:
        return list(stock_productos.keys())

def agregar_pedido_a_cola(pedido):
    global contador_pedidos_totales
    espacio_disponible = semaforo_capacidad.acquire(blocking=True, timeout=5)
    if not espacio_disponible:
        return False, 0.0

    with lock_cola:
        cola_pedidos.append(pedido)
        contador_pedidos_totales += 1
        total_actual = contador_pedidos_totales
        
        # Calcular tiempo de espera estimado bajo lock
        # Estimación simple: (elementos en cola * promedio_segundos) / hilos concurrentes
        tiempo_est = (len(cola_pedidos) * TIEMPO_PROMEDIO_PROCESAMIENTO) / NUM_PROCESADORES
        log(f"+ Pedido agregado. Cola: {len(cola_pedidos)} | Tiempo estimado: {tiempo_est:.1f}s")

    if total_actual >= PEDIDOS_MINIMOS_PARA_BARRERA and not evento_barrera_liberada.is_set():
        try:
            barrera_procesadores.wait()
        except threading.BrokenBarrierError:
            pass
        evento_barrera_liberada.set()

    return True, tiempo_est

def retirar_pedido_de_cola():
    with lock_cola:
        if len(cola_pedidos) > 0:
            pedido = cola_pedidos.pop(0)
            semaforo_capacidad.release()
            return pedido
        return None

def procesar_pedido(pedido):
    producto = pedido["producto"]
    cantidad = pedido["cantidad"]
    cliente = pedido["cliente"]

    time.sleep(random.uniform(2.0, 4.0))  # Simula el promedio configurado

    with lock_stock:
        if producto in stock_productos and stock_productos[producto] >= cantidad:
            stock_productos[producto] -= cantidad
            log(f"✓ DESPACHADO: {cantidad}x {producto} para {cliente}")
        else:
            log(f"✗ RECHAZADO por falta de stock: {producto} para {cliente}")

def hilo_procesador(id_procesador):
    try:
        barrera_procesadores.wait()
    except threading.BrokenBarrierError:
        pass

    while evento_servidor_activo.is_set() or len(cola_pedidos) > 0:
        pedido = retirar_pedido_de_cola()
        if pedido is not None:
            procesar_pedido(pedido)
        else:
            if not evento_servidor_activo.is_set():
                break
            time.sleep(1)

def atender_cliente(conexion_cliente, direccion_cliente, id_cliente):
    nombre_cliente = f"Cliente-{id_cliente}"
    try:
        lista_productos = obtener_lista_productos()
        mensaje_bienvenida = {
            "tipo": "bienvenida",
            "mensaje": f"Bienvenido {nombre_cliente}",
            "productos_disponibles": lista_productos,
            "tu_id": nombre_cliente
        }
        conexion_cliente.sendall(json.dumps(mensaje_bienvenida).encode(ENCODING))

        while True:
            datos_recibidos = conexion_cliente.recv(BUFFER_SIZE)
            if not datos_recibidos:
                break

            msg_datos = json.loads(datos_recibidos.decode(ENCODING))
            tipo_mensaje = msg_datos.get("tipo")

            if tipo_mensaje == "fin":
                break

            elif tipo_mensaje == "pedido":
                pedido = {
                    "producto": msg_datos["producto"],
                    "cantidad": msg_datos["cantidad"],
                    "cliente": nombre_cliente
                }
                exito, tiempo_espera = agregar_pedido_a_cola(pedido)
                respuesta = {
                    "tipo": "confirmacion" if exito else "error",
                    "mensaje": "En cola" if exito else "Cola llena",
                    "estado": "en_cola" if exito else "rechazado",
                    "tiempo_estimado": tiempo_espera
                }
                conexion_cliente.sendall(json.dumps(respuesta).encode(ENCODING))
    except Exception as e:
        log(f"Error con {nombre_cliente}: {e}")
    finally:
        conexion_cliente.close()

def iniciar_servidor():
    print("=" * 60)
    print("   SERVIDOR - MODIFICACIÓN 15 (Estimación de Tiempo)")
    print("=" * 60)
    mostrar_stock()

    hilos_procesadores = []
    for i in range(1, NUM_PROCESADORES + 1):
        hilo = threading.Thread(target=hilo_procesador, args=(i,), name=f"Procesador-{i}", daemon=True)
        hilo.start()
        hilos_procesadores.append(hilo)

    servidor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor_socket.bind((HOST, PORT))
    servidor_socket.listen(MAX_CLIENTES)

    hilos_clientes = []
    clientes_conectados = 0

    try:
        while clientes_conectados < MAX_CLIENTES:
            conexion, direccion = servidor_socket.accept()
            clientes_conectados += 1
            hilo_cliente = threading.Thread(
                target=atender_cliente,
                args=(conexion, direccion, clientes_conectados),
                name=f"Operador-Cliente-{clientes_conectados}",
                daemon=True
            )
            hilo_cliente.start()
            hilos_clientes.append(hilo_cliente)
    except KeyboardInterrupt:
        pass

    for hilo in hilos_clientes:
        hilo.join()

    evento_servidor_activo.clear()
    if not evento_barrera_liberada.is_set():
        barrera_procesadores.abort()

    for hilo in hilos_procesadores:
        hilo.join(timeout=10)

    servidor_socket.close()

if __name__ == "__main__":
    iniciar_servidor()
