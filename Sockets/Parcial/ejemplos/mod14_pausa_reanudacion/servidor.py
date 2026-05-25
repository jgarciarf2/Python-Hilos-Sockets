"""
================================================================================
SERVIDOR - MODIFICACIÓN 14: PAUSA Y REANUDACIÓN DILATADA DEL PROCESAMIENTO
================================================================================
DIFERENCIA PRINCIPAL:
    - Este servidor implementa una variable de control `evento_pausa = threading.Event()`,
      la cual está inicialmente activa (set).
    - Los hilos procesadores (consumidores) verifican `evento_pausa.wait()` antes de
      retirar o procesar cada pedido.
    - Se lanza un hilo en el servidor que lee la consola. Si presionas ENTER en la consola
      del servidor, se alterna el estado (Pausa / Reanudación).
    - Durante la PAUSA, los clientes se pueden conectar y encolar pedidos normalmente,
      pero los procesadores no despacharán nada hasta que se reanude.
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

# Evento de control de pausa
evento_pausa = threading.Event()
evento_pausa.set()  # Por defecto corre libremente

def log(mensaje):
    nombre_hilo = threading.current_thread().name
    hora_actual = time.strftime("%H:%M:%S")
    print(f"[{hora_actual}] [{nombre_hilo}] {mensaje}")

def alternar_pausa():
    while evento_servidor_activo.is_set():
        try:
            input("\n[CONSOLA SERVIDOR] Presione ENTER para pausar/reanudar los procesadores...\n")
            if evento_pausa.is_set():
                evento_pausa.clear()
                log("⏸ PROCESADORES PAUSADOS. Los pedidos se acumularán en la cola, pero no serán despachados.")
            else:
                evento_pausa.set()
                log("▶ PROCESADORES REANUDADOS. Comenzando a despachar pedidos acumulados.")
        except (KeyboardInterrupt, EOFError):
            break

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
        return False

    with lock_cola:
        cola_pedidos.append(pedido)
        log(f"[COLA] Estado: {cola_pedidos}")
        contador_pedidos_totales += 1
        total_actual = contador_pedidos_totales
        log(f"+ Pedido agregado. Cola: {len(cola_pedidos)}")

    if total_actual >= PEDIDOS_MINIMOS_PARA_BARRERA and not evento_barrera_liberada.is_set():
        try:
            barrera_procesadores.wait()
        except threading.BrokenBarrierError:
            pass
        evento_barrera_liberada.set()

    return True

def retirar_pedido_de_cola():
    with lock_cola:
        if len(cola_pedidos) > 0:
            pedido = cola_pedidos.pop(0)
            log(f"[COLA] Estado: {cola_pedidos}")
            semaforo_capacidad.release()
            return pedido
        return None

def procesar_pedido(pedido):
    producto = pedido["producto"]
    cantidad = pedido["cantidad"]
    cliente = pedido["cliente"]

    time.sleep(random.randint(1, 3))

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
        # --- VERIFICAR PAUSA ---
        # Si está pausado (clear), el hilo se bloquea aquí hasta que se llame a .set()
        if not evento_pausa.is_set():
            log(f"⏳ Procesador-{id_procesador} esperando reanudación...")
            evento_pausa.wait()

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
                exito = agregar_pedido_a_cola(pedido)
                respuesta = {
                    "tipo": "confirmacion" if exito else "error",
                    "mensaje": "En cola (servidor pausado temporalmente)" if (exito and not evento_pausa.is_set()) else ("En cola" if exito else "Cola llena"),
                    "estado": "en_cola" if exito else "rechazado"
                }
                conexion_cliente.sendall(json.dumps(respuesta).encode(ENCODING))
    except Exception as e:
        log(f"Error con {nombre_cliente}: {e}")
    finally:
        conexion_cliente.close()

def iniciar_servidor():
    print("=" * 60)
    print("   SERVIDOR - MODIFICACIÓN 14 (Pausa y Reanudación)")
    print("=" * 60)
    mostrar_stock()

    hilos_procesadores = []
    for i in range(1, NUM_PROCESADORES + 1):
        hilo = threading.Thread(target=hilo_procesador, args=(i,), name=f"Procesador-{i}", daemon=True)
        hilo.start()
        hilos_procesadores.append(hilo)

    # Lanzar hilo lector de teclado para alternar pausa
    hilo_cons = threading.Thread(target=alternar_pausa, name="Consola-Servidor", daemon=True)
    hilo_cons.start()

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
    evento_pausa.set()  # Asegurar que se desbloquean
    if not evento_barrera_liberada.is_set():
        barrera_procesadores.abort()

    for hilo in hilos_procesadores:
        hilo.join(timeout=10)

    servidor_socket.close()

if __name__ == "__main__":
    iniciar_servidor()
