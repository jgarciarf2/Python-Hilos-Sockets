"""
================================================================================
SERVIDOR - MODIFICACIÓN 13: RESERVA DE STOCK INMEDIATA EN COLA
================================================================================
DIFERENCIA PRINCIPAL:
    - En lugar de descontar el stock al despachar (lo cual permite vender un producto
      en cola que ya no tiene stock real disponible en estantería), este servidor
      RESERVA el stock de forma INMEDIATA al agregar el pedido a la cola.
    - Si al encolar no hay stock real disponible, el pedido se rechaza de inmediato.
    - Si el pedido se procesa con éxito, la reserva se consolida.
    - Si el pedido se cancela o falla (aquí implementamos cancelación simple),
      las unidades reservadas se devuelven al stock para que otros clientes puedan
      adquirirlas.
================================================================================
"""

import socket
import threading
import time
import random
import json
import uuid

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
    
    # Intentar reservar stock primero (Exclusión mutua sobre stock)
    producto = pedido["producto"]
    cantidad = pedido["cantidad"]
    
    with lock_stock:
        if stock_productos.get(producto, 0) < cantidad:
            log(f"✗ RECHAZADO: No hay stock suficiente para reservar {cantidad}x {producto}.")
            return "sin_stock"
        
        # Descontar/Reservar inmediatamente
        stock_productos[producto] -= cantidad
        log(f"📋 RESERVA EXITOSA: {cantidad}x {producto} reservados. Stock restante: {stock_productos[producto]}")

    espacio_disponible = semaforo_capacidad.acquire(blocking=True, timeout=5)
    if not espacio_disponible:
        # Devolver stock si no pudimos encolar
        with lock_stock:
            stock_productos[producto] += cantidad
            log(f"🔄 DEVOLUCIÓN STOCK: Cola llena. {cantidad}x {producto} devueltos.")
        return "cola_llena"

    with lock_cola:
        cola_pedidos.append(pedido)
        log(f"[COLA] Estado: {cola_pedidos}")
        contador_pedidos_totales += 1
        total_actual = contador_pedidos_totales
        log(f"+ Pedido {pedido['pedido_id']} agregado con stock reservado. Cola: {len(cola_pedidos)}")

    if total_actual >= PEDIDOS_MINIMOS_PARA_BARRERA and not evento_barrera_liberada.is_set():
        try:
            barrera_procesadores.wait()
        except threading.BrokenBarrierError:
            pass
        evento_barrera_liberada.set()

    return "exito"

def retirar_pedido_de_cola():
    with lock_cola:
        if len(cola_pedidos) > 0:
            pedido = cola_pedidos.pop(0)
            log(f"[COLA] Estado: {cola_pedidos}")
            semaforo_capacidad.release()
            return pedido
        return None

def cancelar_pedido_y_devolver_stock(pedido_id):
    """
    Cancela un pedido en cola y libera el stock reservado.
    """
    with lock_cola:
        for idx, pedido in enumerate(cola_pedidos):
            if pedido["pedido_id"] == pedido_id:
                cola_pedidos.pop(idx)
                log(f"[COLA] Estado: {cola_pedidos}")
                semaforo_capacidad.release()
                
                # Devolver stock
                prod = pedido["producto"]
                cant = pedido["cantidad"]
                with lock_stock:
                    stock_productos[prod] += cant
                    log(f"✖ Pedido {pedido_id} CANCELADO. Devolviendo {cant}x {prod} al stock. Stock: {stock_productos[prod]}")
                return True
        return False

def procesar_pedido(pedido):
    # En este módulo, el stock YA fue reservado. Solo simulamos el despacho.
    producto = pedido["producto"]
    cantidad = pedido["cantidad"]
    cliente = pedido["cliente"]

    time.sleep(random.randint(1, 3))
    log(f"✓ DESPACHO CONSOLIDADO: {cantidad}x {producto} entregados a {cliente} (ID: {pedido['pedido_id']})")

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
                pedido_id = str(uuid.uuid4())[:8]
                pedido = {
                    "pedido_id": pedido_id,
                    "producto": msg_datos["producto"],
                    "cantidad": msg_datos["cantidad"],
                    "cliente": nombre_cliente
                }
                resultado = agregar_pedido_a_cola(pedido)
                if resultado == "exito":
                    respuesta = {"tipo": "confirmacion", "pedido_id": pedido_id, "mensaje": f"Reservado y en cola ID: {pedido_id}", "estado": "en_cola"}
                elif resultado == "sin_stock":
                    respuesta = {"tipo": "error", "mensaje": "Sin stock disponible para reserva", "estado": "rechazado"}
                else:
                    respuesta = {"tipo": "error", "mensaje": "Cola llena", "estado": "rechazado"}
                conexion_cliente.sendall(json.dumps(respuesta).encode(ENCODING))

            elif tipo_mensaje == "cancelacion":
                ped_id = msg_datos.get("pedido_id")
                exito_canc = cancelar_pedido_y_devolver_stock(ped_id)
                respuesta = {
                    "tipo": "cancelacion_respuesta",
                    "exito": exito_canc,
                    "mensaje": f"Pedido {ped_id} cancelado y stock liberado" if exito_canc else f"No se pudo cancelar"
                }
                conexion_cliente.sendall(json.dumps(respuesta).encode(ENCODING))
    except Exception as e:
        log(f"Error con {nombre_cliente}: {e}")
    finally:
        conexion_cliente.close()

def iniciar_servidor():
    print("=" * 60)
    print("   SERVIDOR - MODIFICACIÓN 13 (Reserva de Stock Inmediata)")
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
