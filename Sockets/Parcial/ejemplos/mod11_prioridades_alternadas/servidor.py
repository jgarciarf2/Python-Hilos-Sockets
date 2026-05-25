"""
================================================================================
SERVIDOR - MODIFICACIÓN 11: PRIORIDADES ALTERNADAS (PREVENCIÓN DE INANICIÓN)
================================================================================
DIFERENCIA PRINCIPAL:
    - En lugar de atender estrictamente por prioridad (lo cual causaría inanición/starvation
      de los pedidos de menor prioridad si hay un flujo constante de prioridad alta),
      este servidor implementa una política de prioridades alternadas:
      Atiende hasta 5 pedidos de alta prioridad (Prioridad 1) consecutivamente,
      y luego, si existen pedidos de prioridad media (2) o baja (3), se ve obligado
      a procesar uno de ellos antes de continuar con la prioridad alta.
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
    "Laptop": 10, "Mouse": 25, "Teclado": 20, "Monitor": 8,
    "Auriculares": 15, "USB": 30, "Cargador": 18, "Webcam": 12
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

# Control de prioridades alternadas
consecutivos_alta = 0

def log(mensaje):
    nombre_hilo = threading.current_thread().name
    hora_actual = time.strftime("%H:%M:%S")
    print(f"[{hora_actual}] [{nombre_hilo}] {mensaje}")

def mostrar_stock():
    with lock_stock:
        print("\n" + "=" * 50)
        print("        STOCK ACTUAL DE PRODUCTOS")
        print("=" * 50)
        for producto, cantidad in stock_productos.items():
            print(f"  {producto:<15} → {cantidad} unidades")
        print("=" * 50 + "\n")

def obtener_lista_productos():
    with lock_stock:
        return list(stock_productos.keys())

def agregar_pedido_a_cola(pedido):
    global contador_pedidos_totales
    espacio_disponible = semaforo_capacidad.acquire(blocking=True, timeout=5)
    if not espacio_disponible:
        log(f"⚠ Cola llena. Pedido rechazado.")
        return False

    with lock_cola:
        # En esta mod, simplemente encolamos al final, el scheduler decide el orden al retirar
        cola_pedidos.append(pedido)
        log(f"[COLA] Estado: {cola_pedidos}")
        contador_pedidos_totales += 1
        total_actual = contador_pedidos_totales
        log(f"+ Pedido agregado (Pri {pedido['prioridad']}): {pedido['cantidad']}x {pedido['producto']}. Cola: {len(cola_pedidos)}")

    if total_actual >= PEDIDOS_MINIMOS_PARA_BARRERA and not evento_barrera_liberada.is_set():
        log(f"★ Se alcanzaron {PEDIDOS_MINIMOS_PARA_BARRERA} pedidos. Liberando procesadores...")
        try:
            barrera_procesadores.wait()
        except threading.BrokenBarrierError:
            pass
        evento_barrera_liberada.set()

    return True

def retirar_pedido_de_cola():
    """
    [MOD 11] Algoritmo de planificación de prioridades alternadas
    """
    global consecutivos_alta
    with lock_cola:
        if len(cola_pedidos) == 0:
            return None

        pedido_seleccionado = None
        idx_seleccionado = -1

        # Buscar si hay prioridades
        altas = [i for i, p in enumerate(cola_pedidos) if p["prioridad"] == 1]
        medias_bajas = [i for i, p in enumerate(cola_pedidos) if p["prioridad"] in (2, 3)]

        # Lógica de alternancia para prevenir inanición
        if consecutivos_alta >= 5 and len(medias_bajas) > 0:
            # Obligado a atender media/baja para evitar inanición
            idx_seleccionado = medias_bajas[0]
            pedido_seleccionado = cola_pedidos.pop(idx_seleccionado)
            log(f"[COLA] Estado: {cola_pedidos}")
            consecutivos_alta = 0
            log(f"🔄 [STARVATION CONTROL] Atendiendo pedido de prioridad {pedido_seleccionado['prioridad']} tras 5 consecutivos de alta.")
        elif len(altas) > 0:
            # Atender alta
            idx_seleccionado = altas[0]
            pedido_seleccionado = cola_pedidos.pop(idx_seleccionado)
            log(f"[COLA] Estado: {cola_pedidos}")
            consecutivos_alta += 1
        elif len(medias_bajas) > 0:
            # Atender media o baja
            idx_seleccionado = medias_bajas[0]
            pedido_seleccionado = cola_pedidos.pop(idx_seleccionado)
            log(f"[COLA] Estado: {cola_pedidos}")
            consecutivos_alta = 0

        if pedido_seleccionado:
            semaforo_capacidad.release()
            return pedido_seleccionado
        return None

def procesar_pedido(pedido):
    producto = pedido["producto"]
    cantidad = pedido["cantidad"]
    cliente = pedido["cliente"]
    prioridad = pedido["prioridad"]

    tiempo_procesamiento = random.randint(1, 3)
    log(f"⏳ Procesando pedido de {cliente} (Pri {prioridad}): {cantidad}x {producto}...")
    time.sleep(tiempo_procesamiento)

    with lock_stock:
        if producto in stock_productos and stock_productos[producto] >= cantidad:
            stock_productos[producto] -= cantidad
            log(f"✓ DESPACHADO: {cantidad}x {producto} para {cliente} (Prioridad: {prioridad})")
        else:
            log(f"✗ RECHAZADO por falta de stock: {producto} para {cliente}")

def hilo_procesador(id_procesador):
    log(f"Procesador-{id_procesador} iniciado.")
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
    log(f"Procesador-{id_procesador} finalizado.")

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

            pedido_datos = json.loads(datos_recibidos.decode(ENCODING))
            tipo_mensaje = pedido_datos.get("tipo")

            if tipo_mensaje == "fin":
                respuesta_fin = {"tipo": "fin_confirmado", "mensaje": "Gracias por su compra"}
                conexion_cliente.sendall(json.dumps(respuesta_fin).encode(ENCODING))
                break

            elif tipo_mensaje == "pedido":
                pedido = {
                    "producto": pedido_datos["producto"],
                    "cantidad": pedido_datos["cantidad"],
                    "prioridad": pedido_datos.get("prioridad", 3),
                    "cliente": nombre_cliente
                }
                exito = agregar_pedido_a_cola(pedido)
                respuesta = {
                    "tipo": "confirmacion" if exito else "error",
                    "mensaje": "En cola" if exito else "Cola llena",
                    "estado": "en_cola" if exito else "rechazado"
                }
                conexion_cliente.sendall(json.dumps(respuesta).encode(ENCODING))
    except Exception as e:
        log(f"Error con {nombre_cliente}: {e}")
    finally:
        conexion_cliente.close()

def iniciar_servidor():
    print("=" * 60)
    print("   SERVIDOR - MODIFICACIÓN 11 (Prioridades Alternadas)")
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
    mostrar_stock()

if __name__ == "__main__":
    iniciar_servidor()
