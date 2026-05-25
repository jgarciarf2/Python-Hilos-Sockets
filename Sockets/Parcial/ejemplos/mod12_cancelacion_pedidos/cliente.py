"""
================================================================================
CLIENTE - MODIFICACIÓN 12: CANCELACIÓN DE PEDIDOS
================================================================================
DIFERENCIA PRINCIPAL:
    - Este cliente realiza un pedido, recibe su ID de cola, y luego envía una
      solicitud de cancelación para ver cómo el servidor lo saca de la lista.
================================================================================
"""

import socket
import time
import json
import random

HOST = "127.0.0.1"
PORT = 65000
ENCODING = "utf-8"
BUFFER_SIZE = 4096

def log(mensaje):
    hora_actual = time.strftime("%H:%M:%S")
    print(f"[{hora_actual}] [Cliente] {mensaje}")

def ejecutar_cliente():
    try:
        cliente_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cliente_socket.connect((HOST, PORT))
        
        datos_bienvenida = cliente_socket.recv(BUFFER_SIZE)
        if not datos_bienvenida:
            return

        bienvenida = json.loads(datos_bienvenida.decode(ENCODING))
        productos = bienvenida["productos_disponibles"]
        mi_id = bienvenida["tu_id"]
        
        log(f"Conectado como {mi_id}. Productos: {productos}")

        # Enviar primer pedido
        prod1 = random.choice(productos)
        pedido1 = {"tipo": "pedido", "producto": prod1, "cantidad": 2}
        log(f"Enviando Pedido 1: 2x {prod1}...")
        cliente_socket.sendall(json.dumps(pedido1).encode(ENCODING))
        
        resp1_datos = cliente_socket.recv(BUFFER_SIZE)
        resp1 = json.loads(resp1_datos.decode(ENCODING))
        pedido1_id = resp1.get("pedido_id")
        log(f"Respuesta Servidor: {resp1.get('mensaje')}")

        # Enviar segundo pedido
        prod2 = random.choice(productos)
        pedido2 = {"tipo": "pedido", "producto": prod2, "cantidad": 1}
        log(f"Enviando Pedido 2: 1x {prod2}...")
        cliente_socket.sendall(json.dumps(pedido2).encode(ENCODING))
        
        resp2_datos = cliente_socket.recv(BUFFER_SIZE)
        resp2 = json.loads(resp2_datos.decode(ENCODING))
        pedido2_id = resp2.get("pedido_id")
        log(f"Respuesta Servidor: {resp2.get('mensaje')}")

        # Intentar cancelar el Pedido 1
        if pedido1_id:
            time.sleep(0.5)
            log(f"❌ Intentando cancelar Pedido 1 (ID: {pedido1_id})...")
            cancelacion = {"tipo": "cancelacion", "pedido_id": pedido1_id}
            cliente_socket.sendall(json.dumps(cancelacion).encode(ENCODING))
            
            resp_canc_datos = cliente_socket.recv(BUFFER_SIZE)
            resp_canc = json.loads(resp_canc_datos.decode(ENCODING))
            log(f"Respuesta Cancelación: {resp_canc.get('mensaje')}")

        # Esperar un poco
        time.sleep(2.0)

        # Finalizar
        mensaje_fin = {"tipo": "fin"}
        cliente_socket.sendall(json.dumps(mensaje_fin).encode(ENCODING))

    except ConnectionRefusedError:
        log("Servidor apagado.")
    finally:
        cliente_socket.close()

if __name__ == "__main__":
    ejecutar_cliente()
