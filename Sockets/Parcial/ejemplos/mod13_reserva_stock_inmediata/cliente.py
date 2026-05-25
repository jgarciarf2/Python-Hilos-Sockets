"""
================================================================================
CLIENTE - MODIFICACIÓN 13: RESERVA DE STOCK INMEDIATA
================================================================================
DIFERENCIA PRINCIPAL:
    - Envía pedidos y demuestra cómo se reserva el stock de forma inmediata.
    - También puede cancelar pedidos para forzar la devolución del stock al servidor.
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

        # Enviar pedido grande para reservar
        prod = random.choice(productos)
        pedido = {"tipo": "pedido", "producto": prod, "cantidad": 4}
        log(f"Enviando Pedido de Reserva: 4x {prod}...")
        cliente_socket.sendall(json.dumps(pedido).encode(ENCODING))
        
        resp_datos = cliente_socket.recv(BUFFER_SIZE)
        resp = json.loads(resp_datos.decode(ENCODING))
        pedido_id = resp.get("pedido_id")
        log(f"Respuesta Servidor: {resp.get('mensaje')}")

        if pedido_id:
            # Esperar un momento
            time.sleep(1.0)
            log(f"❌ Cancelando pedido {pedido_id} para liberar stock reservado...")
            cancelacion = {"tipo": "cancelacion", "pedido_id": pedido_id}
            cliente_socket.sendall(json.dumps(cancelacion).encode(ENCODING))
            
            resp_canc_datos = cliente_socket.recv(BUFFER_SIZE)
            resp_canc = json.loads(resp_canc_datos.decode(ENCODING))
            log(f"Respuesta Cancelación: {resp_canc.get('mensaje')}")

        time.sleep(1.5)
        # Finalizar
        mensaje_fin = {"tipo": "fin"}
        cliente_socket.sendall(json.dumps(mensaje_fin).encode(ENCODING))

    except ConnectionRefusedError:
        log("Servidor apagado.")
    finally:
        cliente_socket.close()

if __name__ == "__main__":
    ejecutar_cliente()
