"""
================================================================================
CLIENTE - MODIFICACIÓN 14: PAUSA Y REANUDACIÓN
================================================================================
DIFERENCIA PRINCIPAL:
    - Cliente estándar que envía pedidos. Podrás observar en la consola del servidor
      cómo al presionar ENTER se pausa el despacho, pero este cliente sigue recibiendo
      confirmaciones de "En cola" sin problemas, demostrando la separación entre
      recepción y procesamiento concurrente.
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
        
        log(f"Conectado como {mi_id}. Enviando pedidos...")

        for i in range(3):
            prod = random.choice(productos)
            pedido = {"tipo": "pedido", "producto": prod, "cantidad": 1}
            log(f"Enviando Pedido {i+1}: 1x {prod}...")
            cliente_socket.sendall(json.dumps(pedido).encode(ENCODING))
            
            resp_datos = cliente_socket.recv(BUFFER_SIZE)
            resp = json.loads(resp_datos.decode(ENCODING))
            log(f"Respuesta Servidor: {resp.get('mensaje')}")
            time.sleep(1.0)

        # Finalizar
        mensaje_fin = {"tipo": "fin"}
        cliente_socket.sendall(json.dumps(mensaje_fin).encode(ENCODING))

    except ConnectionRefusedError:
        log("Servidor apagado.")
    finally:
        cliente_socket.close()

if __name__ == "__main__":
    ejecutar_cliente()
