"""
================================================================================
CLIENTE - MODIFICACIÓN 15: NOTIFICACIÓN DE TIEMPO ESTIMADO
================================================================================
DIFERENCIA PRINCIPAL:
    - Este cliente envía pedidos y lee el campo `tiempo_estimado` devuelto por el servidor,
      imprimiéndolo de manera atractiva para que el usuario conozca el retardo de cola.
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
            log(f"Enviando Pedido: 1x {prod}...")
            cliente_socket.sendall(json.dumps(pedido).encode(ENCODING))
            
            resp_datos = cliente_socket.recv(BUFFER_SIZE)
            resp = json.loads(resp_datos.decode(ENCODING))
            
            t_est = resp.get("tiempo_estimado", 0.0)
            log(f"Respuesta Servidor: {resp.get('mensaje')}")
            print(f"   ⏱ [ESTIMACIÓN] Su pedido tardará aproximadamente {t_est:.1f} segundos en procesarse.")
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
