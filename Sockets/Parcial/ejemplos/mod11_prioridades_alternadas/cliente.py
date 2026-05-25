"""
================================================================================
CLIENTE - MODIFICACIÓN 11: PRIORIDADES ALTERNADAS
================================================================================
DIFERENCIA PRINCIPAL:
    - Este cliente envía pedidos con prioridades aleatorias (1=Alta, 2=Media, 3=Baja)
      para permitir que el servidor pruebe la alternancia de atención.
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

        # Enviar 4 pedidos con prioridad aleatoria
        for i in range(1, 5):
            producto = random.choice(productos)
            cantidad = random.randint(1, 3)
            prioridad = random.choice([1, 2, 3])  # 1=Alta, 2=Media, 3=Baja
            
            pedido = {
                "tipo": "pedido",
                "producto": producto,
                "cantidad": cantidad,
                "prioridad": prioridad
            }
            
            log(f"Enviando pedido: {cantidad}x {producto} (Prioridad: {prioridad})...")
            cliente_socket.sendall(json.dumps(pedido).encode(ENCODING))
            
            respuesta_datos = cliente_socket.recv(BUFFER_SIZE)
            if respuesta_datos:
                resp = json.loads(respuesta_datos.decode(ENCODING))
                log(f"Respuesta del Servidor: {resp['mensaje']}")

            time.sleep(random.uniform(0.5, 1.5))

        # Finalizar
        mensaje_fin = {"tipo": "fin"}
        cliente_socket.sendall(json.dumps(mensaje_fin).encode(ENCODING))
        
        datos_fin = cliente_socket.recv(BUFFER_SIZE)
        if datos_fin:
            resp_fin = json.loads(datos_fin.decode(ENCODING))
            log(f"Cierre de sesión: {resp_fin['mensaje']}")

    except ConnectionRefusedError:
        log("Servidor apagado.")
    finally:
        cliente_socket.close()

if __name__ == "__main__":
    ejecutar_cliente()
