"""
================================================================================
CLIENTE - MODIFICACIÓN 16: HEARTBEAT / KEEP-ALIVE
================================================================================
DIFERENCIA PRINCIPAL:
    - Este cliente corre un hilo secundario (`daemon=True`) que envía un latido
      `{"tipo": "heartbeat"}` al servidor cada 2 segundos.
    - Esto evita que el socket de comunicación se cierre por inactividad.
    - Si detienes o bloqueas este cliente (p. ej., matando el hilo de latido),
      podrás ver en la consola del servidor cómo a los 6 segundos de silencio
      el servidor cancela la sesión.
================================================================================
"""

import socket
import time
import json
import random
import threading

HOST = "127.0.0.1"
PORT = 65000
ENCODING = "utf-8"
BUFFER_SIZE = 4096

evento_terminar = threading.Event()
lock_socket_envio = threading.Lock()  # Protege escrituras concurrentes del socket

def log(mensaje):
    hora_actual = time.strftime("%H:%M:%S")
    print(f"[{hora_actual}] [Cliente] {mensaje}")

def hilo_heartbeat(cliente_socket):
    while not evento_terminar.is_set():
        time.sleep(2.0)
        if evento_terminar.is_set():
            break
        
        try:
            hb_msg = {"tipo": "heartbeat"}
            with lock_socket_envio:
                cliente_socket.sendall(json.dumps(hb_msg).encode(ENCODING))
            # Opcional: leer el ACK si la aplicación lo requiere, pero
            # en este diseño simple el servidor lo responde y el buffer se vacía
        except Exception:
            break

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
        
        log(f"Conectado como {mi_id}. Iniciando hilo de Heartbeats...")

        # Lanzar hilo de latidos
        t_hb = threading.Thread(target=hilo_heartbeat, args=(cliente_socket,), daemon=True)
        t_hb.start()

        # Enviar dos pedidos con demoras largas de 4 segundos
        # Sin heartbeats, el servidor nos habría desconectado a los 6 segundos de inactividad
        for i in range(2):
            prod = random.choice(productos)
            pedido = {"tipo": "pedido", "producto": prod, "cantidad": 1}
            log(f"Enviando Pedido {i+1}: 1x {prod}...")
            with lock_socket_envio:
                cliente_socket.sendall(json.dumps(pedido).encode(ENCODING))
            
            resp_datos = cliente_socket.recv(BUFFER_SIZE)
            resp = json.loads(resp_datos.decode(ENCODING))
            log(f"Respuesta Servidor: {resp.get('mensaje')}")
            
            log("Esperando 4 segundos antes del próximo pedido (los heartbeats mantendrán viva la conexión)...")
            time.sleep(4.0)

        # Finalizar
        evento_terminar.set()
        time.sleep(0.5)
        mensaje_fin = {"tipo": "fin"}
        with lock_socket_envio:
            cliente_socket.sendall(json.dumps(mensaje_fin).encode(ENCODING))

    except ConnectionRefusedError:
        log("Servidor apagado.")
    finally:
        evento_terminar.set()
        cliente_socket.close()

if __name__ == "__main__":
    ejecutar_cliente()
