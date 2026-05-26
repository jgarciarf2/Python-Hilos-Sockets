"""
CLIENTE DE PEDIDOS — Con soporte de comandos
=============================================
Cliente interactivo que permite:
  - Hacer pedidos: escribir  producto,cantidad
  - Consultar stock: escribir /stock
  - Ver ayuda: escribir /help
  - Salir: escribir salir

DECISIONES CLAVE:
  - is_comando() detecta si el mensaje empieza con '/'
    antes de enviarlo → mismo patrón que el servidor usa para detectarlo
  - Los comandos se envían igual que los pedidos (sendall)
    el servidor es quien los diferencia internamente
  - modo_auto=True → envía pedidos automáticos para pruebas rápidas
    modo_auto=False → modo interactivo para demostración manual

CÓMO EJECUTAR:
  Terminal 1: python servidor_pedidos.py
  Terminal 2: python cliente_pedidos.py
  Luego escribe: /stock, /help, productoA,3, salir
"""

import socket
import random
import time

HOST = 'localhost'
PORT = 12345


def es_comando(mensaje):
    """
    Identifica si el mensaje es un comando o un pedido normal.
    Los comandos empiezan con '/' → /stock, /help
    Los pedidos tienen formato → producto,cantidad
    
    Retorna True si es comando, False si es pedido normal.
    """
    return mensaje.strip().startswith('/')


def iniciar_cliente(modo_auto=False):
    """
    Conecta al servidor y permite enviar pedidos y comandos.

    Parámetros:
        modo_auto → True: envía pedidos automáticos (para pruebas)
                    False: modo interactivo (para sustentación)
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        s.connect((HOST, PORT))
        print(f"[CLIENTE] Conectado a {HOST}:{PORT}")
        print(f"[CLIENTE] Comandos: /stock  /help  producto,cantidad  salir\n")

        # recibir bienvenida del servidor
        info = s.recv(1024).decode()
        print(f"[SERVIDOR]: {info.strip()}\n")

        if modo_auto:
            # ── MODO AUTOMÁTICO: para probar rápidamente ──────────────
            productos = ['productoA', 'productoB', 'productoC']

            # consultar stock ANTES de hacer pedidos
            print("[CLIENTE] Consultando stock inicial...")
            s.sendall('/stock'.encode())
            respuesta = s.recv(1024).decode()
            print(f"[SERVIDOR]: {respuesta}")

            # hacer pedidos aleatorios
            num_pedidos = random.randint(1, 5)
            print(f"[CLIENTE] Enviando {num_pedidos} pedidos automáticos...\n")

            for i in range(num_pedidos):
                producto = random.choice(productos)
                cantidad = random.randint(1, 5)
                mensaje  = f"{producto},{cantidad}"

                print(f"[CLIENTE] Pedido {i+1}/{num_pedidos}: {mensaje}")
                s.sendall(mensaje.encode())
                respuesta = s.recv(1024).decode()
                print(f"[SERVIDOR]: {respuesta.strip()}\n")

                time.sleep(random.uniform(0.5, 1.5))

            # consultar stock DESPUÉS de hacer pedidos → evidencia cambios
            print("[CLIENTE] Consultando stock final...")
            s.sendall('/stock'.encode())
            respuesta = s.recv(1024).decode()
            print(f"[SERVIDOR]: {respuesta}")

        else:
            # ── MODO INTERACTIVO: para sustentación ───────────────────
            while True:
                mensaje = input(">>> ").strip()

                if not mensaje:
                    continue

                if mensaje.lower() == 'salir':
                    print("[CLIENTE] Desconectando...")
                    break

                # identificar si es comando o pedido antes de enviar
                if es_comando(mensaje):
                    print(f"[CLIENTE] Enviando comando: {mensaje}")
                else:
                    print(f"[CLIENTE] Enviando pedido: {mensaje}")

                s.sendall(mensaje.encode())
                respuesta = s.recv(1024).decode()
                print(f"[SERVIDOR]: {respuesta.strip()}\n")

    except ConnectionRefusedError:
        print("[ERROR] No se pudo conectar. ¿Está corriendo servidor_pedidos.py?")
    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        s.close()
        print("[CLIENTE] Conexión cerrada.")


if __name__ == '__main__':
    # cambiar a modo_auto=True para pruebas automáticas
    iniciar_cliente(modo_auto=False)
