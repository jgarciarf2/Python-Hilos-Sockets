"""
CLIENTE BALANCEADO — Se conecta vía balanceador
================================================
En lugar de conectarse directamente al servidor,
primero consulta al balanceador para saber a cuál ir.

FLUJO:
  1. Conectar al balanceador (puerto 9000)
  2. Recibir "REDIRECT:12345" o "REDIRECT:12346"
  3. Cerrar conexión con balanceador
  4. Abrir nueva conexión al puerto indicado
  5. Operar normalmente con ese servidor

CÓMO EJECUTAR:
  Abre varias terminales y ejecuta cliente_balanceado.py en cada una
  Observa cómo el balanceador distribuye entre A y B
"""

import socket
import random
import time

HOST_BALANCEADOR = 'localhost'
PUERTO_BALANCEADOR = 9000


def consultar_balanceador():
    """
    Pregunta al balanceador a qué servidor conectarse.
    Retorna el puerto del servidor elegido, o None si falla.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((HOST_BALANCEADOR, PUERTO_BALANCEADOR))

        # recibir respuesta: "REDIRECT:12345\n"
        respuesta = s.recv(1024).decode().strip()
        s.close()

        if respuesta.startswith("REDIRECT:"):
            puerto = int(respuesta.split(":")[1])
            print(f"[CLIENTE] Balanceador indica → puerto {puerto}")
            return puerto
        else:
            print(f"[CLIENTE] Error del balanceador: {respuesta}")
            return None

    except ConnectionRefusedError:
        print("[CLIENTE] Balanceador no disponible.")
        return None


def iniciar_cliente():
    """
    Consulta el balanceador y se conecta al servidor asignado.
    """
    print("[CLIENTE] Consultando balanceador...")

    # PASO 1: obtener puerto del servidor asignado
    puerto = consultar_balanceador()
    if not puerto:
        print("[CLIENTE] No se pudo obtener servidor. Abortando.")
        return

    # PASO 2: conectarse directamente al servidor asignado
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(('localhost', puerto))
        print(f"[CLIENTE] Conectado al servidor en puerto {puerto}\n")

        # recibir bienvenida del servidor
        info = s.recv(1024).decode()
        print(f"[SERVIDOR]: {info.strip()}\n")

        # PASO 3: enviar pedidos normalmente
        productos     = ['productoA', 'productoB', 'productoC']
        num_pedidos   = random.randint(1, 5)

        print(f"[CLIENTE] Enviando {num_pedidos} pedidos...\n")

        for i in range(num_pedidos):
            producto = random.choice(productos)
            cantidad = random.randint(1, 3)
            mensaje  = f"{producto},{cantidad}"

            print(f"[CLIENTE] Pedido {i+1}/{num_pedidos}: {mensaje}")
            s.sendall(mensaje.encode())
            respuesta = s.recv(1024).decode()
            print(f"[SERVIDOR]: {respuesta.strip()}\n")

            time.sleep(random.uniform(0.5, 1.5))

        # consultar stock al final para ver el estado
        s.sendall('/stock'.encode())
        stock_actual = s.recv(1024).decode()
        print(f"[SERVIDOR]: {stock_actual.strip()}")

    except ConnectionRefusedError:
        print(f"[CLIENTE] Servidor en puerto {puerto} no disponible.")
    except Exception as e:
        print(f"[CLIENTE] Error: {e}")
    finally:
        s.close()
        print("[CLIENTE] Desconectado.")


if __name__ == '__main__':
    iniciar_cliente()
