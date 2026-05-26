"""
MODIFICACIÓN 2 - Cliente
========================
El cliente NO cambia. El cambio del rol del semáforo es interno al servidor.
El cliente sigue enviando pedidos aleatorios vía socket TCP.
"""

import socket
import random
import time

HOST = 'localhost'
PORT = 12345

def iniciar_cliente():
    print('[CLIENTE] Iniciando...')
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((HOST, PORT))

        data = s.recv(1024).decode()
        print(f'[SERVIDOR]: {data.strip()}')

        productos = ['productoA', 'productoB', 'productoC']
        num_pedidos = random.randint(1, 5)
        print(f'[CLIENTE] Se enviarán {num_pedidos} pedidos.')

        for i in range(num_pedidos):
            producto = random.choice(productos)
            cantidad = random.randint(1, 10)
            mensaje = f'{producto},{cantidad}'

            print(f'\n[CLIENTE] Enviando pedido {i+1}/{num_pedidos}: {mensaje}')
            s.sendall(mensaje.encode())

            respuesta = s.recv(1024).decode()
            print(f'[SERVIDOR]: {respuesta.strip()}')

            time.sleep(random.uniform(0.5, 2.0))

        print('\n[CLIENTE] Todos los pedidos enviados. Desconectando...')
        s.close()

    except ConnectionRefusedError:
        print('[ERROR] No se pudo conectar al servidor.')
    except Exception as e:
        print(f'[ERROR] {e}')

if __name__ == '__main__':
    iniciar_cliente()
