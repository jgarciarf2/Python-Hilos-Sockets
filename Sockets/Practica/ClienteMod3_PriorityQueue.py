"""
MODIFICACIÓN 3 - Cliente con PriorityQueue
==========================================
CAMBIO RESPECTO AL CLIENTE BASE:
  - El cliente ahora muestra la prioridad del producto que está pidiendo,
    para que se pueda observar cómo el servidor los reordena.
  - Se puede configurar para enviar más pedidos de productoA (urgente)
    y ver que se despachan primero aunque lleguen después.

Sigue siendo un cliente TCP estándar; la lógica de prioridad
es transparente para él (el servidor es quien ordena).
"""

import socket
import random
import time

HOST = 'localhost'
PORT = 12345

# Mapa de prioridades (solo para mostrar en consola del cliente)
PRIORIDAD_DISPLAY = {
    'productoA': '🔴 urgente (1)',
    'productoB': '🟡 medio   (2)',
    'productoC': '🟢 bajo    (3)'
}

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

            # Mostrar prioridad del producto pedido
            prioridad_str = PRIORIDAD_DISPLAY.get(producto, 'desconocida')
            print(f'\n[CLIENTE] Enviando pedido {i+1}/{num_pedidos}: '
                  f'{mensaje} | Prioridad: {prioridad_str}')

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
