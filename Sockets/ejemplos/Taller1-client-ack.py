"""Ejemplo: cliente que espera ACK del servidor.

VARIACIÓN: Cliente ACK
PROPÓSITO: Cliente que muestra el ACK confirmación del servidor.

FLUJO ACK:
1. Cliente envía "Hola" al servidor.
2. Servidor recibe y responde "[ACK] recibido" al cliente.
3. Cliente imprime "[ACK] recibido" (confirmación).
4. Servidor reenvía a otro cliente.

DIFERENCIA vs base:
- Base: no hay confirmación especial.
- Esta: imprime ACK cuando llega (cliente base también lo vería, pero sin highlighting).

NOTA: El cliente NO hace nada especial, es el servidor quien envía ACK.
Este cliente simplemente recibe e imprime TODO, incluyendo los ACKs.

Ejecuta:
  python Taller1-client-ack.py
"""

from __future__ import annotations  # Anotaciones pospuestas.

import socket  # Módulo sockets para TCP/IP.
import threading  # Módulo de hilos.


# ===== CONSTANTES =====

HOST = "127.0.0.1"  # IP del servidor.
PORT = 50007  # Puerto del servidor.
ENCODING = "utf-8"  # Codificación de texto.
BUFFER_SIZE = 2048  # Tamaño máximo de recv().


# ===== FUNCIONES PRINCIPALES =====

def run_client() -> None:
    """Cliente que envía mensajes y recibe ACK del servidor.

    PROPÓSITO:
    Conecta al servidor y ejecuta chat bidireccional.
    El servidor responde con ACK ([ACK] recibido) después de cada mensaje.
    El cliente imprime TODO lo que recibe (incluyendo ACKs).

    FLUJO:
    1. Solicita nombre.
    2. Conecta al servidor.
    3. Inicia hilo receptor que imprime mensajes Y ACKs.
    4. Loop: lee input(), envía.
    5. Receptor imprime "[ACK] recibido" y luego mensajes del otro cliente.

    DIFERENCIA:
    - Base run_client(): cliente imprime todos los mensajes.
    - Esta: cliente imprime mensajes INCLUIDOS los ACKs del servidor.
    - El cliente es idéntico al base; la diferencia es en el SERVIDOR.
    """

    # name = input("Tu nombre: ").strip()
    # QUE HACE: Solicita nombre al usuario.
    name = input("Tu nombre: ").strip()

    # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    # QUE HACE: Context manager que crea y cierra socket TCP.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # sock.connect((HOST, PORT))
        # QUE HACE: Conecta el socket al servidor.
        sock.connect((HOST, PORT))
        
        # prompt = sock.recv(BUFFER_SIZE).decode(ENCODING)
        # QUE HACE: Recibe "NOMBRE: " del servidor.
        prompt = sock.recv(BUFFER_SIZE).decode(ENCODING)
        
        # if prompt.startswith("NOMBRE"):
        # QUE HACE: Verifica que sea solicitud de nombre.
        if prompt.startswith("NOMBRE"):
            # sock.sendall(f"{name}\n".encode(ENCODING))
            # QUE HACE: Envía nombre al servidor.
            sock.sendall(f"{name}\n".encode(ENCODING))

        # stop_event = threading.Event()
        # QUE HACE: Crea bandera para señalar parada.
        stop_event = threading.Event()
        
        # recv_thread = threading.Thread(...)
        # QUE HACE: Crea hilo que recibe mensajes (incluyendo ACKs).
        recv_thread = threading.Thread(
            target=_receiver,
            args=(sock, stop_event),
            daemon=True,
        )
        # recv_thread.start()
        # QUE HACE: Inicia el hilo receptor.
        recv_thread.start()

        # while not stop_event.is_set():
        # QUE HACE: Loop de chat hasta que se solicite parada.
        while not stop_event.is_set():
            try:
                # message = input()
                # QUE HACE: Lee línea del usuario desde el teclado.
                message = input()
            except EOFError:
                # message = "/exit"
                # QUE HACE: Si EOF, trata como salida.
                message = "/exit"
            
            # if not message:
            # QUE HACE: Ignora mensajes vacíos.
            if not message:
                # continue
                # QUE HACE: Salta a siguiente iteración.
                continue
            
            # sock.sendall(message.encode(ENCODING))
            # QUE HACE: Envía el mensaje al servidor (en claro).
            # - message.encode(ENCODING) = str a bytes (UTF-8).
            # - sendall() = envía todos los bytes.
            sock.sendall(message.encode(ENCODING))
            
            # if message.lower() == "/exit":
            # QUE HACE: Verifica si usuario escribió "/exit".
            if message.lower() == "/exit":
                # break
                # QUE HACE: Sale del loop.
                break
        
        # stop_event.set()
        # QUE HACE: Señaliza parada.
        stop_event.set()


def _receiver(sock: socket.socket, stop_event: threading.Event) -> None:
    """Hilo que recibe mensajes (incluyendo ACKs) del servidor.

    PROPÓSITO:
    Ejecuta en paralelo con el chat principal.
    Recibe TODOS los mensajes del servidor:
    - ACKs: "[ACK] recibido"
    - Mensajes: del otro cliente

    ORDEN TÍPICA:
    1. Cliente A envía "Hola".
    2. Servidor envía a A: "[ACK] recibido".
    3. Servidor envía a B: "A: Hola".

    Args:
        sock: Socket ya conectado.
        stop_event: Evento para señalar parada.
    """

    # while not stop_event.is_set():
    # QUE HACE: Loop mientras no haya señal de parada.
    while not stop_event.is_set():
        # try:
        # QUE HACE: Bloque de control de excepciones.
        try:
            # data = sock.recv(BUFFER_SIZE)
            # QUE HACE: Recibe datos del servidor (BLOQUEA).
            # - PUEDE SER: "[ACK] recibido" o "Alice: Hola\n" etc.
            data = sock.recv(BUFFER_SIZE)
            
            # if not data:
            # QUE HACE: Verifica si recv() retornó 0 bytes (servidor cerró).
            if not data:
                # break
                # QUE HACE: Sale del loop.
                break
            
            # print(data.decode(ENCODING), end="")
            # QUE HACE: Decodifica e imprime el mensaje.
            # - data.decode(ENCODING) = bytes a str (UTF-8).
            # - print(..., end="") = imprime sin salto de línea adicional.
            # - EJEMPLO:
            #   - data = b'[ACK] recibido' -> imprime "[ACK] recibido"
            #   - data = b'Bob: Hola\n' -> imprime "Bob: Hola\n"
            print(data.decode(ENCODING), end="")
        
        # except OSError:
        # QUE HACE: Captura errores de socket.
        except OSError:
            # break
            # QUE HACE: Sale del loop.
            break
    
    # stop_event.set()
    # QUE HACE: Asegura que stop_event esté activado al salir.
    stop_event.set()


def main() -> None:
    """Punto de entrada del cliente ACK.

    FLUJO:
    Llama a run_client() que maneja toda la conexión.
    """

    # run_client()
    # QUE HACE: Ejecuta el cliente.
    run_client()


# if __name__ == "__main__":
# QUE HACE: Verifica si se ejecuta directamente.
if __name__ == "__main__":
    # main()
    # QUE HACE: Llama a main().
    main()
