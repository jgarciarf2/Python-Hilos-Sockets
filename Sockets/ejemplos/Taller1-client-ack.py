"""Ejemplo: cliente que espera ACK del servidor.

Ejecuta:
  python Taller1-client-ack.py
"""

from __future__ import annotations  # Anotaciones.

import socket  # Sockets.
import threading  # Hilos.


HOST = "127.0.0.1"  # IP.
PORT = 50007  # Puerto.
ENCODING = "utf-8"  # Codificacion.
BUFFER_SIZE = 2048  # Buffer.


def run_client() -> None:
    """Envia mensaje y muestra ACK cuando llega.

    Flujo:
        - Solicita nombre.
        - Envia mensajes al servidor.
        - El hilo receptor imprime mensajes y ACK.
    """  # Doc.

    name = input("Tu nombre: ").strip()  # Nombre.

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))
        prompt = sock.recv(BUFFER_SIZE).decode(ENCODING)
        if prompt.startswith("NOMBRE"):
            sock.sendall(f"{name}\n".encode(ENCODING))

        stop_event = threading.Event()
        recv_thread = threading.Thread(
            target=_receiver,
            args=(sock, stop_event),
            daemon=True,
        )
        recv_thread.start()

        while not stop_event.is_set():
            try:
                message = input()
            except EOFError:
                message = "/exit"
            if not message:
                continue
            sock.sendall(message.encode(ENCODING))
            if message.lower() == "/exit":
                break
        stop_event.set()


def _receiver(sock: socket.socket, stop_event: threading.Event) -> None:
    """Recibe mensajes y ACK.

    Args:
        sock: Socket conectado.
        stop_event: Evento para detener el hilo.
    """  # Doc.

    while not stop_event.is_set():
        try:
            data = sock.recv(BUFFER_SIZE)
            if not data:
                break
            print(data.decode(ENCODING), end="")
        except OSError:
            break
    stop_event.set()


def main() -> None:
    """Punto de entrada del cliente con ACK."""

    run_client()


if __name__ == "__main__":
    main()
