"""Ejemplo: cliente que selecciona sala para chatear.

Ejecuta:
  python Taller1-client-salas.py
"""

from __future__ import annotations  # Anotaciones.

import socket  # Sockets.
import threading  # Hilos.


HOST = "127.0.0.1"  # IP.
PORT = 50007  # Puerto.
ENCODING = "utf-8"  # Codificacion.
BUFFER_SIZE = 2048  # Buffer.


def run_client() -> None:
    """Conecta, envia nombre y sala, y chatea."""  # Doc.

    name = input("Tu nombre: ").strip()  # Nombre.
    room = input("Sala: ").strip()  # Sala.

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:  # Socket.
        sock.connect((HOST, PORT))  # Conecta.
        prompt = sock.recv(BUFFER_SIZE).decode(ENCODING)  # Prompt nombre.
        if prompt.startswith("NOMBRE"):
            sock.sendall(f"{name}\n".encode(ENCODING))  # Envia nombre.
        prompt = sock.recv(BUFFER_SIZE).decode(ENCODING)  # Prompt sala.
        if prompt.startswith("SALA"):
            sock.sendall(f"{room}\n".encode(ENCODING))  # Envia sala.

        stop_event = threading.Event()  # Señal.
        recv_thread = threading.Thread(  # Hilo receptor.
            target=_receiver,
            args=(sock, stop_event),
            daemon=True,
        )
        recv_thread.start()  # Start.

        while not stop_event.is_set():  # Loop.
            try:
                message = input()
            except EOFError:
                message = "/exit"
            if not message:
                continue
            sock.sendall(message.encode(ENCODING))  # Envia.
            if message.lower() == "/exit":
                break
        stop_event.set()  # Señal.


def _receiver(sock: socket.socket, stop_event: threading.Event) -> None:
    """Recibe mensajes."""  # Doc.

    while not stop_event.is_set():
        try:
            data = sock.recv(BUFFER_SIZE)  # Recibe.
            if not data:
                break
            print(data.decode(ENCODING), end="")  # Imprime.
        except OSError:
            break
    stop_event.set()  # Señal.


def main() -> None:
    run_client()  # Ejecuta.


if __name__ == "__main__":
    main()  # Entry.
