"""Ejemplo: cliente con cifrado XOR simple.

Ejecuta:
  python Taller1-client-xor.py
"""

from __future__ import annotations  # Anotaciones.

import socket  # Sockets.
import threading  # Hilos.


HOST = "127.0.0.1"  # IP.
PORT = 50007  # Puerto.
ENCODING = "utf-8"  # Codificacion.
BUFFER_SIZE = 2048  # Buffer.
KEY = 37  # Clave XOR.


def xor_bytes(data: bytes) -> bytes:
    """Aplica XOR a cada byte."""  # Doc.

    return bytes(b ^ KEY for b in data)  # XOR.


def run_client() -> None:
    """Cliente que cifra antes de enviar y descifra al recibir."""  # Doc.

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
            sock.sendall(xor_bytes(message.encode(ENCODING)))
            if message.lower() == "/exit":
                break
        stop_event.set()


def _receiver(sock: socket.socket, stop_event: threading.Event) -> None:
    """Recibe y descifra mensajes."""  # Doc.

    while not stop_event.is_set():
        try:
            data = sock.recv(BUFFER_SIZE)
            if not data:
                break
            print(xor_bytes(data).decode(ENCODING), end="")
        except OSError:
            break
    stop_event.set()


def main() -> None:
    run_client()


if __name__ == "__main__":
    main()
