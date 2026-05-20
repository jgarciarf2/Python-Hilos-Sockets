"""Ejemplo: cliente con envio automatico de frases.

Ejecuta:
  python Taller1-client-autosend.py
"""

from __future__ import annotations  # Anotaciones pospuestas.

import socket  # Sockets.
import threading  # Hilos.
import time  # Pausas.


HOST = "127.0.0.1"  # IP servidor.
PORT = 50007  # Puerto.
ENCODING = "utf-8"  # Codificacion.
BUFFER_SIZE = 2048  # Buffer.


def run_client_auto(host: str, port: int) -> None:
    """Cliente que envia mensajes de una lista con pausas.

    Args:
        host: IP o hostname del servidor.
        port: Puerto TCP del servidor.
    """  # Doc.

    name = input("Tu nombre: ").strip()  # Nombre.
    print(f"Conectando como {name}...")  # Log.

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:  # Socket.
        sock.connect((host, port))  # Conexion.
        prompt = sock.recv(BUFFER_SIZE).decode(ENCODING)  # Prompt.
        if prompt.startswith("NOMBRE"):  # Si pide nombre.
            sock.sendall(f"{name}\n".encode(ENCODING))  # Envia nombre.

        stop_event = threading.Event()  # Señal.
        recv_thread = threading.Thread(  # Hilo receptor.
            target=_client_receiver,
            args=(sock, stop_event),
            daemon=True,
        )
        recv_thread.start()  # Start.

        frases = [  # Frases de ejemplo.
            "Hola, soy un cliente automatico",
            "Puedo demostrar cambios rapidos",
            "Envio mensajes cada 1 segundo",
            "Si escriben /exit, me detengo",
        ]

        for frase in frases:  # Envia frases.
            if stop_event.is_set():  # Detener si hay salida.
                break
            sock.sendall(frase.encode(ENCODING))  # Envia.
            time.sleep(1)  # Pausa.

        sock.sendall(b"/exit")  # Cierra.
        stop_event.set()  # Señal.


def _client_receiver(sock: socket.socket, stop_event: threading.Event) -> None:
    """Recibe mensajes del servidor.

    Args:
        sock: Socket conectado.
        stop_event: Evento para detener el hilo receptor.
    """  # Doc.

    while not stop_event.is_set():  # Loop.
        try:
            data = sock.recv(BUFFER_SIZE)  # Recibe.
            if not data:  # Cierre.
                break
            print(data.decode(ENCODING), end="")  # Imprime.
        except OSError:
            break
    stop_event.set()  # Señal.


def main() -> None:
    """Punto de entrada del cliente automatico."""

    run_client_auto(HOST, PORT)  # Ejecuta.


if __name__ == "__main__":
    main()  # Entry.
