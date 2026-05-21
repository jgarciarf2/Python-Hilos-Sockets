"""Ejemplo: cliente para mensajes privados con DOCUMENTACIÓN EXHAUSTIVA.

PROPÓSITO:
- Se conecta al servidor como un cliente normal.
- El usuario puede usar el comando /whisper <nombre> <mensaje> para privados.
- El resto de mensajes se envían de forma pública.

Ejecuta:
  python Taller1-client-pvt.py
"""

from __future__ import annotations
import socket
import threading

HOST = "127.0.0.1"
PORT = 50007
ENCODING = "utf-8"
BUFFER_SIZE = 2048

def run_client() -> None:
    # Capturamos el nombre
    name = input("Tu nombre: ").strip()

    # Abrimos socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))
        
        # Handshake: El servidor pide el nombre
        prompt = sock.recv(BUFFER_SIZE).decode(ENCODING)
        if prompt.startswith("NOMBRE"):
            sock.sendall(f"{name}\n".encode(ENCODING))

        # Hilo para recibir mensajes asíncronos
        stop_event = threading.Event()
        threading.Thread(target=_receiver, args=(sock, stop_event), daemon=True).start()

        print(f"--- Chat Activo ---")
        print("Tip: Usa '/whisper <nombre> <mensaje>' para hablar en privado.")

        # Bucle de envío
        while not stop_event.is_set():
            try:
                msg = input()
            except EOFError:
                msg = "/exit"
                
            if not msg: continue
            
            sock.sendall(msg.encode(ENCODING))
            
            if msg.lower() == "/exit":
                break
        
        stop_event.set()

def _receiver(sock: socket.socket, stop_event: threading.Event) -> None:
    """Muestra los mensajes recibidos (públicos o privados)."""
    while not stop_event.is_set():
        try:
            data = sock.recv(BUFFER_SIZE)
            if not data: break
            print(data.decode(ENCODING), end="")
        except OSError:
            break
    stop_event.set()

if __name__ == "__main__":
    run_client()
