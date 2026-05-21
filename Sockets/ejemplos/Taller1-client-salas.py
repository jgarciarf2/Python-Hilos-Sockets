"""Ejemplo: cliente para salas (rooms) con DOCUMENTACIÓN EXHAUSTIVA línea por línea.

PROPÓSITO:
- Permite al usuario elegir una sala específica (ej: "Sistemas", "Profesores").
- Interactúa con el servidor de salas para realizar el handshake de dos pasos (Nombre + Sala).
- Separa la lógica de envío y recepción mediante hilos.

Ejecuta:
  python Taller1-client-salas.py
"""

# from __future__ import annotations
# - Proporciona compatibilidad con tipos de datos modernos.
from __future__ import annotations

# import socket
# - Permite la creación de sockets cliente para conectarse a servidores.
import socket

# import threading
# - Utilizado para manejar la recepción de mensajes en segundo plano.
import threading


# ==============================================================================
# CONFIGURACIÓN (Debe coincidir con la del Servidor)
# ==============================================================================

# HOST = "127.0.0.1"
# - Dirección IP del servidor.
HOST = "127.0.0.1"

# PORT = 50007
# - Puerto de comunicación.
PORT = 50007

# ENCODING = "utf-8"
# - Estándar de codificación de texto.
ENCODING = "utf-8"

# BUFFER_SIZE = 2048
# - Tamaño del buffer de recepción de bytes.
BUFFER_SIZE = 2048


def run_client() -> None:
    """Función principal del cliente que maneja la conexión y el ciclo de entrada."""

    # name = input("Tu nombre: ").strip()
    # - Captura el nombre del usuario desde la consola.
    name = input("Tu nombre: ").strip()
    
    # room = input("Sala a la que quieres entrar: ").strip()
    # - Captura el nombre de la sala (ej: "gaming", "estudio").
    room = input("Sala a la que quieres entrar: ").strip()

    # with socket.socket(...) as sock:
    # - Abre el socket del cliente (Context Manager asegura el cierre).
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # sock.connect((HOST, PORT))
        # - Intenta establecer la conexión TCP con el servidor.
        sock.connect((HOST, PORT))
        
        # prompt = sock.recv(BUFFER_SIZE).decode(ENCODING)
        # - Recibe la primera petición del servidor ("NOMBRE: ").
        prompt = sock.recv(BUFFER_SIZE).decode(ENCODING)
        
        # if prompt.startswith("NOMBRE"):
        # - Verifica que el protocolo esté siguiendo el orden esperado.
        if prompt.startswith("NOMBRE"):
            # sock.sendall(f"{name}\n".encode(ENCODING))
            # - Envía el nombre del usuario codificado en bytes.
            sock.sendall(f"{name}\n".encode(ENCODING))
            
        # prompt = sock.recv(BUFFER_SIZE).decode(ENCODING)
        # - Recibe la segunda petición del servidor ("SALA: ").
        prompt = sock.recv(BUFFER_SIZE).decode(ENCODING)
        
        # if prompt.startswith("SALA"):
        # - Verifica que el servidor esté pidiendo la sala.
        if prompt.startswith("SALA"):
            # sock.sendall(f"{room}\n".encode(ENCODING))
            # - Envía el identificador de la sala deseada.
            sock.sendall(f"{room}\n".encode(ENCODING))

        # stop_event = threading.Event()
        # - Objeto de sincronización para avisar a los hilos cuándo dejar de funcionar.
        stop_event = threading.Event()
        
        # recv_thread = threading.Thread(...)
        # - Crea el hilo que "escucha" lo que digan los demás en la sala.
        # - target: Función a ejecutar en paralelo.
        # - args: Argumentos necesarios.
        # - daemon=True: Si cerramos el programa principal, el hilo muere también.
        recv_thread = threading.Thread(
            target=_receiver,
            args=(sock, stop_event),
            daemon=True,
        )
        
        # recv_thread.start()
        # - Pone al hilo en marcha inmediatamente.
        recv_thread.start()

        # print(...)
        # - Informa al usuario que ya está listo para escribir.
        print(f"--- Conectado a la sala [{room}] ---")

        # while not stop_event.is_set():
        # - Bucle principal de envío de mensajes.
        while not stop_event.is_set():
            try:
                # message = input()
                # - Bloquea la consola esperando que el usuario escriba algo.
                message = input()
                
            except EOFError:
                # - Se activa si el usuario pulsa Ctrl+D o Ctrl+Z.
                message = "/exit"
                
            # if not message: continue
            # - Evita enviar cadenas vacías por accidente al pulsar Enter.
            if not message:
                continue
                
            # sock.sendall(message.encode(ENCODING))
            # - Envía el mensaje de texto al servidor para que lo reparta en la sala.
            sock.sendall(message.encode(ENCODING))
            
            # if message.lower() == "/exit":
            # - Rompe el bucle local si el usuario decide salir.
            if message.lower() == "/exit":
                break
                
        # stop_event.set()
        # - Activa la señal para detener el hilo receptor si aún siguiera vivo.
        stop_event.set()


def _receiver(sock: socket.socket, stop_event: threading.Event) -> None:
    """Función que corre en un hilo aparte para recibir mensajes del servidor.

    Args:
        sock: Socket activo con conexión al servidor.
        stop_event: Bandera para controlar el fin del hilo.
    """

    # while not stop_event.is_set():
    # - Itera mientras no se haya activado la señal de cierre.
    while not stop_event.is_set():
        try:
            # data = sock.recv(BUFFER_SIZE)
            # - Bloquea el hilo esperando datos entrantes.
            data = sock.recv(BUFFER_SIZE)
            
            # if not data:
            # - El servidor cerró la conexión si recibe 0 bytes.
            if not data:
                break
                
            # print(data.decode(ENCODING), end="")
            # - Imprime el mensaje decodificado. El final vacío evita saltos dobles innecesarios.
            print(data.decode(ENCODING), end="")
            
        except OSError:
            # - Captura errores de red o si el socket se cierra desde otro hilo.
            break
            
    # stop_event.set()
    # - Si el receptor detecta desconexión, avisa al hilo principal que también debe salir.
    stop_event.set()


def main() -> None:
    """Punto de entrada para el script del cliente."""

    # run_client()
    # - Ejecuta la lógica central.
    run_client()


# if __name__ == "__main__":
# - Estructura típica para ejecución directa en Python.
if __name__ == "__main__":
    # main()
    # - Inicia el cliente.
    main()

