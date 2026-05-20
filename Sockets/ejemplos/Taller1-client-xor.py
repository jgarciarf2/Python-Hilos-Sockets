"""Ejemplo: cliente con cifrado XOR simple.

VARIACIÓN: Cliente XOR
PROPÓSITO: Cliente que encripta mensajes antes de enviarlos y los desencripta al recibirlos.

DIFERENCIA vs base:
- Base: envía/recibe en claro.
- Esta: usa xor_bytes() para encriptar antes de sendall(), desencriptar en recv().

FLUJO:
1. Envía nombre al servidor (en claro).
2. Hilo receptor: recibe bytes cifrados, xor_bytes() para desencriptar, imprime.
3. Main: lee input(), xor_bytes() para cifrar, sendall() cifrado.

Ejecuta:
  python Taller1-client-xor.py
"""

from __future__ import annotations  # Anotaciones pospuestas.

import socket  # Módulo sockets para TCP/IP.
import threading  # Módulo de hilos.


# ===== CONSTANTES =====

HOST = "127.0.0.1"  # IP del servidor.
PORT = 50007  # Puerto del servidor.
ENCODING = "utf-8"  # Codificación de texto.
BUFFER_SIZE = 2048  # Tamaño máximo de recv().
KEY = 37  # CLAVE: valor XOR para cifrado (arbitrario).
# - Diferencia: no existe en cliente base.
# - USO: cada byte se cifra/descifra con XOR 37.


# ===== FUNCIONES GLOBALES =====

def xor_bytes(data: bytes) -> bytes:
    """Aplica XOR simple a cada byte.

    PROPÓSITO: Cifra o descifra bytes.
    Como XOR es simétrico, aplicar 2 veces retorna original.

    Args:
        data: Bytes de entrada (cifrados o claros).

    Returns:
        Bytes con XOR aplicado (claros si entrada cifrada, cifrados si entrada clara).
    """

    # return bytes(b ^ KEY for b in data)
    # QUE HACE: Aplica XOR a cada byte.
    # - for b in data = itera sobre cada byte de data.
    # - b ^ KEY = XOR bitwise (operador ^).
    # - bytes(...) = convierte generador a bytes.
    return bytes(b ^ KEY for b in data)


# ===== FUNCIONES PRINCIPALES =====

def run_client() -> None:
    """Cliente que cifra/descifra con XOR antes de comunicarse.

    PROPÓSITO:
    Conecta al servidor y ejecuta chat bidireccional con encriptación XOR.

    FLUJO CIFRADO:
    1. Envía nombre al servidor (en claro).
    2. Inicia hilo receptor que descifra mensajes.
    3. Loop: lee input(), cifra, envía.
    4. Receptor: recibe cifrado, descifra, imprime.

    DIFERENCIA:
    - Base run_client(): todos los mensajes en claro.
    - Esta: usa xor_bytes() en sendall() y en receptor.
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
        # QUE HACE: Recibe "NOMBRE: " del servidor (en claro).
        prompt = sock.recv(BUFFER_SIZE).decode(ENCODING)
        
        # if prompt.startswith("NOMBRE"):
        # QUE HACE: Verifica que sea solicitud de nombre.
        if prompt.startswith("NOMBRE"):
            # sock.sendall(f"{name}\n".encode(ENCODING))
            # QUE HACE: Envía nombre al servidor (en claro, NO cifrado).
            sock.sendall(f"{name}\n".encode(ENCODING))

        # stop_event = threading.Event()
        # QUE HACE: Crea bandera para señalar parada.
        stop_event = threading.Event()
        
        # recv_thread = threading.Thread(...)
        # QUE HACE: Crea hilo que recibe mensajes cifrados.
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
                # QUE HACE: Lee línea del usuario (en claro).
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
            
            # sock.sendall(xor_bytes(message.encode(ENCODING)))
            # QUE HACE: Cifra y envía el mensaje.
            # - message.encode(ENCODING) = str a bytes (claro).
            # - xor_bytes(...) = cifra.
            # - sendall() = envía bytes cifrados.
            # - DIFERENCIA: base envía sin cifrar.
            sock.sendall(xor_bytes(message.encode(ENCODING)))
            
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
    """Hilo que recibe y descifra mensajes.

    PROPÓSITO:
    Ejecuta en paralelo con el chat principal.
    Recibe bytes cifrados del servidor.
    Descifra con xor_bytes() e imprime.

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
            # QUE HACE: Recibe bytes cifrados del servidor.
            data = sock.recv(BUFFER_SIZE)
            
            # if not data:
            # QUE HACE: Verifica si recv() retornó 0 bytes (servidor cerró).
            if not data:
                # break
                # QUE HACE: Sale del loop.
                break
            
            # print(xor_bytes(data).decode(ENCODING), end="")
            # QUE HACE: Descifra, decodifica e imprime.
            # - xor_bytes(data) = descifra (XOR es simétrico).
            # - decode(ENCODING) = bytes a str (UTF-8).
            # - print(..., end="") = imprime sin salto de línea.
            # - DIFERENCIA: base imprime sin descifrar.
            print(xor_bytes(data).decode(ENCODING), end="")
        
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
    """Punto de entrada del cliente XOR.

    FLUJO:
    Llama a run_client() que maneja toda la conexión y cifrado.
    """

    # run_client()
    # QUE HACE: Ejecuta el cliente cifrado.
    run_client()


# if __name__ == "__main__":
# QUE HACE: Verifica si se ejecuta directamente.
if __name__ == "__main__":
    # main()
    # QUE HACE: Llama a main().
    main()
