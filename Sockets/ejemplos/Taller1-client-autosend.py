"""Ejemplo: cliente con envío automático de frases.

VARIACIÓN: AutoSend
PROPÓSITO: Demostrar un cliente que envía una lista predefinida de mensajes
automáticamente (sin requerir entrada del usuario).

DIFERENCIA CLAVE vs base:
- Base: cliente interactivo, usuario escribe input().
- Esta: cliente automatizado, envía frases de una lista con pausas.
- Beneficio: Simular un cliente "bot" que genera conversación sin intervención.

CASO DE USO:
- Testing: validar que el servidor recibe múltiples mensajes.
- Demostración: mostrar que el sistema funciona sin entrada manual.
- Carga: generar carga predecible de mensajes.

Ejecuta:
  python Taller1-client-autosend.py
"""

from __future__ import annotations  # Anotaciones pospuestas (ej: list[str]).

import socket  # Módulo sockets para TCP/IP.
import threading  # Módulo de hilos (Thread, Event).
import time  # time.sleep() para pausas.


# ===== CONSTANTES =====
# QUE HACE ESTA SECCIÓN: Define valores fijos usados en la aplicación.

HOST = "127.0.0.1"  # IP del servidor.
# - "127.0.0.1" = localhost.

PORT = 50007  # Puerto del servidor.
# - 50007 = número de puerto.

ENCODING = "utf-8"  # Codificación de texto.
# - "utf-8" = estándar para textos.

BUFFER_SIZE = 2048  # Tamaño máximo de recv().
# - 2048 bytes = ~2 KB por mensaje.


def run_client_auto(host: str, port: int) -> None:
    """Cliente que envía mensajes de una lista con pausas (totalmente automatizado).

    PROPÓSITO:
    Conecta al servidor y envía una serie predefinida de frases automáticamente.
    Cada frase se envía con 1 segundo de pausa entre ellas.
    Al final, envía "/exit" para cerrar la conexión.
    Mientras, un hilo receptor imprime mensajes entrantes.

    DIFERENCIA:
    - Base run_client(): espera input() del usuario (interactivo).
    - Esta run_client_auto(): envía frases de una lista (automatizado).

    FLUJO:
    1. Solicita nombre al usuario (solo una vez).
    2. Crea socket y se conecta.
    3. Recibe "NOMBRE: " y envía nombre.
    4. Inicia hilo receptor en paralelo.
    5. Loop: envía cada frase, espera 1 seg.
    6. Envía "/exit" y cierra.

    Args:
        host: IP o hostname del servidor (ej: "127.0.0.1").
        port: Puerto TCP del servidor (ej: 50007).
    """

    # name = input("Tu nombre: ").strip()
    # QUE HACE: Solicita nombre al usuario (solo una vez, no por cada mensaje).
    # - input("Tu nombre: ") = pide nombre en consola.
    # - .strip() = elimina espacios al inicio/final.
    # - RESULTADO: name = string con el nombre.
    name = input("Tu nombre: ").strip()
    
    # print(f"Conectando como {name}...")
    # QUE HACE: Imprime log de conexión.
    print(f"Conectando como {name}...")

    # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    # QUE HACE: Context manager que crea y cierra socket TCP.
    # - socket.AF_INET = IPv4.
    # - socket.SOCK_STREAM = TCP.
    # - with = asegura que close() se ejecute al salir.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # sock.connect((host, port))
        # QUE HACE: Conecta el socket al servidor.
        # - (host, port) = ("127.0.0.1", 50007).
        sock.connect((host, port))
        
        # prompt = sock.recv(BUFFER_SIZE).decode(ENCODING)
        # QUE HACE: Recibe el primer mensaje del servidor ("NOMBRE: ").
        # - recv(BUFFER_SIZE) = recibe hasta 2048 bytes.
        # - decode(ENCODING) = convierte bytes a str.
        prompt = sock.recv(BUFFER_SIZE).decode(ENCODING)
        
        # if prompt.startswith("NOMBRE"):
        # QUE HACE: Verifica que el mensaje sea solicitud de nombre.
        if prompt.startswith("NOMBRE"):
            # sock.sendall(f"{name}\n".encode(ENCODING))
            # QUE HACE: Envía el nombre al servidor.
            # - f"{name}\n" = formatea nombre + salto de línea.
            # - encode(ENCODING) = convierte str a bytes.
            sock.sendall(f"{name}\n".encode(ENCODING))

        # stop_event = threading.Event()
        # QUE HACE: Crea una bandera para señalar parada.
        # - threading.Event() = bandera compartida entre hilos.
        stop_event = threading.Event()
        
        # recv_thread = threading.Thread(...)
        # QUE HACE: Crea un hilo que recibe mensajes en paralelo.
        # - target = función a ejecutar (_client_receiver).
        # - args = argumentos (sock, stop_event).
        # - daemon = True (hilo muere si proceso principal muere).
        recv_thread = threading.Thread(
            target=_client_receiver,
            args=(sock, stop_event),
            daemon=True,
        )
        # recv_thread.start()
        # QUE HACE: Inicia el hilo receptor.
        recv_thread.start()

        # frases = [...]
        # QUE HACE: Define lista de mensajes a enviar automáticamente.
        # - Lista de 4 frases de ejemplo.
        # - Cada frase es un string.
        # - DIFERENCIA: en cliente base, usuario escribe línea por línea en input().
        # - AQUÍ: frases predefinidas se envían automáticamente.
        frases = [
            "Hola, soy un cliente automatico",
            "Puedo demostrar cambios rapidos",
            "Envio mensajes cada 1 segundo",
            "Si escriben /exit, me detengo",
        ]

        # for frase in frases:
        # QUE HACE: Loop que itera sobre cada frase de la lista.
        # - frase = string actual (ej: "Hola, soy un cliente automatico").
        for frase in frases:
            # if stop_event.is_set():
            # QUE HACE: Verifica si hay señal de parada (receptor falló).
            if stop_event.is_set():
                # break
                # QUE HACE: Sale del loop si hay señal.
                break
            
            # sock.sendall(frase.encode(ENCODING))
            # QUE HACE: Envía la frase al servidor.
            # - frase.encode(ENCODING) = convierte str a bytes.
            # - sendall() = envía todos los bytes.
            sock.sendall(frase.encode(ENCODING))
            
            # time.sleep(1)
            # QUE HACE: Pausa 1 segundo entre frases.
            # - EFECTO: mensajes no son instantáneos, hay tiempo entre ellos.
            # - DIFERENCIA: cliente base envía cuando usuario presiona Enter (variable).
            # - AQUÍ: envío automático cada 1 segundo.
            time.sleep(1)

        # sock.sendall(b"/exit")
        # QUE HACE: Envía comando de salida al servidor.
        # - b"/exit" = bytes (ya codificado).
        sock.sendall(b"/exit")
        
        # stop_event.set()
        # QUE HACE: Señaliza parada (notifica al receptor que cierre).
        stop_event.set()


def _client_receiver(sock: socket.socket, stop_event: threading.Event) -> None:
    """Hilo que recibe mensajes del servidor continuamente.

    PROPÓSITO:
    Ejecuta en paralelo con el envío de frases.
    Lee mensajes que envía el servidor (del otro cliente).
    Imprime cada mensaje recibido.
    Se detiene cuando stop_event es activado.

    Args:
        sock: Socket ya conectado al servidor.
        stop_event: Evento que indica cuándo parar el hilo.
    """

    # while not stop_event.is_set():
    # QUE HACE: Loop que continúa mientras stop_event NO esté activado.
    while not stop_event.is_set():
        # try:
        # QUE HACE: Bloque de control de excepciones.
        try:
            # data = sock.recv(BUFFER_SIZE)
            # QUE HACE: Recibe datos del servidor (BLOQUEA).
            # - recv(BUFFER_SIZE) = recibe hasta 2048 bytes.
            # - data = bytes recibidos.
            data = sock.recv(BUFFER_SIZE)
            
            # if not data:
            # QUE HACE: Verifica si recv() retornó 0 bytes (servidor cerró).
            if not data:
                # break
                # QUE HACE: Sale del loop.
                break
            
            # print(data.decode(ENCODING), end="")
            # QUE HACE: Decodifica y imprime el mensaje.
            # - decode(ENCODING) = bytes a str (UTF-8).
            # - end="" = no agrega salto de línea (mensaje ya viene con \n).
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
    """Punto de entrada del cliente automatizado.

    FLUJO:
    1. Llama a run_client_auto() con los parámetros de conexión (host, port).
    2. run_client_auto() maneja toda la conexión y envío de frases.
    """

    # run_client_auto(HOST, PORT)
    # QUE HACE: Ejecuta el cliente automatizado.
    # - HOST = "127.0.0.1" (localhost).
    # - PORT = 50007 (puerto predeterminado).
    run_client_auto(HOST, PORT)


# if __name__ == "__main__":
# QUE HACE: Verifica si el script se ejecuta directamente.
if __name__ == "__main__":
    # main()
    # QUE HACE: Llama a main() si se ejecuta directamente.
    main()
