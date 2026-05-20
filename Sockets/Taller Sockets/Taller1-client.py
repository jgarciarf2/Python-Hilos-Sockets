"""Cliente del chat bidireccional.

Ejecuta:
    python Taller1-client.py
    python Taller1-client.py --spammer
"""

from __future__ import annotations  # Habilita anotaciones de tipo pospuestas.

import argparse  # Parseo de argumentos CLI.
import socket  # API de sockets para TCP.
import threading  # Hilos y semaforos.
import time  # Pausas para el spammer.


HOST = "127.0.0.1"  # IP del servidor.
PORT = 50007  # Puerto del servidor.
ENCODING = "utf-8"  # Codificacion.
BUFFER_SIZE = 2048  # Tamaño maximo del buffer.


def run_client(host: str, port: int, spammer: bool = False) -> None:
    """Conecta al servidor y mantiene un hilo receptor activo."""  # Doc breve.

    name = input("Tu nombre: ").strip() if not spammer else "SPAMMER"  # Nombre o modo spam.
    print(f"Conectando como {name}...")  # Log de conexion.

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:  # Socket TCP.
        sock.connect((host, port))  # Conecta al servidor.
        prompt = sock.recv(BUFFER_SIZE).decode(ENCODING)  # Espera prompt.
        if prompt.startswith("NOMBRE"):  # Verifica solicitud de nombre.
            sock.sendall(f"{name}\n".encode(ENCODING))  # Envia nombre.

        print_lock = threading.Semaphore(1)  # Evita mezclas al imprimir.
        stop_event = threading.Event()  # Señal de parada.

        # Hilo para recibir mensajes mientras el usuario escribe.
        recv_thread = threading.Thread(
            target=_client_receiver,  # Funcion receptor.
            args=(sock, print_lock, stop_event),  # Parametros del hilo.
            daemon=True,  # Hilo se cierra con el proceso.
        )
        recv_thread.start()  # Inicia receptor.

        if spammer:  # Modo cliente especial.
            _client_spam(sock, print_lock, stop_event)  # Envia mensajes rapidos.
        else:
            _client_chat_loop(sock, stop_event)  # Chat interactivo.


def _client_receiver(sock: socket.socket, print_lock: threading.Semaphore, stop_event: threading.Event) -> None:
    """Recibe mensajes de forma asincrona."""  # Doc breve.

    while not stop_event.is_set():  # Loop mientras no haya salida.
        try:  # Controla errores de red.
            data = sock.recv(BUFFER_SIZE)  # Lee del socket.
            if not data:  # Socket cerrado.
                break  # Sale del loop.
            with print_lock:  # Bloquea impresion.
                print(data.decode(ENCODING), end="")  # Imprime mensaje.
        except OSError:  # Error de socket.
            break  # Sale del loop.
    stop_event.set()  # Señaliza el final.


def _client_chat_loop(sock: socket.socket, stop_event: threading.Event) -> None:
    """Lee desde teclado y envia mensajes al servidor."""  # Doc breve.

    while not stop_event.is_set():  # Loop de escritura.
        try:
            message = input()  # Lee teclado.
        except EOFError:
            message = "/exit"  # Cierra si no hay input.
        if not message:  # Ignora vacios.
            continue
        try:
            sock.sendall(message.encode(ENCODING))  # Envia mensaje.
        except OSError:
            break  # Sale si falla envio.
        if message.lower() == "/exit":  # Salida manual.
            break  # Termina.
    stop_event.set()  # Señaliza salida.


def _client_spam(sock: socket.socket, print_lock: threading.Semaphore, stop_event: threading.Event) -> None:
    """Cliente especial que intenta sobrecargar con mensajes rapidos."""  # Doc breve.

    total = 200  # Cantidad de mensajes.
    for i in range(1, total + 1):  # Loop de spam.
        if stop_event.is_set():  # Sale si se detiene.
            break
        payload = f"SPAM {i}"  # Mensaje a enviar.
        try:
            sock.sendall(payload.encode(ENCODING))  # Envia spam.
        except OSError:
            break  # Sale si falla.
        # Pausa minima para no saturar la red local completamente.
        time.sleep(0.01)  # Pausa corta.

    with print_lock:  # Protege salida en consola.
        print("Spammer finalizado. Enviando /exit...")  # Log.
    try:
        sock.sendall(b"/exit")  # Cierra chat.
    except OSError:
        pass  # Ignora error.
    stop_event.set()  # Señaliza salida.


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cliente del chat bidireccional")  # Parser CLI.
    parser.add_argument("--host", default=HOST)  # IP servidor.
    parser.add_argument("--port", type=int, default=PORT)  # Puerto servidor.
    parser.add_argument("--spammer", action="store_true", help="Activa el cliente especial")  # Bandera spam.
    return parser.parse_args()  # Devuelve argumentos.


def main() -> None:
    args = parse_args()  # Lee args.
    run_client(args.host, args.port, spammer=args.spammer)  # Ejecuta cliente.


if __name__ == "__main__":
    main()  # Entrada principal.
