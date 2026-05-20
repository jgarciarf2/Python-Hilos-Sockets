"""Servidor del chat bidireccional.

Ejecuta:
    python Taller1-server.py
"""

from __future__ import annotations  # Habilita anotaciones de tipo pospuestas.

import socket  # API de sockets para comunicacion TCP.
import threading  # Hilos, semaforos y barreras.
import time  # Pausas para mantener vivo el proceso.
from dataclasses import dataclass, field  # Estructuras simples con datos.


HOST = "127.0.0.1"  # IP local donde escucha el servidor.
PORT = 50007  # Puerto TCP.
ENCODING = "utf-8"  # Codificacion de los mensajes.
BUFFER_SIZE = 2048  # Tamaño maximo del buffer de lectura.


@dataclass  # Crea __init__ y otros metodos utiles.
class ClientState:
    """Estado de un cliente conectado."""  # Descripcion del contenedor.

    name: str  # Nombre del cliente.
    conn: socket.socket  # Socket del cliente.
    addr: tuple[str, int]  # Direccion remota (ip, puerto).
    alive: bool = True  # Bandera para saber si sigue activo.


@dataclass  # Estructura principal del servidor.
class ChatServer:
    """Servidor que orquesta la comunicacion y el historial del chat."""  # Doc general.

    host: str = HOST  # IP de escucha.
    port: int = PORT  # Puerto de escucha.
    history: list[str] = field(default_factory=list)  # Historial en memoria.
    history_lock: threading.Lock = field(default_factory=threading.Lock)  # Protege historial.
    send_semaphore: threading.Semaphore = field(default_factory=lambda: threading.Semaphore(1))  # Serializa envios.
    in_flight_limit: threading.BoundedSemaphore = field(default_factory=lambda: threading.BoundedSemaphore(5))  # Limita carga.
    ready_barrier: threading.Barrier = field(default_factory=lambda: threading.Barrier(2))  # Espera 2 clientes.
    clients: list[ClientState] = field(default_factory=list)  # Lista de clientes.

    def start(self) -> None:
        """Inicia el servidor y acepta dos conexiones."""  # Doc del metodo.

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:  # Socket TCP.
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Reusar puerto.
            server.bind((self.host, self.port))  # Asigna IP y puerto.
            server.listen(2)  # Cola de hasta 2 conexiones.
            print(f"Servidor escuchando en {self.host}:{self.port}")  # Log inicial.

            while len(self.clients) < 2:  # Acepta exactamente 2 clientes.
                conn, addr = server.accept()  # Espera conexion.
                name = self._recv_name(conn)  # Solicita nombre.
                if not name:  # Si no hay nombre, se descarta.
                    conn.close()  # Cierra socket.
                    continue  # Vuelve a esperar.
                client = ClientState(name=name, conn=conn, addr=addr)  # Crea estado.
                self.clients.append(client)  # Guarda cliente.
                print(f"Conectado: {name} desde {addr}")  # Log.

                thread = threading.Thread(  # Hilo por cliente.
                    target=self._handle_client,  # Manejo de mensajes.
                    args=(client,),  # Pasa el cliente.
                    daemon=True,  # Hilo se cierra con el proceso.
                )
                thread.start()  # Inicia el hilo.

            # Mantiene el servidor vivo hasta que todos salgan.
            while any(c.alive for c in self.clients):  # Revisa clientes activos.
                time.sleep(0.2)  # Evita loop agresivo.

        print("Servidor finalizado.")  # Log final.

    def _recv_name(self, conn: socket.socket) -> str:
        """Solicita el nombre al cliente para identificarlo en el chat."""  # Doc breve.

        try:  # Control de errores de red.
            conn.sendall("NOMBRE: ".encode(ENCODING))  # Pide nombre.
            raw = conn.recv(BUFFER_SIZE)  # Lee respuesta.
            return raw.decode(ENCODING).strip()  # Devuelve limpio.
        except OSError:  # Error de socket.
            return ""  # Sin nombre.

    def _handle_client(self, client: ClientState) -> None:
        """Escucha mensajes de un cliente y los reenvia al otro."""  # Doc breve.

        try:  # Barrera puede lanzar error.
            # Barrera: espera a que ambos clientes esten listos.
            self.ready_barrier.wait()  # Sincroniza el inicio.
            self._send_to(client, "Ambos usuarios conectados. Puedes chatear.\n")  # Aviso.
        except threading.BrokenBarrierError:  # Si algun cliente falla.
            client.alive = False  # Marca como inactivo.
            return  # Sale del hilo.

        while client.alive:  # Loop de mensajes.
            try:  # Control de errores de socket.
                data = client.conn.recv(BUFFER_SIZE)  # Espera mensaje.
                if not data:  # Socket cerrado.
                    break  # Sale del loop.
                message = data.decode(ENCODING).strip()  # Convierte a texto.
                if message.lower() == "/exit":  # Comando de salida.
                    break  # Termina el chat.

                # Controla la sobrecarga limitando mensajes en vuelo.
                if not self.in_flight_limit.acquire(blocking=False):  # Intenta tomar cupo.
                    print(f"[DROP] {client.name}: sobrecarga controlada")  # Log drop.
                    continue  # Ignora el mensaje.

                try:  # Protege liberacion del semaforo.
                    self._register_message(f"{client.name}: {message}")  # Guarda historial.
                    self._broadcast(client, f"{client.name}: {message}\n")  # Reenvia.
                finally:
                    self.in_flight_limit.release()  # Libera cupo.

            except OSError:  # Error de socket.
                break  # Sale del loop.

        client.alive = False  # Marca desconexion.
        self._register_message(f"{client.name} se desconecto")  # Historial.
        print(f"Desconectado: {client.name}")  # Log.
        try:
            client.conn.close()  # Cierra socket.
        except OSError:
            pass  # Ignora error al cerrar.

    def _register_message(self, message: str) -> None:
        """Agrega un mensaje al historial y lo imprime en servidor."""  # Doc.

        with self.history_lock:  # Protege historial compartido.
            self.history.append(message)  # Agrega al historial.
            print(f"[HIST] {message}")  # Muestra en consola.

    def _broadcast(self, sender: ClientState, message: str) -> None:
        """Reenvia el mensaje al cliente opuesto con un semaforo de envio."""  # Doc.

        with self.send_semaphore:  # Evita envios simultaneos.
            for client in self.clients:  # Itera clientes.
                if client is sender or not client.alive:  # Salta emisor o inactivos.
                    continue  # No se envia.
                self._send_to(client, message)  # Envia mensaje.

    def _send_to(self, client: ClientState, message: str) -> None:
        """Envio protegido para un cliente."""  # Doc.

        try:
            client.conn.sendall(message.encode(ENCODING))  # Envia bytes.
        except OSError:  # Error al enviar.
            client.alive = False  # Marca inactivo.


def main() -> None:
    ChatServer().start()  # Inicia servidor con valores por defecto.


if __name__ == "__main__":
    main()  # Entrada principal del script.
