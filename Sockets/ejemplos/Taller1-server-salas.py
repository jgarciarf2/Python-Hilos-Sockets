"""Ejemplo: servidor con salas (rooms) seleccionadas por el cliente.

Ejecuta:
  python Taller1-server-salas.py
"""

from __future__ import annotations  # Anotaciones.

import socket  # Sockets.
import threading  # Hilos y barreras.
import time  # Pausas.
from dataclasses import dataclass, field  # Data classes.


HOST = "127.0.0.1"  # IP.
PORT = 50007  # Puerto.
ENCODING = "utf-8"  # Codificacion.
BUFFER_SIZE = 2048  # Buffer.


@dataclass
class ClientState:
    """Estado del cliente en una sala.

    Atributos:
        name: Nombre del cliente.
        room: Sala seleccionada.
        conn: Socket TCP del cliente.
        addr: Tupla (ip, puerto).
        alive: Bandera de actividad.
    """  # Doc.

    name: str  # Nombre.
    room: str  # Sala.
    conn: socket.socket  # Socket.
    addr: tuple[str, int]  # Direccion.
    alive: bool = True  # Estado.


@dataclass
class ChatServer:
    """Servidor con salas separadas.

    Atributos:
        host: IP de escucha.
        port: Puerto de escucha.
        ready_barrier: Barrera (no usada para salas, se mantiene de ejemplo).
        send_semaphore: Semaforo para serializar envios.
        rooms: Diccionario sala -> lista de clientes.
    """  # Doc.

    host: str = HOST  # IP.
    port: int = PORT  # Puerto.
    ready_barrier: threading.Barrier = field(default_factory=lambda: threading.Barrier(2))  # Barrera.
    send_semaphore: threading.Semaphore = field(default_factory=lambda: threading.Semaphore(1))  # Envio.
    rooms: dict[str, list[ClientState]] = field(default_factory=dict)  # Mapa de salas.

    def start(self) -> None:
        """Inicia el servidor y acepta clientes.

        Flujo:
            - Acepta conexiones sin limite fijo.
            - Pide nombre y sala.
            - Lanza un hilo por cliente.
        """  # Doc.

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:  # Socket.
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Reusar.
            server.bind((self.host, self.port))  # Bind.
            server.listen(4)  # Permite mas conexiones.
            print(f"Servidor escuchando en {self.host}:{self.port}")  # Log.

            while True:
                conn, addr = server.accept()  # Accept.
                name, room = self._recv_identity(conn)  # Nombre y sala.
                if not name or not room:  # Validacion.
                    conn.close()  # Cierra.
                    continue
                client = ClientState(name=name, room=room, conn=conn, addr=addr)  # Estado.
                self.rooms.setdefault(room, []).append(client)  # Agrega a sala.
                print(f"Conectado: {name} en sala {room}")  # Log.

                thread = threading.Thread(  # Hilo.
                    target=self._handle_client,
                    args=(client,),
                    daemon=True,
                )
                thread.start()  # Start.

    def _recv_identity(self, conn: socket.socket) -> tuple[str, str]:
        """Pide nombre y sala al cliente.

        Args:
            conn: Socket conectado.

        Returns:
            Tupla (nombre, sala) o cadenas vacias si falla.
        """  # Doc.

        try:
            conn.sendall("NOMBRE: ".encode(ENCODING))  # Pide nombre.
            name = conn.recv(BUFFER_SIZE).decode(ENCODING).strip()  # Lee.
            conn.sendall("SALA: ".encode(ENCODING))  # Pide sala.
            room = conn.recv(BUFFER_SIZE).decode(ENCODING).strip()  # Lee.
            return name, room  # Retorna.
        except OSError:
            return "", ""  # Falla.

    def _handle_client(self, client: ClientState) -> None:
        """Maneja mensajes dentro de la sala.

        Args:
            client: Estado del cliente atendido.
        """  # Doc.

        while client.alive:
            try:
                data = client.conn.recv(BUFFER_SIZE)  # Recibe.
                if not data:
                    break
                message = data.decode(ENCODING).strip()  # Texto.
                if message.lower() == "/exit":
                    break
                self._broadcast_room(client, f"{client.name}: {message}\n")  # Envia.
            except OSError:
                break

        client.alive = False  # Marca.
        try:
            client.conn.close()  # Cierra.
        except OSError:
            pass

    def _broadcast_room(self, sender: ClientState, message: str) -> None:
        """Reenvia solo a clientes de la misma sala.

        Args:
            sender: Cliente emisor.
            message: Texto a enviar a la sala.
        """  # Doc.

        with self.send_semaphore:  # Semaforo.
            for client in self.rooms.get(sender.room, []):  # Clientes de sala.
                if client is sender or not client.alive:
                    continue
                try:
                    client.conn.sendall(message.encode(ENCODING))  # Envia.
                except OSError:
                    client.alive = False


def main() -> None:
    """Punto de entrada del servidor con salas."""

    ChatServer().start()  # Ejecuta.


if __name__ == "__main__":
    main()  # Entry.
