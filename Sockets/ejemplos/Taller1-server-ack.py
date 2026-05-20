"""Ejemplo: servidor que exige ACK por cada mensaje recibido.

Ejecuta:
  python Taller1-server-ack.py
"""

from __future__ import annotations  # Anotaciones.

import socket  # Sockets.
import threading  # Hilos.
import time  # Pausas.
from dataclasses import dataclass, field  # Data classes.


HOST = "127.0.0.1"  # IP.
PORT = 50007  # Puerto.
ENCODING = "utf-8"  # Codificacion.
BUFFER_SIZE = 2048  # Buffer.
ACK = "[ACK]"  # Token de confirmacion.


@dataclass
class ClientState:
    """Estado del cliente."""  # Doc.

    name: str  # Nombre.
    conn: socket.socket  # Socket.
    addr: tuple[str, int]  # Direccion.
    alive: bool = True  # Estado.


@dataclass
class ChatServer:
    """Servidor con confirmacion de recepcion."""  # Doc.

    host: str = HOST  # IP.
    port: int = PORT  # Puerto.
    send_semaphore: threading.Semaphore = field(default_factory=lambda: threading.Semaphore(1))  # Envio.
    ready_barrier: threading.Barrier = field(default_factory=lambda: threading.Barrier(2))  # Barrera.
    clients: list[ClientState] = field(default_factory=list)  # Clientes.

    def start(self) -> None:
        """Inicia servidor."""  # Doc.

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.host, self.port))
            server.listen(2)
            print(f"Servidor escuchando en {self.host}:{self.port}")

            while len(self.clients) < 2:
                conn, addr = server.accept()
                name = self._recv_name(conn)
                if not name:
                    conn.close()
                    continue
                client = ClientState(name=name, conn=conn, addr=addr)
                self.clients.append(client)
                print(f"Conectado: {name}")

                thread = threading.Thread(
                    target=self._handle_client,
                    args=(client,),
                    daemon=True,
                )
                thread.start()

            while any(c.alive for c in self.clients):
                time.sleep(0.2)

        print("Servidor finalizado.")

    def _recv_name(self, conn: socket.socket) -> str:
        """Pide nombre."""  # Doc.

        try:
            conn.sendall("NOMBRE: ".encode(ENCODING))
            raw = conn.recv(BUFFER_SIZE)
            return raw.decode(ENCODING).strip()
        except OSError:
            return ""

    def _handle_client(self, client: ClientState) -> None:
        """Recibe mensajes y responde ACK al emisor."""  # Doc.

        try:
            self.ready_barrier.wait()
            self._send_to(client, "Ambos usuarios conectados. Puedes chatear.\n")
        except threading.BrokenBarrierError:
            client.alive = False
            return

        while client.alive:
            try:
                data = client.conn.recv(BUFFER_SIZE)
                if not data:
                    break
                message = data.decode(ENCODING).strip()
                if message.lower() == "/exit":
                    break
                self._send_to(client, f"{ACK} recibido")  # Confirma.
                self._broadcast(client, f"{client.name}: {message}\n")
            except OSError:
                break

        client.alive = False
        try:
            client.conn.close()
        except OSError:
            pass

    def _broadcast(self, sender: ClientState, message: str) -> None:
        """Reenvia a los otros clientes."""  # Doc.

        with self.send_semaphore:
            for client in self.clients:
                if client is sender or not client.alive:
                    continue
                self._send_to(client, message)

    def _send_to(self, client: ClientState, message: str) -> None:
        """Envio seguro."""  # Doc.

        try:
            client.conn.sendall(message.encode(ENCODING))
        except OSError:
            client.alive = False


def main() -> None:
    ChatServer().start()


if __name__ == "__main__":
    main()
