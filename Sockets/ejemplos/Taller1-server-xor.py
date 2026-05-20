"""Ejemplo: servidor con cifrado XOR simple.

Ejecuta:
  python Taller1-server-xor.py
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
KEY = 37  # Clave XOR.


def xor_bytes(data: bytes) -> bytes:
    """Aplica XOR simple a cada byte.

    Args:
        data: Bytes de entrada.

    Returns:
        Bytes con XOR aplicado usando la clave KEY.
    """  # Doc.

    return bytes(b ^ KEY for b in data)  # XOR.


@dataclass
class ClientState:
    """Estado del cliente.

    Atributos:
        name: Nombre del cliente.
        conn: Socket TCP asociado.
        addr: Tupla (ip, puerto).
        alive: Bandera de actividad.
    """  # Doc.

    name: str  # Nombre.
    conn: socket.socket  # Socket.
    addr: tuple[str, int]  # Direccion.
    alive: bool = True  # Estado.


@dataclass
class ChatServer:
    """Servidor que cifra y descifra mensajes.

    Atributos:
        host: IP de escucha.
        port: Puerto de escucha.
        send_semaphore: Semaforo para serializar envios.
        ready_barrier: Barrera para sincronizar inicio.
        clients: Lista de clientes.
    """  # Doc.

    host: str = HOST  # IP.
    port: int = PORT  # Puerto.
    send_semaphore: threading.Semaphore = field(default_factory=lambda: threading.Semaphore(1))  # Envio.
    ready_barrier: threading.Barrier = field(default_factory=lambda: threading.Barrier(2))  # Barrera.
    clients: list[ClientState] = field(default_factory=list)  # Clientes.

    def start(self) -> None:
        """Inicia servidor.

        Acepta dos clientes, lanza hilos y mantiene vivo el proceso.
        """  # Doc.

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
        """Solicita nombre.

        Args:
            conn: Socket conectado.

        Returns:
            Nombre o cadena vacia.
        """  # Doc.

        try:
            conn.sendall("NOMBRE: ".encode(ENCODING))
            raw = conn.recv(BUFFER_SIZE)
            return raw.decode(ENCODING).strip()
        except OSError:
            return ""

    def _handle_client(self, client: ClientState) -> None:
        """Recibe cifrado, descifra y reenvia.

        Args:
            client: Estado del cliente atendido.
        """  # Doc.

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
                plain = xor_bytes(data).decode(ENCODING).strip()
                if plain.lower() == "/exit":
                    break
                self._broadcast(client, f"{client.name}: {plain}\n")
            except OSError:
                break

        client.alive = False
        try:
            client.conn.close()
        except OSError:
            pass

    def _broadcast(self, sender: ClientState, message: str) -> None:
        """Envia cifrado a otros clientes.

        Args:
            sender: Cliente emisor.
            message: Texto en claro a cifrar y reenviar.
        """  # Doc.

        encrypted = xor_bytes(message.encode(ENCODING))  # Cifra.
        with self.send_semaphore:
            for client in self.clients:
                if client is sender or not client.alive:
                    continue
                try:
                    client.conn.sendall(encrypted)
                except OSError:
                    client.alive = False

    def _send_to(self, client: ClientState, message: str) -> None:
        """Envio cifrado.

        Args:
            client: Destinatario.
            message: Texto en claro.
        """  # Doc.

        try:
            client.conn.sendall(xor_bytes(message.encode(ENCODING)))
        except OSError:
            client.alive = False


def main() -> None:
    """Punto de entrada del servidor XOR."""

    ChatServer().start()


if __name__ == "__main__":
    main()
