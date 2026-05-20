"""Ejemplo: servidor con limite de mensajes por cliente (rate limit simple).

Ejecuta:
  python Taller1-server-limite-por-cliente.py
"""

from __future__ import annotations  # Anotaciones.

import socket  # Sockets.
import threading  # Hilos y sincronizacion.
import time  # Tiempo.
from dataclasses import dataclass, field  # Data classes.


HOST = "127.0.0.1"  # IP.
PORT = 50007  # Puerto.
ENCODING = "utf-8"  # Codificacion.
BUFFER_SIZE = 2048  # Buffer.
MAX_MSG_PER_SECOND = 5  # Limite por cliente.


@dataclass
class ClientState:
    """Estado del cliente con control de ritmo."""  # Doc.

    name: str  # Nombre.
    conn: socket.socket  # Socket.
    addr: tuple[str, int]  # Direccion.
    alive: bool = True  # Estado.
    last_window: float = field(default_factory=time.time)  # Inicio de ventana.
    window_count: int = 0  # Mensajes en la ventana.


@dataclass
class ChatServer:
    """Servidor con rate limit por cliente."""  # Doc.

    host: str = HOST  # IP.
    port: int = PORT  # Puerto.
    send_semaphore: threading.Semaphore = field(default_factory=lambda: threading.Semaphore(1))  # Envio.
    ready_barrier: threading.Barrier = field(default_factory=lambda: threading.Barrier(2))  # Barrera.
    clients: list[ClientState] = field(default_factory=list)  # Clientes.

    def start(self) -> None:
        """Inicia el servidor."""  # Doc.

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:  # Socket.
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Reusar.
            server.bind((self.host, self.port))  # Bind.
            server.listen(2)  # Listen.
            print(f"Servidor escuchando en {self.host}:{self.port}")  # Log.

            while len(self.clients) < 2:  # Espera 2.
                conn, addr = server.accept()  # Accept.
                name = self._recv_name(conn)  # Nombre.
                if not name:  # Si falla.
                    conn.close()  # Cierra.
                    continue
                client = ClientState(name=name, conn=conn, addr=addr)  # Estado.
                self.clients.append(client)  # Guarda.
                print(f"Conectado: {name} desde {addr}")  # Log.

                thread = threading.Thread(  # Hilo.
                    target=self._handle_client,
                    args=(client,),
                    daemon=True,
                )
                thread.start()  # Start.

            while any(c.alive for c in self.clients):  # Vivo.
                time.sleep(0.2)  # Pausa.

        print("Servidor finalizado.")  # Log.

    def _recv_name(self, conn: socket.socket) -> str:
        """Solicita el nombre."""  # Doc.

        try:
            conn.sendall("NOMBRE: ".encode(ENCODING))  # Pide.
            raw = conn.recv(BUFFER_SIZE)  # Lee.
            return raw.decode(ENCODING).strip()  # Retorna.
        except OSError:
            return ""  # Falla.

    def _handle_client(self, client: ClientState) -> None:
        """Maneja el cliente con limite de mensajes."""  # Doc.

        try:
            self.ready_barrier.wait()  # Barrera.
            self._send_to(client, "Ambos usuarios conectados. Puedes chatear.\n")  # Aviso.
        except threading.BrokenBarrierError:
            client.alive = False  # Marca.
            return

        while client.alive:  # Loop.
            try:
                data = client.conn.recv(BUFFER_SIZE)  # Recibe.
                if not data:  # Cierre.
                    break
                message = data.decode(ENCODING).strip()  # Texto.
                if message.lower() == "/exit":  # Salida.
                    break

                if not self._allow_message(client):  # Limite.
                    print(f"[DROP] {client.name}: rate limit")  # Log.
                    continue

                self._broadcast(client, f"{client.name}: {message}\n")  # Envia.
            except OSError:
                break

        client.alive = False  # Marca.
        print(f"Desconectado: {client.name}")  # Log.
        try:
            client.conn.close()  # Cierra.
        except OSError:
            pass

    def _allow_message(self, client: ClientState) -> bool:
        """Valida rate limit con ventana de 1 segundo."""  # Doc.

        now = time.time()  # Tiempo actual.
        if now - client.last_window >= 1.0:  # Nueva ventana.
            client.last_window = now  # Reinicia.
            client.window_count = 0  # Reinicia.
        client.window_count += 1  # Suma.
        return client.window_count <= MAX_MSG_PER_SECOND  # Decide.

    def _broadcast(self, sender: ClientState, message: str) -> None:
        """Reenvia con semaforo."""  # Doc.

        with self.send_semaphore:  # Semaforo.
            for client in self.clients:  # Itera.
                if client is sender or not client.alive:  # Salta.
                    continue
                self._send_to(client, message)  # Envia.

    def _send_to(self, client: ClientState, message: str) -> None:
        """Envio seguro."""  # Doc.

        try:
            client.conn.sendall(message.encode(ENCODING))  # Envia.
        except OSError:
            client.alive = False  # Marca.


def main() -> None:
    ChatServer().start()  # Ejecuta.


if __name__ == "__main__":
    main()  # Entry.
