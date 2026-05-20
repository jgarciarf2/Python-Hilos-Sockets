"""Ejemplo: servidor que ademas guarda el historial en un archivo.

Ejecuta:
  python Taller1-server-log-archivo.py
"""

from __future__ import annotations  # Anotaciones pospuestas.

import socket  # Sockets TCP.
import threading  # Hilos, semaforos y barreras.
import time  # Pausas.
from dataclasses import dataclass, field  # Data classes.
from datetime import datetime  # Fecha y hora para el archivo.
from pathlib import Path  # Manejo de rutas.


HOST = "127.0.0.1"  # IP local.
PORT = 50007  # Puerto.
ENCODING = "utf-8"  # Codificacion.
BUFFER_SIZE = 2048  # Buffer.
LOG_PATH = Path("chat_historial.txt")  # Archivo de historial.


@dataclass
class ClientState:
    """Estado de un cliente conectado."""  # Doc.

    name: str  # Nombre.
    conn: socket.socket  # Socket.
    addr: tuple[str, int]  # Direccion.
    alive: bool = True  # Estado.


@dataclass
class ChatServer:
    """Servidor con persistencia de historial."""  # Doc.

    host: str = HOST  # IP.
    port: int = PORT  # Puerto.
    history: list[str] = field(default_factory=list)  # Historial.
    history_lock: threading.Lock = field(default_factory=threading.Lock)  # Lock historial.
    send_semaphore: threading.Semaphore = field(default_factory=lambda: threading.Semaphore(1))  # Envio.
    in_flight_limit: threading.BoundedSemaphore = field(default_factory=lambda: threading.BoundedSemaphore(5))  # Carga.
    ready_barrier: threading.Barrier = field(default_factory=lambda: threading.Barrier(2))  # Sincroniza.
    clients: list[ClientState] = field(default_factory=list)  # Clientes.

    def start(self) -> None:
        """Inicia el servidor."""  # Doc.

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:  # Socket TCP.
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Reusar.
            server.bind((self.host, self.port))  # Bind.
            server.listen(2)  # Espera 2.
            print(f"Servidor escuchando en {self.host}:{self.port}")  # Log.

            while len(self.clients) < 2:  # Acepta 2.
                conn, addr = server.accept()  # Accept.
                name = self._recv_name(conn)  # Pide nombre.
                if not name:  # Si falla.
                    conn.close()  # Cierra.
                    continue  # Continua.
                client = ClientState(name=name, conn=conn, addr=addr)  # Estado.
                self.clients.append(client)  # Guarda.
                print(f"Conectado: {name} desde {addr}")  # Log.

                thread = threading.Thread(  # Hilo.
                    target=self._handle_client,  # Target.
                    args=(client,),  # Args.
                    daemon=True,  # Daemon.
                )
                thread.start()  # Start.

            while any(c.alive for c in self.clients):  # Mientras vivo.
                time.sleep(0.2)  # Pausa.

        print("Servidor finalizado.")  # Log.

    def _recv_name(self, conn: socket.socket) -> str:
        """Solicita el nombre al cliente."""  # Doc.

        try:
            conn.sendall("NOMBRE: ".encode(ENCODING))  # Pide.
            raw = conn.recv(BUFFER_SIZE)  # Lee.
            return raw.decode(ENCODING).strip()  # Retorna.
        except OSError:
            return ""  # Falla.

    def _handle_client(self, client: ClientState) -> None:
        """Hilo por cliente."""  # Doc.

        try:
            self.ready_barrier.wait()  # Barrera.
            self._send_to(client, "Ambos usuarios conectados. Puedes chatear.\n")  # Aviso.
        except threading.BrokenBarrierError:
            client.alive = False  # Estado.
            return  # Sale.

        while client.alive:  # Loop.
            try:
                data = client.conn.recv(BUFFER_SIZE)  # Recibe.
                if not data:  # Cierre.
                    break
                message = data.decode(ENCODING).strip()  # Texto.
                if message.lower() == "/exit":  # Salida.
                    break

                if not self.in_flight_limit.acquire(blocking=False):  # Limite.
                    print(f"[DROP] {client.name}: sobrecarga controlada")  # Log.
                    continue  # Ignora.

                try:
                    self._register_message(f"{client.name}: {message}")  # Historial.
                    self._broadcast(client, f"{client.name}: {message}\n")  # Envia.
                finally:
                    self.in_flight_limit.release()  # Libera.

            except OSError:
                break  # Sale.

        client.alive = False  # Marca.
        self._register_message(f"{client.name} se desconecto")  # Log.
        print(f"Desconectado: {client.name}")  # Log.
        try:
            client.conn.close()  # Cierra.
        except OSError:
            pass

    def _register_message(self, message: str) -> None:
        """Registra en memoria y archivo."""  # Doc.

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Sello.
        line = f"[{timestamp}] {message}"  # Linea.
        with self.history_lock:  # Lock.
            self.history.append(line)  # Memoria.
            print(f"[HIST] {line}")  # Consola.
            LOG_PATH.write_text("\n".join(self.history) + "\n", encoding=ENCODING)  # Persistencia.

    def _broadcast(self, sender: ClientState, message: str) -> None:
        """Reenvia mensaje al otro cliente."""  # Doc.

        with self.send_semaphore:  # Semaforo.
            for client in self.clients:  # Itera.
                if client is sender or not client.alive:  # Condicion.
                    continue  # Salta.
                self._send_to(client, message)  # Envia.

    def _send_to(self, client: ClientState, message: str) -> None:
        """Envio seguro."""  # Doc.

        try:
            client.conn.sendall(message.encode(ENCODING))  # Envia.
        except OSError:
            client.alive = False  # Marca.


def main() -> None:
    ChatServer().start()  # Inicia.


if __name__ == "__main__":
    main()  # Entry.
