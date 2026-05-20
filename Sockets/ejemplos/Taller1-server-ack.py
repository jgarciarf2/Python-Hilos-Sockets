"""Ejemplo: servidor que exige ACK por cada mensaje recibido.

VARIACIÓN: Acknowledgment (ACK)
PROPÓSITO: Protocolo de confirmación bidireccional.
Cuando el servidor recibe un mensaje, responde "[ACK] recibido" al remitente.
Luego reenvía el mensaje al otro cliente.

PROTOCOLO ACK:
1. Cliente A envía "Hola".
2. Servidor recibe "Hola".
3. Servidor responde "[ACK] recibido" a cliente A.
4. Servidor reenvía "A: Hola" a cliente B.

BENEFICIO:
- Cliente A sabe que su mensaje fue recibido (feedback inmediato).
- Útil para debugging y validación de recepción.

DIFERENCIA vs base:
- Base: servidor solo reenvía, sin confirmación.
- Esta: servidor confirma recepción con "[ACK] recibido".

Ejecuta:
  python Taller1-server-ack.py
"""

from __future__ import annotations  # Anotaciones pospuestas.

import socket  # Módulo sockets para TCP/IP.
import threading  # Módulo de hilos.
import time  # time.sleep() para pausas.
from dataclasses import dataclass, field  # Decoradores para data classes.


# ===== CONSTANTES =====

HOST = "127.0.0.1"  # IP de escucha.
PORT = 50007  # Puerto de escucha.
ENCODING = "utf-8"  # Codificación de texto.
BUFFER_SIZE = 2048  # Tamaño máximo de recv().
ACK = "[ACK]"  # CLAVE: token de confirmación.
# - Diferencia: no existe en servidor base.
# - USO: se envía al cliente como confirmación.


# ===== CLASES DE DATOS =====

@dataclass
class ClientState:
    """Estado del cliente.

    Atributos:
        name: Nombre del cliente (string).
        conn: Socket TCP asociado al cliente.
        addr: Tupla (ip, puerto) remota.
        alive: Bandera de actividad.
    """

    name: str  # Nombre del cliente.
    conn: socket.socket  # Socket TCP.
    addr: tuple[str, int]  # Tupla (IP, puerto).
    alive: bool = True  # Bandera de estado.


@dataclass
class ChatServer:
    """Servidor con confirmación de recepción (ACK).

    Atributos:
        host: IP de escucha (default: "127.0.0.1").
        port: Puerto de escucha (default: 50007).
        send_semaphore: Semáforo(1) para serializar envios.
        ready_barrier: Barrier(2) para sincronizar 2 clientes.
        clients: Lista de ClientState (clientes conectados).
    """

    host: str = HOST  # IP de escucha.
    port: int = PORT  # Puerto de escucha.
    send_semaphore: threading.Semaphore = field(default_factory=lambda: threading.Semaphore(1))  # Serializa envios.
    ready_barrier: threading.Barrier = field(default_factory=lambda: threading.Barrier(2))  # Sincroniza 2 clientes.
    clients: list[ClientState] = field(default_factory=list)  # Clientes conectados.

    def start(self) -> None:
        """Inicia el servidor TCP con ACK.

        FLUJO:
        1. Crea socket TCP, escucha conexiones.
        2. Acepta exactamente 2 clientes.
        3. Para cada cliente, lanza hilo _handle_client().
        4. Mantiene servidor vivo mientras haya clientes activos.

        PROTOCOLO ACK:
        - Cuando recibe mensaje: responde "[ACK] recibido" al remitente.
        - Luego reenvía el mensaje al otro cliente.
        """

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
        """Solicita nombre al cliente.

        Args:
            conn: Socket conectado.

        Returns:
            Nombre del cliente o "" si falla.
        """

        try:
            conn.sendall("NOMBRE: ".encode(ENCODING))
            raw = conn.recv(BUFFER_SIZE)
            return raw.decode(ENCODING).strip()
        except OSError:
            return ""

    def _handle_client(self, client: ClientState) -> None:
        """Hilo que atiende a un cliente con protocolo ACK.

        FLUJO CON ACK:
        1. Espera en barrera.
        2. Loop: recibe mensaje.
        3. Envía "[ACK] recibido" al remitente (confirmación).
        4. Reenvía el mensaje al otro cliente.

        DIFERENCIA:
        - Base: solo reenvía (sin ACK).
        - Esta: responde con ACK primero, luego reenvía.

        Args:
            client: ClientState del cliente atendido.
        """

        try:
            self.ready_barrier.wait()
            self._send_to(client, "Ambos usuarios conectados. Puedes chatear.\n")
        except threading.BrokenBarrierError:
            client.alive = False
            return

        while client.alive:
            try:
                # data = client.conn.recv(BUFFER_SIZE)
                # QUE HACE: Recibe mensaje del cliente.
                data = client.conn.recv(BUFFER_SIZE)
                
                if not data:
                    break
                
                # message = data.decode(ENCODING).strip()
                # QUE HACE: Decodifica a string y elimina espacios.
                message = data.decode(ENCODING).strip()
                
                if message.lower() == "/exit":
                    break
                
                # self._send_to(client, f"{ACK} recibido")
                # QUE HACE: Responde ACK al remitente (confirmación inmediata).
                # - ACK = "[ACK]" (token predefinido).
                # - EFECTO: cliente A recibe "[ACK] recibido" cuando envía.
                # - PROPÓSITO: feedback de recepción.
                self._send_to(client, f"{ACK} recibido")
                
                # self._broadcast(client, f"{client.name}: {message}\n")
                # QUE HACE: Reenvía el mensaje al otro cliente.
                # - ORDEN: ACK se envía PRIMERO, luego broadcast.
                self._broadcast(client, f"{client.name}: {message}\n")
            except OSError:
                break

        client.alive = False
        try:
            client.conn.close()
        except OSError:
            pass

    def _broadcast(self, sender: ClientState, message: str) -> None:
        """Reenvia mensaje a los demás clientes.

        Args:
            sender: ClientState del remitente.
            message: Texto formateado a reenvijar.
        """

        with self.send_semaphore:
            for client in self.clients:
                if client is sender or not client.alive:
                    continue
                self._send_to(client, message)

    def _send_to(self, client: ClientState, message: str) -> None:
        """Envío seguro a un cliente.

        Args:
            client: ClientState del destinatario.
            message: Texto formateado.
        """

        try:
            client.conn.sendall(message.encode(ENCODING))
        except OSError:
            client.alive = False


def main() -> None:
    """Punto de entrada del servidor con ACK."""

    ChatServer().start()


if __name__ == "__main__":
    main()
