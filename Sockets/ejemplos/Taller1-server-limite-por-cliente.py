"""Ejemplo: servidor con límite de mensajes por cliente (rate limit simple).

VARIACIÓN: Rate-Limit Per-Client
PROPÓSITO: Cada cliente tiene su propio límite de tasa (5 msgs/seg).
Esto evita que UN cliente solo domine la conversación.

DIFERENCIA CLAVE vs base:
- Base: usa in_flight_limit global (BoundedSemaphore(5)) - limite en vuelo total.
- Esta: usa rate limit PER CLIENTE - cada uno puede enviar máx 5 msgs/seg.
- Beneficio: control granular por usuario.

ALGORITMO (sliding window):
- Cada cliente tiene last_window (inicio de ventana actual) y window_count.
- Si ahora - last_window >= 1.0 seg, resetea ventana.
- Sino, suma 1 a window_count.
- Permite si window_count <= MAX_MSG_PER_SECOND (5).

Ejecuta:
  python Taller1-server-limite-por-cliente.py
"""

from __future__ import annotations  # Anotaciones pospuestas.

import socket  # Módulo sockets para TCP/IP.
import threading  # Módulo de hilos.
import time  # Función time.time() para timestamps, time.sleep() para pausas.
from dataclasses import dataclass, field  # Decoradores para data classes.


# ===== CONSTANTES =====

HOST = "127.0.0.1"  # IP de escucha.
PORT = 50007  # Puerto de escucha.
ENCODING = "utf-8"  # Codificación de texto.
BUFFER_SIZE = 2048  # Tamaño máximo de recv().
MAX_MSG_PER_SECOND = 5  # CLAVE: límite de mensajes por segundo POR CLIENTE.
# - Diferencia: no es límite global, sino individual.


# ===== CLASES DE DATOS =====

@dataclass
class ClientState:
    """Estado del cliente con control de ritmo (rate limit).

    DIFERENCIA: Tiene atributos additional last_window y window_count para rate limiting.

    Atributos:
        name: Nombre del cliente (string).
        conn: Socket TCP asociado al cliente.
        addr: Tupla (ip, puerto) remota.
        alive: Bandera de actividad.
        last_window: Timestamp de inicio de la ventana de 1 seg actual (float).
        window_count: Contador de mensajes en la ventana actual (int).
    """

    name: str  # Nombre del cliente.
    conn: socket.socket  # Socket TCP.
    addr: tuple[str, int]  # Tupla (IP, puerto).
    alive: bool = True  # Bandera de estado.
    last_window: float = field(default_factory=time.time)  # Timestamp de inicio de ventana.
    # - QUE HACE: field(default_factory=time.time) = cada instancia obtiene el tiempo actual.
    # - USO: para calcular si pasó 1 segundo.
    window_count: int = 0  # Contador de mensajes en ventana actual.
    # - Comienza en 0, se suma 1 por cada mensaje.
    # - Se resetea cuando la ventana vuelve a empezar.


@dataclass
class ChatServer:
    """Servidor con rate limit individual por cliente.

    Atributos:
        host: IP de escucha (default: "127.0.0.1").
        port: Puerto de escucha (default: 50007).
        send_semaphore: Semáforo(1) para serializar envios de broadcast.
        ready_barrier: Barrier(2) para sincronizar 2 clientes al inicio.
        clients: Lista de ClientState (clientes conectados).
    """

    host: str = HOST  # IP de escucha.
    port: int = PORT  # Puerto de escucha.
    send_semaphore: threading.Semaphore = field(default_factory=lambda: threading.Semaphore(1))  # Serializa envios.
    ready_barrier: threading.Barrier = field(default_factory=lambda: threading.Barrier(2))  # Sincroniza 2 clientes.
    clients: list[ClientState] = field(default_factory=list)  # Clientes conectados.

    def start(self) -> None:
        """Inicia el servidor TCP.

        FLUJO:
        1. Crea socket TCP, escucha conexiones.
        2. Acepta exactamente 2 clientes.
        3. Para cada cliente, lanza hilo _handle_client().
        4. Mantiene servidor vivo mientras haya clientes activos.

        CONTROL DE CARGA:
        - Rate limit: cada cliente limitado a 5 msgs/seg (via _allow_message).
        - Broadcast: serializado con send_semaphore.
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
                print(f"Conectado: {name} desde {addr}")

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
            Nombre del cliente (string) o "" si falla.
        """

        try:
            conn.sendall("NOMBRE: ".encode(ENCODING))
            raw = conn.recv(BUFFER_SIZE)
            return raw.decode(ENCODING).strip()
        except OSError:
            return ""

    def _handle_client(self, client: ClientState) -> None:
        """Hilo que atiende a un cliente con rate limit.

        FLUJO:
        1. Espera en barrera (hasta que ambos clientes lleguen).
        2. Loop: recibe mensaje.
        3. Verifica rate limit con _allow_message().
        4. Si OK, reenvía al otro cliente.
        5. Si excede límite, descarta.

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
                data = client.conn.recv(BUFFER_SIZE)
                if not data:
                    break
                message = data.decode(ENCODING).strip()
                if message.lower() == "/exit":
                    break

                # CLAVE: verificar rate limit ANTES de procesar el mensaje.
                if not self._allow_message(client):
                    print(f"[DROP] {client.name}: rate limit")
                    continue

                self._broadcast(client, f"{client.name}: {message}\n")
            except OSError:
                break

        client.alive = False
        print(f"Desconectado: {client.name}")
        try:
            client.conn.close()
        except OSError:
            pass

    def _allow_message(self, client: ClientState) -> bool:
        """Valida si el mensaje respeta el rate limit (5 msgs/sec por cliente).

        ALGORITMO (Sliding Window):
        - Ventana = 1 segundo.
        - now = tiempo actual (float).
        - Si (now - last_window) >= 1.0, la ventana vence. Reinicia.
        - Sino, la ventana sigue activa. Suma 1 al counter.
        - Permitir si window_count <= MAX_MSG_PER_SECOND (5).

        EFECTO:
        - Mensaje 1: new window, count=1, OK.
        - Mensaje 2: same window (< 1 seg), count=2, OK.
        - ...
        - Mensaje 6: same window, count=6, RECHAZADO (count > 5).
        - 1 seg después: new window, count=1, OK.

        Args:
            client: ClientState del cliente a evaluar.

        Returns:
            True si el mensaje es permitido, False si excede el límite.
        """

        # now = time.time()
        # QUE HACE: Obtiene el tiempo actual (float, segundos desde epoch).
        now = time.time()
        
        # if now - client.last_window >= 1.0:
        # QUE HACE: Verifica si la ventana vence (pasó 1 segundo).
        # - now - last_window = tiempo transcurrido desde inicio de ventana.
        # - >= 1.0 = si pasó 1 segundo o más.
        if now - client.last_window >= 1.0:
            # client.last_window = now
            # QUE HACE: Reinicia el inicio de ventana al tiempo actual.
            client.last_window = now
            # client.window_count = 0
            # QUE HACE: Reinicia el contador a 0.
            client.window_count = 0
        
        # client.window_count += 1
        # QUE HACE: Suma 1 al contador (incrementa por este mensaje).
        client.window_count += 1
        
        # return client.window_count <= MAX_MSG_PER_SECOND
        # QUE HACE: Retorna True si el contador NO excede el límite (5).
        # - Si window_count = 6, retorna False (rechazado).
        # - Si window_count = 5, retorna True (aceptado).
        # - EFECTO: el caller descarta el mensaje si retorna False.
        return client.window_count <= MAX_MSG_PER_SECOND

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
    """Punto de entrada del servidor con rate limit."""

    ChatServer().start()


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()  # Entry.
