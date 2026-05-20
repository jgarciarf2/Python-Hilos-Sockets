"""Ejemplo: servidor con cifrado XOR simple.

VARIACIÓN: Cifrado XOR
PROPÓSITO: Demostrar cifrado de mensajes usando XOR.
Todos los mensajes se cifran antes de enviarse y se descifran al recibir.

ALGORITMO XOR:
- XOR es simétrico: (a XOR key) XOR key = a.
- Clave: KEY = 37 (número arbitrario).
- Cada byte se XOR con 37 en envío y recepción (decodifica automáticamente).

FLUJO:
1. Cliente envía mensaje cifrado (con XOR).
2. Servidor recibe bytes cifrados.
3. Servidor XOR decodifica para obtener el texto original.
4. Servidor XOR cifra antes de reenviar al otro cliente.
5. Otro cliente recibe bytes cifrados y XOR decodifica.

NOTA SEGURIDAD:
- XOR es MUY débil criptográficamente (solo educativo).
- En producción, usar AES, RSA, TLS, etc.

Ejecuta:
  python Taller1-server-xor.py
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
KEY = 37  # CLAVE: valor XOR (arbitrario, solo ejemplo).
# - Diferencia: no existe en servidor base.
# - USO: cada byte se XOR con 37.


# ===== FUNCIONES GLOBALES =====

def xor_bytes(data: bytes) -> bytes:
    """Aplica XOR simple a cada byte con la clave KEY.

    PROPÓSITO: Cifra o descifra bytes.
    Como XOR es simétrico, aplicar 2 veces retorna original.

    ALGORITMO:
    - Para cada byte b en data: b XOR KEY = byte cifrado.
    - Repetir: byte_cifrado XOR KEY = b (recupera original).

    EJEMPLO:
    - data = b'A' = [65]
    - 65 XOR 37 = 100
    - xor_bytes(b'A') = bytes([100])
    - xor_bytes(bytes([100])) = b'A' (recupera).

    Args:
        data: Bytes de entrada (cifrados o claros).

    Returns:
        Bytes con XOR aplicado (claros si entrada cifrada, cifrados si entrada clara).
    """

    # return bytes(b ^ KEY for b in data)
    # QUE HACE: Aplica XOR a cada byte.
    # - for b in data = itera sobre cada byte de data.
    # - b ^ KEY = XOR bitwise (operador ^).
    # - bytes(...) = convierte generador a bytes.
    # - RESULTADO: nuevo bytes object con cada byte XOR 37.
    return bytes(b ^ KEY for b in data)


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
    """Servidor que cifra/descifra mensajes con XOR.

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
        """Inicia el servidor TCP con cifrado XOR.

        FLUJO:
        1. Crea socket TCP, escucha conexiones.
        2. Acepta exactamente 2 clientes.
        3. Para cada cliente, lanza hilo _handle_client().
        4. Mantiene servidor vivo mientras haya clientes activos.

        CIFRADO:
        - NOMBRE: se envía en claro (primer handshake).
        - Mensajes: se intercambian cifrados con XOR.
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
        """Solicita nombre al cliente (en claro, NO cifrado).

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
        """Hilo que atiende a un cliente con XOR.

        CIFRADO:
        - data recibida del cliente = bytes cifrados.
        - xor_bytes(data) = decodifica (XOR con KEY).
        - Al reenviar: cifra nuevamente con xor_bytes.

        FLUJO:
        1. Espera en barrera.
        2. Loop: recibe bytes cifrados.
        3. Descifra con xor_bytes().
        4. Reenvía (cifrado) al otro cliente.

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
                # QUE HACE: Recibe bytes del cliente.
                # - NOTA: estos bytes ESTÁN CIFRADOS (cliente envía cifrado).
                data = client.conn.recv(BUFFER_SIZE)
                
                if not data:
                    break
                
                # plain = xor_bytes(data).decode(ENCODING).strip()
                # QUE HACE: Descifra y decodifica a string.
                # - xor_bytes(data) = aplica XOR (decodifica).
                # - decode(ENCODING) = bytes a str (UTF-8).
                # - .strip() = elimina espacios/newlines.
                # - RESULTADO: plain = string original del cliente.
                plain = xor_bytes(data).decode(ENCODING).strip()
                
                if plain.lower() == "/exit":
                    break
                
                # self._broadcast(client, f"{client.name}: {plain}\n")
                # QUE HACE: Reenvía el mensaje al otro cliente.
                # - El mensaje va cifrado (ver _broadcast).
                self._broadcast(client, f"{client.name}: {plain}\n")
            except OSError:
                break

        client.alive = False
        try:
            client.conn.close()
        except OSError:
            pass

    def _broadcast(self, sender: ClientState, message: str) -> None:
        """Reenvia mensaje a los demás clientes (CIFRADO).

        CIFRADO:
        - message = string en claro (ej: "Alice: Hola").
        - Se cifra con xor_bytes() antes de enviarlo.
        - El otro cliente recibe bytes cifrados y descifra.

        Args:
            sender: ClientState del remitente.
            message: Texto en claro a cifrar y reenviar.
        """

        # encrypted = xor_bytes(message.encode(ENCODING))
        # QUE HACE: Codifica a bytes y cifra.
        # - message.encode(ENCODING) = str a bytes (ej: b'Alice: Hola').
        # - xor_bytes(...) = aplica XOR (cifra).
        # - encrypted = bytes cifrados.
        encrypted = xor_bytes(message.encode(ENCODING))
        
        # with self.send_semaphore:
        # QUE HACE: Adquiere semáforo antes de enviar.
        with self.send_semaphore:
            for client in self.clients:
                if client is sender or not client.alive:
                    continue
                try:
                    # client.conn.sendall(encrypted)
                    # QUE HACE: Envía los bytes cifrados al cliente.
                    client.conn.sendall(encrypted)
                except OSError:
                    client.alive = False

    def _send_to(self, client: ClientState, message: str) -> None:
        """Envío cifrado a un cliente.

        Args:
            client: ClientState del destinatario.
            message: Texto en claro a cifrar.
        """

        try:
            # client.conn.sendall(xor_bytes(message.encode(ENCODING)))
            # QUE HACE: Cifra el mensaje y lo envía.
            # - message.encode(ENCODING) = str a bytes.
            # - xor_bytes(...) = cifra.
            # - sendall() = envía bytes cifrados.
            client.conn.sendall(xor_bytes(message.encode(ENCODING)))
        except OSError:
            client.alive = False


def main() -> None:
    """Punto de entrada del servidor XOR."""

    ChatServer().start()


if __name__ == "__main__":
    main()

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
