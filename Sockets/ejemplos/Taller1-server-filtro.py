"""Ejemplo: servidor con filtro de palabras (Censura) con DOCUMENTACIÓN EXHAUSTIVA.

PROPÓSITO:
- Demuestra cómo el servidor puede "analizar" y "modificar" los datos antes de reenviarlos.
- Si un usuario escribe una palabra prohibida, el servidor la reemplaza por asteriscos.

CONCEPTO CLAVE:
- Procesamiento de strings: message.replace(word, "***").
- Seguridad de contenido: El servidor actúa como moderador centralizado.

Ejecuta:
  python Taller1-server-filtro.py
"""

from __future__ import annotations
import socket
import threading
from dataclasses import dataclass, field

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
HOST = "127.0.0.1"
PORT = 50007
ENCODING = "utf-8"
BUFFER_SIZE = 2048

# PALABRAS_PROHIBIDAS
# - Lista de términos que el servidor no permitirá mostrar en claro.
PALABRAS_PROHIBIDAS = ["tonto", "feo", "malo", "error"]

@dataclass
class ClientState:
    """Representación del cliente en el servidor."""
    name: str = ""
    conn: socket.socket = field(default=None)
    alive: bool = True

@dataclass
class FilterServer:
    """Servidor que censura palabras prohibidas antes del broadcast."""

    host: str = HOST
    port: int = PORT
    clients: list[ClientState] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def start(self) -> None:
        """Inicia el servidor."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.host, self.port))
            s.listen(5)
            print(f"Servidor con FILTRO escuchando en {self.host}:{self.port}")
            print(f"Palabras censuradas: {PALABRAS_PROHIBIDAS}")

            while True:
                conn, addr = s.accept()
                conn.sendall(b"NOMBRE: ")
                name = conn.recv(BUFFER_SIZE).decode(ENCODING).strip()
                
                client = ClientState(name=name, conn=conn)
                with self.lock:
                    self.clients.append(client)
                
                print(f" [+] {name} conectado.")
                threading.Thread(target=self._handle, args=(client,), daemon=True).start()

    def _handle(self, client: ClientState) -> None:
        """Recibe, filtra y reenvía."""
        while client.alive:
            try:
                data = client.conn.recv(BUFFER_SIZE)
                if not data: break
                
                original_msg = data.decode(ENCODING).strip()
                if original_msg.lower() == "/exit": break
                
                # --- APLICAR FILTRO ---
                filtered_msg = original_msg
                
                # for prohibida in PALABRAS_PROHIBIDAS:
                # - Recorre cada palabra de la lista negra.
                for prohibida in PALABRAS_PROHIBIDAS:
                    # filtered_msg.replace(...)
                    # - Busca la palabra (ignorando mayúsculas/minúsculas con .lower()).
                    # - Nota: una versión pro usaría Regex, esta es simple para el taller.
                    if prohibida in filtered_msg.lower():
                        # Creamos una cadena de asteriscos del mismo largo
                        censura = "*" * len(prohibida)
                        # Reemplazamos
                        filtered_msg = filtered_msg.replace(prohibida, censura)
                
                # Notificamos si hubo cambios (opcional)
                if filtered_msg != original_msg:
                    client.conn.sendall(b"[SISTEMA]: Tu mensaje fue moderado por lenguaje inapropiado.\n")

                # Reenviar mensaje filtrado
                self._broadcast(f"{client.name}: {filtered_msg}\n", client)
                
            except OSError:
                break

        # Limpieza
        client.alive = False
        with self.lock:
            if client in self.clients: self.clients.remove(client)
        client.conn.close()

    def _broadcast(self, msg: str, sender: ClientState) -> None:
        """Envía a todos excepto al emisor."""
        data = msg.encode(ENCODING)
        with self.lock:
            for c in self.clients:
                if c is not sender and c.alive:
                    try:
                        c.conn.sendall(data)
                    except OSError:
                        c.alive = False

if __name__ == "__main__":
    FilterServer().start()
