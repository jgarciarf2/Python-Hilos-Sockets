"""Ejemplo: servidor con Mensajes Privados (/whisper) con DOCUMENTACIÓN EXHAUSTIVA.

PROPÓSITO:
- Permite enviar mensajes a una persona específica sin que los demás lo vean.
- Implementa lógica de comandos dentro del flujo de mensajes.
- Sintaxis: "/whisper Juan Hola, ¿cómo vas?"

CONCEPTO CLAVE:
- Búsqueda en listas: El servidor busca al destinatario por su atributo 'name' en la lista global.
- Mensajes directos: Se usa 'sendall' específicamente sobre el socket del destinatario encontrado.

Ejecuta:
  python Taller1-server-pvt.py
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

@dataclass
class ClientState:
    """Guarda la conexión y el nombre de cada usuario."""
    name: str
    conn: socket.socket
    addr: tuple[str, int]
    alive: bool = True

@dataclass
class ChatServer:
    """Servidor que detecta el comando /whisper para mensajes privados."""

    host: str = HOST
    port: int = PORT
    
    # clients: list[ClientState]
    # - Mantiene la lista de todos los usuarios conectados para poder buscarlos.
    clients: list[ClientState] = field(default_factory=list)
    
    # clients_lock: threading.Lock
    # - Lock de exclusión mutua para proteger la lista 'clients'.
    # - Evita que un hilo añada un cliente mientras otro lo borra (Race Conditions).
    clients_lock: threading.Lock = field(default_factory=threading.Lock)

    def start(self) -> None:
        """Inicia el servidor y acepta clientes."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.host, self.port))
            server.listen(5)
            print(f"Servidor de Privados escuchando en {self.host}:{self.port}")

            while True:
                conn, addr = server.accept()
                
                # Realizamos el handshake para obtener el nombre
                conn.sendall("NOMBRE: ".encode(ENCODING))
                name = conn.recv(BUFFER_SIZE).decode(ENCODING).strip()
                
                if not name:
                    conn.close()
                    continue
                
                client = ClientState(name=name, conn=conn, addr=addr)
                
                # Bloqueamos la lista para añadir al nuevo cliente de forma segura
                with self.clients_lock:
                    self.clients.append(client)
                
                print(f" [+] {name} conectado desde {addr}")
                
                # Iniciamos hilo de atención
                threading.Thread(target=self._handle_client, args=(client,), daemon=True).start()

    def _handle_client(self, client: ClientState) -> None:
        """Gestiona los mensajes del cliente y detecta comandos."""
        
        while client.alive:
            try:
                data = client.conn.recv(BUFFER_SIZE)
                if not data: break
                
                message = data.decode(ENCODING).strip()
                
                # --- LÓGICA DE COMANDOS ---
                
                # startsWith("/whisper"):
                # - Detecta si el mensaje es una petición de mensaje privado.
                if message.startswith("/whisper"):
                    self._handle_whisper(client, message)
                
                elif message.lower() == "/exit":
                    break
                
                else:
                    # Mensaje público normal
                    self._broadcast(f"{client.name}: {message}\n", exclude_client=client)
                    
            except OSError:
                break

        # Limpieza al salir
        client.alive = False
        with self.clients_lock:
            if client in self.clients:
                self.clients.remove(client)
        client.conn.close()
        print(f" [-] {client.name} desconectado.")

    def _handle_whisper(self, sender: ClientState, full_message: str) -> None:
        """Analiza la cadena para extraer destinatario y contenido.
        
        Sintaxis esperada: "/whisper <nombre> <mensaje>"
        """
        
        # parts = full_message.split(" ", 2)
        # - Divide el string: ["/whisper", "nombre", "el mensaje restante"]
        # - El tercer parámetro (2) limita a tres fragmentos máximos.
        parts = full_message.split(" ", 2)
        
        # Validamos que tenga al menos 3 partes
        if len(parts) < 3:
            sender.conn.sendall("Error: Usa /whisper <nombre> <mensaje>\n".encode(ENCODING))
            return
        
        target_name = parts[1]
        content = parts[2]
        
        # Buscamos al destinatario en la lista global
        target_client = None
        with self.clients_lock:
            for c in self.clients:
                if c.name.lower() == target_name.lower() and c.alive:
                    target_client = c
                    break
        
        if target_client:
            # Enviamos el secreto solo a él
            pvt_msg = f"[PRIVADO de {sender.name}]: {content}\n"
            try:
                target_client.conn.sendall(pvt_msg.encode(ENCODING))
                # Confirmación al emisor
                sender.conn.sendall(f"Susurraste a {target_name}: {content}\n".encode(ENCODING))
            except OSError:
                sender.conn.sendall(f"Error: No se pudo enviar el mensaje a {target_name}.\n".encode(ENCODING))
        else:
            sender.conn.sendall(f"Error: El usuario '{target_name}' no existe o está offline.\n".encode(ENCODING))

    def _broadcast(self, message: str, exclude_client: ClientState = None) -> None:
        """Envía mensaje a todos los que NO sean el emisor."""
        data = message.encode(ENCODING)
        with self.clients_lock:
            for c in self.clients:
                if c is not exclude_client and c.alive:
                    try:
                        c.conn.sendall(data)
                    except OSError:
                        c.alive = False

if __name__ == "__main__":
    ChatServer().start()
