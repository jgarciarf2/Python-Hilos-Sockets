"""Ejemplo: servidor con salas (rooms) con DOCUMENTACIÓN EXHAUSTIVA línea por línea.

PROPÓSITO:
- Permite que los clientes se agrupen en "salas" independientes.
- Un mensaje enviado en la Sala A NO lo reciben los de la Sala B.
- Demuestra el uso de diccionarios para organizar hilos/conexiones por categorías.

CONCEPTO CLAVE:
- Mapas (diccionarios): self.rooms = { "sala1": [cliente1, cliente2], "sala2": [...] }
- Aislamiento: El broadcast busca la lista de la sala específica en lugar de la lista global.

Ejecuta:
  python Taller1-server-salas.py
"""

# from __future__ import annotations
# - Permite el uso de tipos de clase dentro de la misma clase antes de ser definida completamente.
# - Ejemplo: usar ClientState como tipo dentro de ClientState.
from __future__ import annotations

# import socket
# - Biblioteca base para redes TCP/IP.
# - AF_INET = IPv4, SOCK_STREAM = TCP.
import socket

# import threading
# - Permite concurrencia mediante hilos.
# - Cada cliente tiene su propio hilo de atención para no bloquear a los demás.
import threading

# import time
# - Usado aquí para pausas o marcas de tiempo si fuera necesario.
import time

# from dataclasses import dataclass, field
# - Facilita la creación de clases que principalmente guardan datos.
# - field(default_factory=...) permite inicializar listas o dicts de forma segura en cada instancia.
from dataclasses import dataclass, field


# ==============================================================================
# CONFIGURACIÓN E INTERFAZ DE RED
# ==============================================================================

# HOST = "127.0.0.1"
# - Dirección de bucle local (localhost).
HOST = "127.0.0.1"

# PORT = 50007
# - Puerto arbitrario fuera del rango reservado (<1024).
PORT = 50007

# ENCODING = "utf-8"
# - Estándar para convertir strings a bytes.
ENCODING = "utf-8"

# BUFFER_SIZE = 2048
# - Tamaño máximo de cada paquete recibido.
BUFFER_SIZE = 2048


@dataclass
class ClientState:
    """Clase que encapsula toda la información de un usuario conectado.

    ATRIBUTOS:
    - name: Apodo del usuario.
    - room: Nombre de la sala a la que pertenece.
    - conn: El objeto socket activo para enviar/recibir.
    - addr: Tupla (IP, Puerto) de origen.
    - alive: Controla si el hilo de atención debe seguir activo.
    """

    # name: str
    # - Identificador textual del usuario.
    name: str
    
    # room: str
    # - Identificador de la sala de chat.
    room: str
    
    # conn: socket.socket
    # - Conexión TCP abierta.
    conn: socket.socket
    
    # addr: tuple[str, int]
    # - Dirección física (ej: 127.0.0.1, 55432).
    addr: tuple[str, int]
    
    # alive: bool = True
    # - Bandera booleana para terminar el bucle de escucha.
    alive: bool = True


@dataclass
class ChatServer:
    """Servidor que gestiona múltiples salas de chat simultáneas.

    LÓGICA:
    - En lugar de una lista plana de clientes, usa un diccionario 'rooms'.
    - Al entrar, el servidor pregunta: "¿A qué sala quieres entrar?".
    - El broadcast se limita a los miembros de esa clave en el diccionario.
    """

    # host: str = HOST
    # - IP de escucha (local).
    host: str = HOST
    
    # port: int = PORT
    # - Puerto de escucha.
    port: int = PORT
    
    # send_semaphore: threading.Semaphore = field(...)
    # - Semáforo de valor 1 (Mutex).
    # - Garantiza que solo un hilo a la vez acceda al socket para enviar datos.
    # - Evita que mensajes de distintos clientes se "pisen" o mezclen bytes.
    send_semaphore: threading.Semaphore = field(default_factory=lambda: threading.Semaphore(1))
    
    # rooms: dict[str, list[ClientState]] = field(...)
    # - Estructura central: La llave es el nombre de la sala (str).
    # - El valor es una lista de objetos ClientState.
    rooms: dict[str, list[ClientState]] = field(default_factory=dict)

    def start(self) -> None:
        """Punto de inicio del servidor. Configura el socket maestro."""

        # with socket.socket(...) as server:
        # - Crea el socket del servidor.
        # - AF_INET: IPv4.
        # - SOCK_STREAM: Protocolo TCP orientado a conexión.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            # server.setsockopt(...)
            # - SO_REUSEADDR: Permite reiniciar el servidor inmediatamente si se cierra.
            # - Evita el error "Address already in use" durante reinicios rápidos.
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # server.bind((self.host, self.port))
            # - Vincula el proceso al puerto 50007.
            server.bind((self.host, self.port))
            
            # server.listen(4)
            # - Prepara al servidor para aceptar conexiones (cola de espera de 4).
            server.listen(4)
            
            # print(...)
            # - Log de inicio en consola.
            print(f"Servidor de SALAS escuchando en {self.host}:{self.port}")

            # while True:
            # - Bucle infinito para aceptar clientes uno tras otro.
            while True:
                # conn, addr = server.accept()
                # - Bloquea la ejecución hasta que llega un nuevo cliente.
                # - conn: nuevo socket para hablar con ese cliente específico.
                # - addr: dirección del cliente.
                conn, addr = server.accept()
                
                # name, room = self._recv_identity(conn)
                # - Función auxiliar que realiza el "handshake" inicial (pide nombre y sala).
                name, room = self._recv_identity(conn)
                
                # if not name or not room:
                # - Si el cliente se desconecta o envía datos vacíos durante el handshake.
                if not name or not room:
                    conn.close() # Cierra conexión fallida.
                    continue    # Vuelve a esperar al siguiente cliente.
                
                # client = ClientState(...)
                # - Crea el objeto de estado para el nuevo usuario.
                client = ClientState(name=name, room=room, conn=conn, addr=addr)
                
                # self.rooms.setdefault(room, []).append(client)
                # - setdefault: si la sala no existe en el dict, crea una lista vacía [].
                # - append: añade al cliente a la lista de esa sala en específico.
                self.rooms.setdefault(room, []).append(client)
                
                # print(...)
                # - Log del servidor para seguimiento.
                print(f" [+] {name} entró a la sala [{room}] desde {addr}")

                # thread = threading.Thread(...)
                # - Crea un hilo dedicado para gestionar los mensajes de este cliente.
                # - target: Función a ejecutar.
                # - args: Parámetros de la función.
                # - daemon=True: El hilo muere automáticamente si el servidor se apaga.
                thread = threading.Thread(
                    target=self._handle_client,
                    args=(client,),
                    daemon=True,
                )
                
                # thread.start()
                # - Inicia la ejecución del hilo de forma asíncrona.
                thread.start()

    def _recv_identity(self, conn: socket.socket) -> tuple[str, str]:
        """Protocolo inicial para identificar al usuario y su sala deseada.

        FLUJO:
        1. Envía "NOMBRE: "
        2. Recibe respuesta.
        3. Envía "SALA: "
        4. Recibe respuesta.
        """

        # try/except OSError:
        # - Captura fallos de red si el cliente se desconecta antes de identificarse.
        try:
            # conn.sendall(...)
            # - Envía el recordatorio al cliente.
            conn.sendall("NOMBRE: ".encode(ENCODING))
            
            # name = conn.recv(...).decode(...).strip()
            # - Recibe los bytes, los decodifica a texto y limpia espacios/saltos de línea.
            name = conn.recv(BUFFER_SIZE).decode(ENCODING).strip()
            
            # conn.sendall(...)
            # - Envía el segundo recordatorio.
            conn.sendall("SALA: ".encode(ENCODING))
            
            # room = conn.recv(...).decode(...).strip()
            # - Recibe el nombre de la sala deseada.
            room = conn.recv(BUFFER_SIZE).decode(ENCODING).strip()
            
            # return name, room
            # - Devuelve la tupla de strings.
            return name, room
            
        except OSError:
            # return "", ""
            # - Indica fallo en la identificación.
            return "", ""

    def _handle_client(self, client: ClientState) -> None:
        """Bucle de escucha para un cliente específico en su hilo dedicado.

        Args:
            client: El objeto con la conexión y la sala asignada.
        """

        # while client.alive:
        # - Mantiene la escucha activa mientras el cliente no se desconecte o cierre.
        while client.alive:
            try:
                # data = client.conn.recv(BUFFER_SIZE)
                # - Bloquea el hilo esperando datos de este usuario.
                data = client.conn.recv(BUFFER_SIZE)
                
                # if not data:
                # - Si recv devuelve 0 bytes, significa desconexión.
                if not data:
                    break
                
                # message = data.decode(ENCODING).strip()
                # - Convierte los bytes en string legible.
                message = data.decode(ENCODING).strip()
                
                # if message.lower() == "/exit":
                # - Comando estándar para salir del chat de forma amable.
                if message.lower() == "/exit":
                    break
                
                # self._broadcast_room(...)
                # - Envía el mensaje SOLO a los de su misma sala.
                # - Formato: "Nombre: Mensaje"
                self._broadcast_room(client, f"[{client.room}] {client.name}: {message}\n")
                
            except OSError:
                # break
                # - Sale si hay cualquier error de red inesperado.
                break

        # client.alive = False
        # - Marca como inactivo antes de la limpieza.
        client.alive = False
        
        # try: client.conn.close()
        # - Libera el socket para el sistema operativo.
        try:
            client.conn.close()
        except OSError:
            pass
        
        # print(...)
        # - Log de desconexión.
        print(f" [-] {client.name} salió de la sala {client.room}")

    def _broadcast_room(self, sender: ClientState, message: str) -> None:
        """Envía un mensaje únicamente a los usuarios de la sala del emisor.

        LÓGICA:
        1. Adquiere semáforo de envío (exclusión mutua).
        2. Obtiene la lista de clientes de la sala 'sender.room'.
        3. Recorre la lista y envía el mensaje a todos excepto al emisor.
        """

        # with self.send_semaphore:
        # - Asegura que los envíos sean serializados y no se mezclen.
        with self.send_semaphore:
            # target_room_list = self.rooms.get(sender.room, [])
            # - Obtiene la lista de personas en esa sala.
            # - Si la sala no existe (raro), devuelve lista vacía [].
            target_room_list = self.rooms.get(sender.room, [])
            
            # for client in target_room_list:
            # - Itera sobre todos los compañeros de sala.
            for client in target_room_list:
                # if client is sender or not client.alive:
                # - No se envía el mensaje a quien lo mandó.
                # - No se envía a clientes marcados como inactivos.
                if client is sender or not client.alive:
                    continue
                
                # try/except OSError:
                # - Maneja errores durante el envío (ej: cliente que se desconectó de golpe).
                try:
                    # client.conn.sendall(...)
                    # - Transmisión efectiva de los datos.
                    client.conn.sendall(message.encode(ENCODING))
                    
                except OSError:
                    # client.alive = False
                    # - Marca para eliminación si el socket falló.
                    client.alive = False


def main() -> None:
    """Instancia el servidor y lo pone en marcha."""

    # ChatServer().start()
    # - Constructor por defecto (usa HOST y PORT globales).
    # - Inicia el método bloqueante principal.
    ChatServer().start()


# if __name__ == "__main__":
# - Estándar de Python para scripts ejecutables.
if __name__ == "__main__":
    # main()
    # - Arranca el programa.
    main()

