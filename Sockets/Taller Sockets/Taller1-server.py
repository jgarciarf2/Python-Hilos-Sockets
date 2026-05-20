"""Servidor del chat bidireccional con documentación exhaustiva línea por línea.

Este archivo implementa un servidor TCP que:
1. Acepta exactamente 2 clientes conectados simultáneamente.
2. Usa una BARRERA para sincronizar el inicio (ambos clientes listos).
3. Recibe mensajes de cada cliente y los reenvia al otro (bidireccional).
4. Mantiene un historial en memoria de todos los mensajes.
5. Usa SEMÁFOROS para evitar condiciones de carrera y controlar sobrecarga.
6. Usa HILOS para manejar múltiples clientes de forma concurrente.

Conceptos clave:
- BARRERA: Detiene hilos hasta que N participantes lleguen a wait().
- SEMÁFORO: Contador que limita acceso a recursos compartidos.
- LOCK: Exclusión mutua simple (semáforo con 1 permiso).
- HILO: Ejecución concurrente de código en el mismo proceso.

Ejecuta:
    python Taller1-server.py
"""

# LÍNEA 1: Habilita anotaciones de tipo pospuestas para mejor legibilidad.
# Ejemplo: list[str] en lugar de typing.List[str].
# Requiere Python 3.10+ o "from __future__ import annotations".
from __future__ import annotations

# LÍNEA 2: Módulo socket proporciona la API TCP/IP.
# Socket = punto de conexión de red que envía/recibe datos.
# AF_INET = IPv4 (internet), SOCK_STREAM = TCP (flujo confiable).
import socket

# LÍNEA 3: Módulo threading proporciona hilos y primitivas de sincronización.
# threading.Thread = hilo de ejecución.
# threading.Lock = mutex (exclusión mutua).
# threading.Semaphore = contador protegido.
# threading.Barrier = sincronizador que bloquea hasta N llamadas a wait().
import threading

# LÍNEA 4: Módulo time proporciona funciones de tiempo.
# time.sleep(0.2) = duerme 0.2 segundos (evita loops agresivos).
import time

# LÍNEA 5: Módulo dataclasses proporciona decorador @dataclass.
# @dataclass genera __init__, __repr__, __eq__ automáticamente.
# field() permite valores por defecto complejos.
from dataclasses import dataclass, field


# CONSTANTES GLOBALES

# HOST: La dirección IP donde el servidor escucha.
# 127.0.0.1 = localhost (solo máquina local).
# 0.0.0.0 = escuchar en todas las interfaces de red.
HOST = "127.0.0.1"

# PORT: El puerto TCP donde el servidor escucha.
# Puertos 0-1023 requieren permisos de administrador.
# Puertos 1024-65535 pueden usarse sin permisos especiales.
# 50007 es un puerto arbitrario para pruebas.
PORT = 50007

# ENCODING: Codificación para convertir entre str (texto) y bytes.
# UTF-8 = estándar universal que soporta todos los idiomas y símbolos.
# Ejemplo: "Hola".encode("utf-8") = b'Hola' (bytes).
# Ejemplo: b'Hola'.decode("utf-8") = "Hola" (str).
ENCODING = "utf-8"

# BUFFER_SIZE: Cantidad máxima de bytes que socket.recv() lee de una vez.
# 2048 bytes = 2 KB, típico para aplicaciones de chat.
# Si el mensaje es más grande, se recibe en múltiples recv().
BUFFER_SIZE = 2048


# CLASE 1: ClientState
# Propósito: Encapsular el estado de un cliente conectado en un solo objeto.
# Beneficio: Fácil pasar un cliente entre funciones sin pasar 4 parámetros.

# @dataclass = decorador que genera __init__ automáticamente.
# Ejemplo: @dataclass genera: __init__(self, name, conn, addr, alive=True)
@dataclass
class ClientState:
    """Encapsula el estado de un cliente conectado al servidor.

    ATRIBUTOS:
    
    name (str):
        El nombre identificador del cliente. Se usa para etiquetar mensajes.
        Ejemplo: "Alice" -> mensajes mostrados como "Alice: Hola mundo".
        Rango típico: 1-32 caracteres alfanuméricos.
    
    conn (socket.socket):
        El objeto socket TCP conectado al cliente.
        Se usa para:
            - conn.sendall(bytes) = enviar datos.
            - conn.recv(bytes) = recibir datos.
            - conn.close() = cerrar la conexión.
        Es único por cliente.
    
    addr (tuple[str, int]):
        La tupla (dirección_ip_remota, puerto_remoto) del cliente.
        Ejemplo: ("192.168.1.100", 54321)
        Se usa para logging y debugging.
        No se modifica después de conectar.
    
    alive (bool):
        Bandera que indica si el cliente está activo.
        Valor inicial: True (cliente conectado).
        Se pone False cuando:
            - El cliente envía "/exit".
            - La conexión se rompe.
            - Hay error al recibir/enviar.
        Se usa en loops: while client.alive: para salir cuando sea necesario.
    """

    # name: es un str, ej: "Alice"
    name: str

    # conn: es un socket.socket, ej: <socket.socket object>
    conn: socket.socket

    # addr: es una tupla (str, int), ej: ("127.0.0.1", 12345)
    addr: tuple[str, int]

    # alive: es un bool, valor por defecto True
    alive: bool = True


# CLASE 2: ChatServer
# Propósito: Orquestar la lógica central del servidor de chat.
# Responsabilidades:
#   1. Crear el socket y escuchar conexiones.
#   2. Aceptar exactamente 2 clientes.
#   3. Lanzar hilos para cada cliente.
#   4. Manejar sincronización y recursos compartidos.

# @dataclass = genera __init__ con todos los parámetros nombrados.
@dataclass
class ChatServer:
    """Servidor de chat bidireccional con sincronización y control de carga.

    RESPONSABILIDADES:
    
    1. start(): Aceptar conexiones y lanzar hilos.
    2. _recv_name(conn): Solicitar nombre al cliente.
    3. _handle_client(client): Hilo que atiende un cliente.
    4. _register_message(msg): Guardar en historial.
    5. _broadcast(sender, msg): Reenviar al otro cliente.
    6. _send_to(client, msg): Enviar seguro a un cliente.

    ATRIBUTOS:

    host (str):
        IP donde escucha el servidor.
        Valor típico: "127.0.0.1" (localhost).
        Rango: cualquier IPv4 válida.

    port (int):
        Puerto TCP donde escucha.
        Valor típico: 50007 (arbitrario para pruebas).
        Rango válido: 1024-65535 (sin permisos de admin).

    history (list[str]):
        Lista que mantiene el historial de todos los mensajes.
        Acceso: múltiples hilos pueden leer/escribir simultáneamente.
        Protección: history_lock evita corrupción de datos.
        Crecimiento: sin límite (en producción, habría límite).

    history_lock (threading.Lock):
        Mutex (exclusión mutua) que protege 'history'.
        Lock = semáforo con 1 permiso.
        Funciona con: with history_lock: garantiza exclusión.
        Problema que evita: dos hilos escribiendo history simultáneamente.

    send_semaphore (threading.Semaphore):
        Semáforo con 1 permiso que serializa envíos.
        Funciona con: with send_semaphore: garantiza 1 hilo a la vez.
        Protege: evita que dos hilos escriban al socket del cliente destino simultáneamente.
        Problema que evita: mensajes entrelazados en el buffer del socket destino.

    in_flight_limit (threading.BoundedSemaphore):
        Semáforo acotado con 5 permisos.
        Limita: cantidad de mensajes siendo procesados simultáneamente.
        Funciona: acquire(blocking=False) retorna False si no hay permisos.
        Problema que evita: spammer saturar el servidor descartando mensajes.

    ready_barrier (threading.Barrier):
        Barrera que sincroniza 2 participantes (los 2 clientes).
        Funciona: barrier.wait() bloquea hasta que 2 hilos lleguen.
        Problema que resuelve: evita que un cliente envíe antes que el otro esté conectado.
        Excepción: BrokenBarrierError si un hilo se desconecta en wait().

    clients (list[ClientState]):
        Lista de clientes conectados.
        Tamaño: siempre 0, 1, o 2 en este servidor.
        Acceso: múltiples hilos leen, pero solo main() escribe.
        Iteración: for client in self.clients para enviar a todos.
    """

    # host: str, valor por defecto HOST ("127.0.0.1")
    host: str = HOST

    # port: int, valor por defecto PORT (50007)
    port: int = PORT

    # history: list[str], comienza vacía, se inicializa con default_factory=list
    # default_factory=list garantiza que cada instancia obtiene su propia lista (no compartida).
    history: list[str] = field(default_factory=list)

    # history_lock: threading.Lock, se crea una instancia nueva por cada ChatServer.
    # default_factory=threading.Lock llama a threading.Lock() durante __init__.
    history_lock: threading.Lock = field(default_factory=threading.Lock)

    # send_semaphore: threading.Semaphore(1), serializa envíos.
    # default_factory=lambda: threading.Semaphore(1) crea un semáforo con 1 permiso.
    send_semaphore: threading.Semaphore = field(default_factory=lambda: threading.Semaphore(1))

    # in_flight_limit: threading.BoundedSemaphore(5), limita carga.
    # default_factory=lambda: threading.BoundedSemaphore(5) crea un semáforo con 5 permisos.
    in_flight_limit: threading.BoundedSemaphore = field(default_factory=lambda: threading.BoundedSemaphore(5))

    # ready_barrier: threading.Barrier(2), sincroniza 2 clientes.
    # default_factory=lambda: threading.Barrier(2) crea una barrera para 2 participantes.
    ready_barrier: threading.Barrier = field(default_factory=lambda: threading.Barrier(2))

    # clients: list[ClientState], comienza vacía.
    # Se llena conforme se conectan clientes (máximo 2).
    clients: list[ClientState] = field(default_factory=list)

    def start(self) -> None:
        """Inicia el servidor y acepta dos conexiones.

        Flujo:
            1) Crea y configura el socket del servidor.
            2) Acepta dos clientes, pide nombre y crea un hilo por cliente.
            3) Mantiene el proceso vivo mientras existan clientes activos.
        """  # Doc del metodo.

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:  # Socket TCP.
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Reusar puerto.
            server.bind((self.host, self.port))  # Asigna IP y puerto.
            server.listen(2)  # Cola de hasta 2 conexiones.
            print(f"Servidor escuchando en {self.host}:{self.port}")  # Log inicial.

            while len(self.clients) < 2:  # Acepta exactamente 2 clientes.
                conn, addr = server.accept()  # Espera conexion.
                name = self._recv_name(conn)  # Solicita nombre.
                if not name:  # Si no hay nombre, se descarta.
                    conn.close()  # Cierra socket.
                    continue  # Vuelve a esperar.
                client = ClientState(name=name, conn=conn, addr=addr)  # Crea estado.
                self.clients.append(client)  # Guarda cliente.
                print(f"Conectado: {name} desde {addr}")  # Log.

                thread = threading.Thread(  # Hilo por cliente.
                    target=self._handle_client,  # Manejo de mensajes.
                    args=(client,),  # Pasa el cliente.
                    daemon=True,  # Hilo se cierra con el proceso.
                )
                thread.start()  # Inicia el hilo.

            # Mantiene el servidor vivo hasta que todos salgan.
            while any(c.alive for c in self.clients):  # Revisa clientes activos.
                time.sleep(0.2)  # Evita loop agresivo.

        print("Servidor finalizado.")  # Log final.

    def _recv_name(self, conn: socket.socket) -> str:
        """Solicita el nombre al cliente para identificarlo en el chat.

        Args:
            conn: Socket del cliente ya conectado.

        Returns:
            El nombre recibido como str, o cadena vacia si falla.
        """  # Doc breve.

        try:  # Control de errores de red.
            conn.sendall("NOMBRE: ".encode(ENCODING))  # Pide nombre.
            raw = conn.recv(BUFFER_SIZE)  # Lee respuesta.
            return raw.decode(ENCODING).strip()  # Devuelve limpio.
        except OSError:  # Error de socket.
            return ""  # Sin nombre.

    def _handle_client(self, client: ClientState) -> None:
        """Escucha mensajes de un cliente y los reenvia al otro.

        Args:
            client: Estado del cliente que este hilo administra.

        Efectos:
            - Espera la barrera de inicio.
            - Recibe mensajes y los reenvia al otro cliente.
            - Registra historial y maneja desconexion.
        """  # Doc breve.

        try:  # Barrera puede lanzar error.
            # Barrera: espera a que ambos clientes esten listos.
            self.ready_barrier.wait()  # Sincroniza el inicio.
            self._send_to(client, "Ambos usuarios conectados. Puedes chatear.\n")  # Aviso.
        except threading.BrokenBarrierError:  # Si algun cliente falla.
            client.alive = False  # Marca como inactivo.
            return  # Sale del hilo.

        while client.alive:  # Loop de mensajes.
            try:  # Control de errores de socket.
                data = client.conn.recv(BUFFER_SIZE)  # Espera mensaje.
                if not data:  # Socket cerrado.
                    break  # Sale del loop.
                message = data.decode(ENCODING).strip()  # Convierte a texto.
                if message.lower() == "/exit":  # Comando de salida.
                    break  # Termina el chat.

                # Controla la sobrecarga limitando mensajes en vuelo.
                if not self.in_flight_limit.acquire(blocking=False):  # Intenta tomar cupo.
                    print(f"[DROP] {client.name}: sobrecarga controlada")  # Log drop.
                    continue  # Ignora el mensaje.

                try:  # Protege liberacion del semaforo.
                    self._register_message(f"{client.name}: {message}")  # Guarda historial.
                    self._broadcast(client, f"{client.name}: {message}\n")  # Reenvia.
                finally:
                    self.in_flight_limit.release()  # Libera cupo.

            except OSError:  # Error de socket.
                break  # Sale del loop.

        client.alive = False  # Marca desconexion.
        self._register_message(f"{client.name} se desconecto")  # Historial.
        print(f"Desconectado: {client.name}")  # Log.
        try:
            client.conn.close()  # Cierra socket.
        except OSError:
            pass  # Ignora error al cerrar.

    def _register_message(self, message: str) -> None:
        """Agrega un mensaje al historial y lo imprime en servidor.

        Args:
            message: Texto ya formateado para guardar en historial.
        """  # Doc.

        with self.history_lock:  # Protege historial compartido.
            self.history.append(message)  # Agrega al historial.
            print(f"[HIST] {message}")  # Muestra en consola.

    def _broadcast(self, sender: ClientState, message: str) -> None:
        """Reenvia el mensaje al cliente opuesto con un semaforo de envio.

        Args:
            sender: Cliente que origina el mensaje.
            message: Texto a reenviar al resto de clientes.
        """  # Doc.

        with self.send_semaphore:  # Evita envios simultaneos.
            for client in self.clients:  # Itera clientes.
                if client is sender or not client.alive:  # Salta emisor o inactivos.
                    continue  # No se envia.
                self._send_to(client, message)  # Envia mensaje.

    def _send_to(self, client: ClientState, message: str) -> None:
        """Envio protegido para un cliente.

        Args:
            client: Destinatario del mensaje.
            message: Texto en claro que sera codificado y enviado.
        """  # Doc.

        try:
            client.conn.sendall(message.encode(ENCODING))  # Envia bytes.
        except OSError:  # Error al enviar.
            client.alive = False  # Marca inactivo.


def main() -> None:
    """Punto de entrada del servidor.

    Crea una instancia con valores por defecto y ejecuta el loop principal.
    """

    ChatServer().start()  # Inicia servidor con valores por defecto.


if __name__ == "__main__":
    main()  # Entrada principal del script.
