"""Ejemplo: servidor que además guarda el historial en un archivo.

VARIACIÓN: Log-Archivo
PROPÓSITO: Todos los mensajes del chat se guardan en un archivo "chat_historial.txt"
para persistencia permanente (no solo en memoria).

DIFERENCIA CLAVE vs base:
- Base: _register_message() solo agrega a lista memory history.
- Esta: _register_message() además escribe a archivo LOG_PATH.
- Beneficio: Si el servidor se reinicia, historial se recupera.

Ejecuta:
  python Taller1-server-log-archivo.py
"""

from __future__ import annotations  # Anotaciones pospuestas (ej: list[str]).

import socket  # Módulo sockets para TCP/IP.
import threading  # Módulo de hilos (Barrier, Semaphore, Lock).
import time  # time.sleep() para pausas.
from dataclasses import dataclass, field  # Decoradores para clases de datos.
from datetime import datetime  # datetime.now() para timestamps.
from pathlib import Path  # Path() para manejo robusto de archivos.


# ===== CONSTANTES =====
# QUE HACE ESTA SECCIÓN: Define valores fijos usados en toda la aplicación.
# BENEFICIO: Cambiar estos valores centralizados sin buscar por todo el código.

HOST = "127.0.0.1"  # IP de escucha (localhost).
# - "127.0.0.1" = localhost (solo conexiones locales).
# - Uso: socket bind and listen en esta IP.

PORT = 50007  # Puerto TCP.
# - 50007 = número de puerto (> 1024 para no necesitar permisos admin).
# - Uso: clientes se conectan a (HOST, PORT).

ENCODING = "utf-8"  # Codificación de texto.
# - "utf-8" = estándar para textos (soporta español, emoji, etc).
# - Uso: str.encode(ENCODING) y bytes.decode(ENCODING).

BUFFER_SIZE = 2048  # Tamaño máximo de recv().
# - 2048 bytes = ~2 KB por mensaje.
# - Uso: socket.recv(BUFFER_SIZE) recibe hasta este tamaño.

LOG_PATH = Path("chat_historial.txt")  # Ruta del archivo de persistencia.
# - Path() = objeto que maneja rutas portátiles (Windows/Mac/Linux).
# - Uso: escribir/leer historial persistente.
# - DIFERENCIA: esto es NUEVO en esta variación (no existe en base).


# ===== CLASES DE DATOS =====

@dataclass
class ClientState:
    """Estado de un cliente conectado.

    Atributos:
        name: Nombre del cliente (string).
        conn: Socket TCP asociado al cliente.
        addr: Tupla (ip, puerto) remota.
        alive: Bandera de actividad.
    """

    name: str  # Nombre del cliente (ej: "Alice").
    conn: socket.socket  # Socket TCP (conexión al cliente).
    addr: tuple[str, int]  # Tupla (IP, puerto) remota.
    alive: bool = True  # Bandera (True = activo, False = desconectado).


@dataclass
class ChatServer:
    """Servidor con persistencia de historial EN ARCHIVO.

    VARIACIÓN: Esta clase es igual a la base, pero _register_message() ahora escribe
    a LOG_PATH (nueva funcionalidad).

    Atributos:
        host: IP de escucha (default: "127.0.0.1").
        port: Puerto de escucha (default: 50007).
        history: Lista con historial en memoria (para broadcasting).
        history_lock: Lock (Semaphore(1)) para proteger la historia.
        send_semaphore: Semáforo para serializar sendall() calls.
        in_flight_limit: BoundedSemaphore(5) para controlar carga concurrente.
        ready_barrier: Barrier(2) para sincronizar los 2 clientes al inicio.
        clients: Lista de ClientState (clientes conectados).
    """

    host: str = HOST  # IP de escucha.
    port: int = PORT  # Puerto de escucha.
    history: list[str] = field(default_factory=list)  # Historial en memoria.
    history_lock: threading.Lock = field(default_factory=threading.Lock)  # Lock para history.
    send_semaphore: threading.Semaphore = field(default_factory=lambda: threading.Semaphore(1))  # Serializa sends.
    in_flight_limit: threading.BoundedSemaphore = field(default_factory=lambda: threading.BoundedSemaphore(5))  # Controla carga.
    ready_barrier: threading.Barrier = field(default_factory=lambda: threading.Barrier(2))  # Sincroniza 2 clientes.
    clients: list[ClientState] = field(default_factory=list)  # Clientes conectados.

    def start(self) -> None:
        """Inicia el servidor TCP.

        FLUJO:
        1. Crea socket TCP en (host, port).
        2. Escucha conexiones (queue=2).
        3. Acepta 2 clientes en un loop.
        4. Para cada cliente, lanza hilo _handle_client().
        5. Mantiene servidor vivo mientras hay clientes activos.

        SINCRONIZACIÓN:
        - ready_barrier garantiza que ambos clientes llegan antes de chatear.
        - in_flight_limit evita que la carga crezca indefinidamente.
        - history_lock protege acceso a lista memory.
        """

        # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        # QUE HACE: Context manager que crea y cierra socket TCP.
        # - socket.AF_INET = IPv4.
        # - socket.SOCK_STREAM = TCP (confiable, orientado a conexión).
        # - with = asegura que close() se ejecute al salir.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            
            # server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # QUE HACE: Permite reutilizar el puerto (evita error "Address already in use").
            # - SOL_SOCKET = nivel socket.
            # - SO_REUSEADDR = opción.
            # - 1 = valor (habilitar).
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # server.bind((self.host, self.port))
            # QUE HACE: Vincula el socket a (IP, puerto).
            # - RESULTADO: socket escucha en 127.0.0.1:50007.
            server.bind((self.host, self.port))
            
            # server.listen(2)
            # QUE HACE: Pone el socket en modo escucha.
            # - ARGUMENTO: 2 = tamaño de la cola de conexiones pendientes.
            server.listen(2)
            
            # print(f"Servidor escuchando en {self.host}:{self.port}")
            # QUE HACE: Imprime log de arranque.
            print(f"Servidor escuchando en {self.host}:{self.port}")

            # while len(self.clients) < 2:
            # QUE HACE: Loop que acepta exactamente 2 clientes.
            # - len(self.clients) < 2 = mientras haya menos de 2 clientes.
            while len(self.clients) < 2:
                # conn, addr = server.accept()
                # QUE HACE: Bloquea hasta que un cliente se conecte.
                # - conn = socket conectado al cliente.
                # - addr = tupla (ip, puerto) del cliente.
                conn, addr = server.accept()
                
                # name = self._recv_name(conn)
                # QUE HACE: Solicita el nombre del cliente.
                # - VER: método _recv_name() más abajo.
                name = self._recv_name(conn)
                
                # if not name:
                # QUE HACE: Verifica si la lectura de nombre falló.
                if not name:
                    # conn.close()
                    # QUE HACE: Cierra la conexión.
                    conn.close()
                    # continue
                    # QUE HACE: Continúa el loop (intenta aceptar otro cliente).
                    continue
                
                # client = ClientState(name=name, conn=conn, addr=addr)
                # QUE HACE: Crea objeto ClientState con datos del cliente.
                # - name = nombre del cliente.
                # - conn = socket ya conectado.
                # - addr = (ip, puerto).
                # - alive = True (default).
                client = ClientState(name=name, conn=conn, addr=addr)
                
                # self.clients.append(client)
                # QUE HACE: Agrega el cliente a la lista de conectados.
                self.clients.append(client)
                
                # print(f"Conectado: {name} desde {addr}")
                # QUE HACE: Imprime log de conexión.
                print(f"Conectado: {name} desde {addr}")

                # thread = threading.Thread(...)
                # QUE HACE: Crea un hilo para atender a este cliente.
                # - target = función que ejecuta el hilo (_handle_client).
                # - args = argumentos pasados a target (client,).
                # - daemon = True (hilo muere si el proceso principal muere).
                thread = threading.Thread(
                    target=self._handle_client,
                    args=(client,),
                    daemon=True,
                )
                # thread.start()
                # QUE HACE: Inicia el hilo (comienza a ejecutar _handle_client).
                thread.start()

            # while any(c.alive for c in self.clients):
            # QUE HACE: Loop que mantiene el servidor vivo.
            # - any(...) = retorna True si al menos 1 condición es verdadera.
            # - c.alive for c in self.clients = genera booleans de cada cliente.
            # - EFECTO: mientras haya al menos 1 cliente vivo, continúa.
            while any(c.alive for c in self.clients):
                # time.sleep(0.2)
                # QUE HACE: Pausa 0.2 segundos (reduce CPU busy-wait).
                time.sleep(0.2)

        # print("Servidor finalizado.")
        # QUE HACE: Imprime cuando el servidor cierra.
        print("Servidor finalizado.")

    def _recv_name(self, conn: socket.socket) -> str:
        """Solicita el nombre del cliente y lo retorna.

        PROTOCOLO:
        1. Servidor envía "NOMBRE: ".
        2. Cliente recibe y envía nombre + salto de línea.
        3. Servidor recibe, decodifica y retorna.

        Args:
            conn: Socket del cliente ya conectado.

        Returns:
            Nombre del cliente (string) o "" si falla.
        """

        try:
            # conn.sendall("NOMBRE: ".encode(ENCODING))
            # QUE HACE: Envía solicitud de nombre al cliente.
            # - "NOMBRE: ".encode(ENCODING) = convierte str a bytes (UTF-8).
            # - sendall() = envía todos los bytes.
            conn.sendall("NOMBRE: ".encode(ENCODING))
            
            # raw = conn.recv(BUFFER_SIZE)
            # QUE HACE: Recibe la respuesta del cliente (nombre + salto de línea).
            # - recv(BUFFER_SIZE) = recibe hasta 2048 bytes.
            # - raw = bytes recibidos (ej: b'Alice\n').
            raw = conn.recv(BUFFER_SIZE)
            
            # return raw.decode(ENCODING).strip()
            # QUE HACE: Decodifica bytes a string, elimina espacios/newlines, y retorna.
            # - decode(ENCODING) = b'Alice\n' -> "Alice\n".
            # - .strip() = "Alice\n" -> "Alice".
            return raw.decode(ENCODING).strip()
        
        # except OSError:
        # QUE HACE: Captura errores de socket (conexión perdida, timeout).
        except OSError:
            # return ""
            # QUE HACE: Retorna cadena vacía si hay error.
            return ""

    def _handle_client(self, client: ClientState) -> None:
        """Hilo que atiende a un cliente individual.

        FLUJO:
        1. Espera en barrera (hasta que ambos clientes lleguen).
        2. Loop de recepción (lee mensajes del cliente).
        3. Si mensaje es "/exit", cierra la conexión.
        4. Sino, registra en historial y reenvía al otro cliente.
        5. Maneja sobrecarga con in_flight_limit (descarta si saturado).

        SINCRONIZACIÓN:
        - Barrera (ready_barrier): ambos hilos se sincronizan al inicio.
        - Semáforo (send_semaphore): serializa envios de broadcast.
        - Semáforo acotado (in_flight_limit): controla carga.
        - Lock (history_lock): protege acceso a history.

        Args:
            client: ClientState del cliente atendido por este hilo.
        """

        # try:
        # QUE HACE: Bloque de control de excepciones.
        try:
            # self.ready_barrier.wait()
            # QUE HACE: Espera en la barrera hasta que ambos clientes lleguen.
            # - Barrier(2) = requiere 2 threads.
            # - .wait() = bloquea hasta que ambos llamen a wait().
            # - EFECTO: ambos clientes avanzan simultáneamente.
            self.ready_barrier.wait()
            
            # self._send_to(client, "Ambos usuarios conectados. Puedes chatear.\n")
            # QUE HACE: Envía mensaje de bienvenida al cliente.
            # - VER: método _send_to() más abajo.
            self._send_to(client, "Ambos usuarios conectados. Puedes chatear.\n")
        
        # except threading.BrokenBarrierError:
        # QUE HACE: Captura error si la barrera se rompe (ej: otro cliente desconecta).
        except threading.BrokenBarrierError:
            # client.alive = False
            # QUE HACE: Marca el cliente como inactivo.
            client.alive = False
            # return
            # QUE HACE: Sale del hilo.
            return

        # while client.alive:
        # QUE HACE: Loop que continúa mientras el cliente esté activo.
        while client.alive:
            # try:
            # QUE HACE: Bloque de control de excepciones.
            try:
                # data = client.conn.recv(BUFFER_SIZE)
                # QUE HACE: Recibe datos del cliente (BLOQUEA).
                # - recv(BUFFER_SIZE) = recibe hasta 2048 bytes.
                # - data = bytes recibidos (ej: b'Hola').
                data = client.conn.recv(BUFFER_SIZE)
                
                # if not data:
                # QUE HACE: Verifica si recv() retornó 0 bytes (cliente cerró).
                if not data:
                    # break
                    # QUE HACE: Sale del loop (cliente desconectó).
                    break
                
                # message = data.decode(ENCODING).strip()
                # QUE HACE: Decodifica bytes a string y elimina espacios/newlines.
                # - decode(ENCODING) = b'Hola' -> "Hola".
                # - strip() = elimina espacios al inicio/final.
                message = data.decode(ENCODING).strip()
                
                # if message.lower() == "/exit":
                # QUE HACE: Verifica si el cliente envió comando de salida.
                # - message.lower() = convierte a minúsculas (para comparar).
                if message.lower() == "/exit":
                    # break
                    # QUE HACE: Sale del loop (cierra la conexión).
                    break

                # if not self.in_flight_limit.acquire(blocking=False):
                # QUE HACE: Intenta adquirir un permiso del semáforo acotado.
                # - in_flight_limit = BoundedSemaphore(5).
                # - acquire(blocking=False) = intenta sin esperar.
                # - Si retorna False, ya hay 5 mensajes en proceso.
                if not self.in_flight_limit.acquire(blocking=False):
                    # print(f"[DROP] {client.name}: sobrecarga controlada")
                    # QUE HACE: Imprime que se descartó el mensaje.
                    print(f"[DROP] {client.name}: sobrecarga controlada")
                    # continue
                    # QUE HACE: Salta a la siguiente iteración (descarta este mensaje).
                    continue

                # try:
                # QUE HACE: Bloque para asegurar que release() se ejecute.
                try:
                    # self._register_message(f"{client.name}: {message}")
                    # QUE HACE: Registra el mensaje en historial (y ahora TAMBIÉN en archivo).
                    # - VER: método _register_message() más abajo (DIFERENCIA CLAVE).
                    self._register_message(f"{client.name}: {message}")
                    
                    # self._broadcast(client, f"{client.name}: {message}\n")
                    # QUE HACE: Reenvía el mensaje al otro cliente.
                    # - VER: método _broadcast() más abajo.
                    self._broadcast(client, f"{client.name}: {message}\n")
                
                # finally:
                # QUE HACE: Bloque que SIEMPRE se ejecuta (incluso si hay excepción).
                finally:
                    # self.in_flight_limit.release()
                    # QUE HACE: Libera el permiso del semáforo acotado.
                    # - EFECTO: permite que otro mensaje entre en process.
                    self.in_flight_limit.release()

            # except OSError:
            # QUE HACE: Captura errores de socket (conexión perdida, etc).
            except OSError:
                # break
                # QUE HACE: Sale del loop.
                break

        # client.alive = False
        # QUE HACE: Marca el cliente como inactivo.
        client.alive = False
        
        # self._register_message(f"{client.name} se desconecto")
        # QUE HACE: Registra desconexión en historial (y archivo).
        self._register_message(f"{client.name} se desconecto")
        
        # print(f"Desconectado: {client.name}")
        # QUE HACE: Imprime log de desconexión.
        print(f"Desconectado: {client.name}")
        
        # try:
        # QUE HACE: Bloque de control de excepciones.
        try:
            # client.conn.close()
            # QUE HACE: Cierra el socket del cliente.
            client.conn.close()
        # except OSError:
        # QUE HACE: Ignora error al cerrar.
        except OSError:
            # pass
            # QUE HACE: No hacer nada.
            pass

    def _register_message(self, message: str) -> None:
        """Registra un mensaje en historial (memoria Y archivo).

        DIFERENCIA CLAVE EN ESTA VARIACIÓN:
        - Base: solo agrega a lista memory.
        - Esta: también escribe a LOG_PATH (chat_historial.txt) con timestamp.

        PROCEDIMIENTO:
        1. Agrega timestamp (ej: "2024-01-15 10:30:45").
        2. Crea línea formateada: "[timestamp] message".
        3. Protege con lock la lista memory.
        4. Imprime en consola.
        5. NUEVO: escribe al archivo LOG_PATH.

        Args:
            message: Texto ya formateado del mensaje (ej: "Alice: Hola").
        """

        # timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # QUE HACE: Obtiene la hora actual y formatea como string.
        # - datetime.now() = retorna objeto datetime actual.
        # - strftime("%Y-%m-%d %H:%M:%S") = formatea (ej: "2024-01-15 10:30:45").
        # - timestamp = string "2024-01-15 10:30:45".
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # line = f"[{timestamp}] {message}"
        # QUE HACE: Crea la línea formateada.
        # - f"..." = f-string.
        # - RESULTADO: "[2024-01-15 10:30:45] Alice: Hola".
        line = f"[{timestamp}] {message}"
        
        # with self.history_lock:
        # QUE HACE: Adquiere el lock antes de acceder a history.
        # - history_lock = Semaphore(1) (mutex).
        # - EFECTO: solo 1 hilo accede a la vez (exclusión mutua).
        with self.history_lock:
            # self.history.append(line)
            # QUE HACE: Agrega la línea al historial en memoria.
            self.history.append(line)
            
            # print(f"[HIST] {line}")
            # QUE HACE: Imprime en consola (prefijo [HIST] para identificar).
            print(f"[HIST] {line}")
            
            # LOG_PATH.write_text("\n".join(self.history) + "\n", encoding=ENCODING)
            # QUE HACE: Escribe TODO el historial al archivo (sobrescribe).
            # - DIFERENCIA: esto es la funcionalidad NUEVA.
            # - LOG_PATH = Path("chat_historial.txt").
            # - "\n".join(self.history) = une todas las líneas con saltos de línea.
            # - + "\n" = agrega salto de línea al final.
            # - write_text(..., encoding=ENCODING) = escribe al archivo en UTF-8.
            # - EFECTO: archivo se actualiza después de cada mensaje.
            # - NOTA: write_text() sobrescribe el archivo completo (no append).
            LOG_PATH.write_text("\n".join(self.history) + "\n", encoding=ENCODING)

    def _broadcast(self, sender: ClientState, message: str) -> None:
        """Reenvia mensaje a los demás clientes (excepto el remitente).

        PROPÓSITO:
        Envía el mensaje del cliente A al cliente B (si está vivo).
        Usa send_semaphore para serializar (solo 1 envio a la vez).

        Args:
            sender: ClientState del cliente que envió el mensaje.
            message: Texto ya formateado a reenvijar (ej: "Alice: Hola\n").
        """

        # with self.send_semaphore:
        # QUE HACE: Adquiere el semáforo antes de enviar.
        # - send_semaphore = Semaphore(1) (mutex).
        # - EFECTO: solo 1 hilo puede enviar a la vez.
        with self.send_semaphore:
            # for client in self.clients:
            # QUE HACE: Itera sobre todos los clientes conectados.
            for client in self.clients:
                # if client is sender or not client.alive:
                # QUE HACE: Verifica si es el remitente o está inactivo.
                # - client is sender = ¿es el mismo cliente que envió?.
                # - not client.alive = ¿está desconectado?.
                if client is sender or not client.alive:
                    # continue
                    # QUE HACE: Salta a la siguiente iteración (no envía).
                    continue
                # self._send_to(client, message)
                # QUE HACE: Envía el mensaje al cliente.
                # - VER: método _send_to() más abajo.
                self._send_to(client, message)

    def _send_to(self, client: ClientState, message: str) -> None:
        """Envío seguro a un cliente (marca como inactivo si falla).

        Args:
            client: ClientState del destinatario.
            message: Texto ya formateado en string.
        """

        try:
            # client.conn.sendall(message.encode(ENCODING))
            # QUE HACE: Envía el mensaje al cliente.
            # - message.encode(ENCODING) = convierte str a bytes (UTF-8).
            # - sendall() = envía todos los bytes (reintentos si es necesario).
            client.conn.sendall(message.encode(ENCODING))
        # except OSError:
        # QUE HACE: Captura error de socket.
        except OSError:
            # client.alive = False
            # QUE HACE: Marca el cliente como inactivo.
            # - EFECTO: el hilo _handle_client() saldrá del loop.
            client.alive = False


def main() -> None:
    """Punto de entrada del servidor con log a archivo.

    FLUJO:
    1. Crea instancia de ChatServer.
    2. Llama a start() para iniciar el loop de aceptación.
    """

    # ChatServer().start()
    # QUE HACE: Instancia ChatServer y llama a start().
    # - ChatServer() = crea instancia con valores default.
    # - .start() = inicia el servidor (loop de aceptación).
    ChatServer().start()


# if __name__ == "__main__":
# QUE HACE: Verifica si el script se ejecuta directamente (no importado).
# - __name__ = "__main__" si se ejecuta, "modulo_name" si se importa.
if __name__ == "__main__":
    # main()
    # QUE HACE: Llama a main() si se ejecuta directamente.
    main()
