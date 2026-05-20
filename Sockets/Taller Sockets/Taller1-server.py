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
        """

        # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        # QUE HACE: Crea un socket TCP para IPv4.
        # - socket.AF_INET = IPv4 (Address Family Internet).
        # - socket.SOCK_STREAM = TCP (confiable, orientado a conexión).
        # - with = context manager que asegura close() al salir.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            # server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # QUE HACE: Permite reusar el puerto inmediatamente después de cerrar.
            # - Evita el error "Address already in use" si reiniciamos rápido.
            # - Sin esto hay que esperar ~60 segundos (TIME_WAIT del SO).
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # server.bind((self.host, self.port))
            # QUE HACE: Vincula el socket a una dirección IP y puerto específico.
            # - ARGUMENTOS: (self.host="127.0.0.1", self.port=50007).
            # - El SO sabe que este socket escucha en 127.0.0.1:50007.
            server.bind((self.host, self.port))
            
            # server.listen(2)
            # QUE HACE: Prepara el socket para aceptar conexiones.
            # - ARGUMENTO: 2 = tamaño máximo de la cola (hasta 2 conexiones pendientes).
            server.listen(2)
            
            # print(f"Servidor escuchando en {self.host}:{self.port}")
            # QUE HACE: Imprime en consola que el servidor está activo.
            print(f"Servidor escuchando en {self.host}:{self.port}")

            # while len(self.clients) < 2:
            # QUE HACE: Loop que acepta exactamente 2 clientes.
            while len(self.clients) < 2:
                # conn, addr = server.accept()
                # QUE HACE: server.accept() bloquea esperando a un cliente.
                # - RETORNA: (socket_del_cliente, (ip_remota, puerto_remoto)).
                # - conn = socket único para comunicarse con este cliente.
                # - addr = tuple (ip, puerto) del cliente.
                conn, addr = server.accept()
                
                # name = self._recv_name(conn)
                # QUE HACE: Llama a _recv_name(conn) para solicitar el nombre del cliente.
                # - ARGUMENTO: conn = socket ya conectado del cliente.
                # - RETORNA: nombre (str) o "" si falla.
                # - VER: método _recv_name() más abajo para detalles.
                name = self._recv_name(conn)
                
                # if not name:
                # QUE HACE: Verifica si el nombre es vacío (conexión falló).
                if not name:
                    # conn.close()
                    # QUE HACE: Cierra el socket de este cliente rechazado.
                    conn.close()
                    
                    # continue
                    # QUE HACE: Salta al siguiente accept(), espera otro cliente.
                    continue
                
                # client = ClientState(name=name, conn=conn, addr=addr)
                # QUE HACE: Crea un objeto que encapsula los datos del cliente.
                # - @dataclass genera __init__ automáticamente.
                # - ARGUMENTOS: name (str), conn (socket), addr (tuple).
                # - alive (bool) se pone True por defecto.
                client = ClientState(name=name, conn=conn, addr=addr)
                
                # self.clients.append(client)
                # QUE HACE: Agrega el cliente a la lista de clientes.
                # - Aumenta len(self.clients) en 1.
                # - Cuando len(self.clients) == 2, el while termina.
                self.clients.append(client)
                
                # print(f"Conectado: {name} desde {addr}")
                # QUE HACE: Imprime un log de que el cliente se conectó.
                print(f"Conectado: {name} desde {addr}")

                # thread = threading.Thread(target=self._handle_client, args=(client,), daemon=True)
                # QUE HACE: Crea un hilo que ejecutará _handle_client(client).
                # - target = función a ejecutar: self._handle_client.
                # - args = argumentos para la función: (client,).
                # - daemon = True: el hilo muere cuando el proceso principal muere.
                thread = threading.Thread(
                    target=self._handle_client,
                    args=(client,),
                    daemon=True,
                )
                
                # thread.start()
                # QUE HACE: Inicia el hilo (comienza a ejecutar _handle_client(client)).
                # - No es bloqueante, retorna inmediatamente.
                # - El hilo ejecuta en paralelo (concurrencia).
                thread.start()

            # while any(c.alive for c in self.clients):
            # QUE HACE: Loop que mantiene el servidor vivo mientras haya clientes.
            # - any() = True si AL MENOS UN cliente está vivo (alive=True).
            # - El loop termina cuando ambos clientes sean alive=False.
            while any(c.alive for c in self.clients):
                # time.sleep(0.2)
                # QUE HACE: Pausa 0.2 segundos para evitar saturar CPU con polling.
                # - Cada 0.2 seg verifica si algún cliente sigue vivo.
                time.sleep(0.2)

        # Aquí se ejecuta server.close() automáticamente (context manager con)
        # QUE HACE: Cierra el socket servidor, libera el puerto.
        
        # print("Servidor finalizado.")
        # QUE HACE: Imprime que el servidor se apagó.
        print("Servidor finalizado.")

    def _recv_name(self, conn: socket.socket) -> str:
        """Solicita el nombre al cliente para identificarlo en el chat.

        Args:
            conn: Socket del cliente ya conectado.

        Returns:
            El nombre recibido como str, o cadena vacia si falla.
        """

        # try:
        # QUE HACE: Inicia un bloque de manejo de errores.
        try:
            # conn.sendall("NOMBRE: ".encode(ENCODING))
            # QUE HACE: Envía la solicitud de nombre al cliente.
            # - conn = socket del cliente.
            # - sendall() envía todos los bytes (reintentos si es necesario).
            # - "NOMBRE: " = string de solicitud.
            # - encode(ENCODING) = convierte str a bytes usando UTF-8.
            # - RESULTADO: cliente recibe b'NOMBRE: ' y entra en input().
            conn.sendall("NOMBRE: ".encode(ENCODING))
            
            # raw = conn.recv(BUFFER_SIZE)
            # QUE HACE: Recibe la respuesta del cliente (bloquea hasta recibir datos).
            # - conn = socket del cliente.
            # - BUFFER_SIZE = 2048 (máximo de bytes a recibir).
            # - raw = bytes recibidos (ej: b'Alice\n').
            # - RETORNA: 0 bytes si el cliente cerró la conexión.
            raw = conn.recv(BUFFER_SIZE)
            
            # return raw.decode(ENCODING).strip()
            # QUE HACE: Decodifica, limpia y retorna el nombre.
            # - decode(ENCODING) = convierte bytes a str (b'Alice\n' -> "Alice\n").
            # - strip() = elimina espacios y saltos de línea ("Alice\n" -> "Alice").
            # - RETORNA: nombre limpio como string.
            return raw.decode(ENCODING).strip()
        
        # except OSError:
        # QUE HACE: Captura errores de socket (conexión cerrada, timeout, etc).
        except OSError:
            # return ""
            # QUE HACE: Retorna string vacío para indicar fallo.
            # - start() interpreta esto como "rechazar este cliente".
            return ""

    def _handle_client(self, client: ClientState) -> None:
        """Escucha mensajes de un cliente y los reenvia al otro.

        Args:
            client: Estado del cliente que este hilo administra.

        Efectos:
            - Espera la barrera de inicio.
            - Recibe mensajes y los reenvia al otro cliente.
            - Registra historial y maneja desconexion.
        """

        # try:
        # QUE HACE: Inicia bloque para sincronización con barrera.
        try:
            # self.ready_barrier.wait()
            # QUE HACE: Espera en la barrera hasta que 2 hilos lleguen.
            # - ready_barrier = threading.Barrier(2).
            # - wait() BLOQUEA hasta que 2 hilos llamem a wait().
            # - Cuando ambos llaman, la barrera se "rompe" y avanzan juntos.
            # - PROPÓSITO: evitar que un cliente envíe antes del otro.
            self.ready_barrier.wait()
            
            # self._send_to(client, "Ambos usuarios conectados. Puedes chatear.\n")
            # QUE HACE: Envía mensaje de confirmación al cliente.
            # - client = ClientState del cliente actual.
            # - Mensaje = "Ambos usuarios conectados. Puedes chatear.\n".
            # - VER: método _send_to() más abajo.
            self._send_to(client, "Ambos usuarios conectados. Puedes chatear.\n")
        
        # except threading.BrokenBarrierError:
        # QUE HACE: Captura si la barrera falla (alguien se desconecta en wait()).
        except threading.BrokenBarrierError:
            # client.alive = False
            # QUE HACE: Marca al cliente como inactivo.
            # - El while loop no se ejecutará.
            client.alive = False
            
            # return
            # QUE HACE: Sale del método (termina el hilo).
            return

        # while client.alive:
        # QUE HACE: Loop principal que recibe mensajes mientras el cliente esté activo.
        # - SALE CUANDO: client.alive se ponga False (desconexión).
        # - CONDICIÓN: se evalúa al inicio de cada iteración.
        while client.alive:
            # try:
            # QUE HACE: Inicia bloque para manejar errores de socket.
            try:
                # data = client.conn.recv(BUFFER_SIZE)
                # QUE HACE: Espera a recibir datos del cliente (BLOQUEA indefinidamente).
                # - client.conn = socket del cliente.
                # - recv(BUFFER_SIZE) = recibe hasta 2048 bytes.
                # - RETORNA: bytes recibidos o b'' (0 bytes si cliente cerró).
                # - data = bytes del mensaje (ej: b'Hola').
                data = client.conn.recv(BUFFER_SIZE)
                
                # if not data:
                # QUE HACE: Verifica si recv() retornó 0 bytes (cliente cerró conexión).
                # - not b'' = True (bytes vacíos son falsos en Python).
                if not data:
                    # break
                    # QUE HACE: Sale del while loop (va a limpieza).
                    break
                
                # message = data.decode(ENCODING).strip()
                # QUE HACE: Decodifica bytes a str y limpia espacios.
                # - decode(ENCODING) = b'Hola' -> "Hola" (UTF-8).
                # - strip() = elimina espacios/saltos ("Hola\\n" -> "Hola").
                # - message = string limpio del mensaje.
                message = data.decode(ENCODING).strip()
                
                # if message.lower() == "/exit":
                # QUE HACE: Verifica si el cliente envió el comando de salida.
                # - message.lower() = convierte a minúsculas ("/EXIT" -> "/exit").
                # - Si es "/exit", cliente quiere desconectarse.
                if message.lower() == "/exit":
                    # break
                    # QUE HACE: Sale del while loop (limpieza y cierre).
                    break

                # if not self.in_flight_limit.acquire(blocking=False):
                # QUE HACE: Intenta obtener un permiso del semáforo sin bloquear.
                # - in_flight_limit = BoundedSemaphore(5) con 5 permisos.
                # - acquire(blocking=False) retorna True si hay permiso, False si no.
                # - not False = True (si NO hay permiso, entra en el if).
                # - PROPÓSITO: controlar sobrecarga.
                if not self.in_flight_limit.acquire(blocking=False):
                    # print(f"[DROP] {client.name}: sobrecarga controlada")
                    # QUE HACE: Imprime que el mensaje fue descartado.
                    # - [DROP] = etiqueta para identificar descartes.
                    # - {client.name} = nombre del cliente.
                    # - ÚTIL: monitoreo/debugging.
                    print(f"[DROP] {client.name}: sobrecarga controlada")
                    
                    # continue
                    # QUE HACE: Salta al siguiente while (intenta recibir otro mensaje).
                    # - No ejecuta el resto (no registra ni reenvia).
                    continue

                # try:
                # QUE HACE: Inicia bloque garantizado para release().
                try:
                    # self._register_message(f"{client.name}: {message}")
                    # QUE HACE: Registra el mensaje en el historial.
                    # - ARGUMENTO: string formateado "Alice: Hola".
                    # - VER: método _register_message() más abajo.
                    self._register_message(f"{client.name}: {message}")
                    
                    # self._broadcast(client, f"{client.name}: {message}\\n")
                    # QUE HACE: Reenvia el mensaje al otro cliente.
                    # - ARGUMENTOS: client (emisor), mensaje con salto de línea.
                    # - VER: método _broadcast() más abajo.
                    self._broadcast(client, f"{client.name}: {message}\n")
                
                # finally:
                # QUE HACE: Bloque que SIEMPRE se ejecuta, incluso si hay error.
                finally:
                    # self.in_flight_limit.release()
                    # QUE HACE: Libera el permiso del semáforo.
                    # - release() incrementa el contador del semáforo.
                    # - IMPORTANTE: si no hace release(), quedan permisos atrapados (deadlock).
                    # - finally GARANTIZA: siempre se libera, incluso si hay excepción.
                    self.in_flight_limit.release()

            # except OSError:
            # QUE HACE: Captura errores de socket.
            # - EJEMPLOS: cliente cerró conexión, timeout, error de red.
            except OSError:
                # break
                # QUE HACE: Sale del while loop (va a limpieza).
                break

        # Aquí termina el while. Limpieza de desconexión.

        # client.alive = False
        # QUE HACE: Marca al cliente como inactivo.
        # - EFECTO: el main loop while any(c.alive...) verá esto.
        client.alive = False
        
        # self._register_message(f"{client.name} se desconecto")
        # QUE HACE: Registra en historial que el cliente se fue.
        # - EJEMPLO EN HISTORIAL: "Alice se desconecto".
        self._register_message(f"{client.name} se desconecto")
        
        # print(f"Desconectado: {client.name}")
        # QUE HACE: Imprime en consola que el cliente se desconectó.
        print(f"Desconectado: {client.name}")
        
        # try:
        # QUE HACE: Intenta cerrar el socket (puede fallar si ya está cerrado).
        try:
            # client.conn.close()
            # QUE HACE: Cierra el socket del cliente.
            # - EFECTO: libera recursos, conexión cerrada.
            client.conn.close()
        
        # except OSError:
        # QUE HACE: Si hay error al cerrar (socket ya cerrado, etc), ignora.
        except OSError:
            # pass
            # QUE HACE: No hacer nada, simplemente ignorar el error.
            pass

    def _register_message(self, message: str) -> None:
        """Agrega un mensaje al historial y lo imprime en servidor.

        Args:
            message: Texto ya formateado para guardar en historial.
        """

        # with self.history_lock:
        # QUE HACE: Adquiere el lock (mutex) que protege el historial.
        # - self.history_lock = threading.Lock().
        # - with = context manager: asegura que se libera el lock al salir.
        # - DENTRO del bloque with: solo 1 hilo ejecuta (exclusión mutua).
        # - PROBLEMA EVITADO: dos hilos escribiendo history simultáneamente (condición de carrera).
        with self.history_lock:
            # self.history.append(message)
            # QUE HACE: Agrega el mensaje al final de la lista historial.
            # - self.history = list[str] que contiene todos los mensajes.
            # - message = string ya formateado (ej: "Alice: Hola mundo").
            # - PROTEGIDO: solo 1 hilo aquí por el lock.
            # - EFECTO: history crece (sin límite en este servidor).
            self.history.append(message)
            
            # print(f"[HIST] {message}")
            # QUE HACE: Imprime el mensaje en consola con prefijo [HIST].
            # - [HIST] = etiqueta para identificar registros de historial.
            # - {message} = contenido del mensaje.
            # - ÚTIL: monitoreo en tiempo real del servidor.
            # - EJEMPLO: "[HIST] Alice: Hola mundo"
            print(f"[HIST] {message}")

    def _broadcast(self, sender: ClientState, message: str) -> None:
        """Reenvia el mensaje al cliente opuesto con un semaforo de envio.

        Args:
            sender: Cliente que origina el mensaje.
            message: Texto a reenviar al resto de clientes.
        """

        # with self.send_semaphore:
        # QUE HACE: Adquiere el semáforo que serializa envíos.
        # - self.send_semaphore = threading.Semaphore(1) con 1 permiso.
        # - with = context manager: asegura que se libera el semáforo al salir.
        # - DENTRO del bloque with: solo 1 hilo envía (serialización).
        # - PROBLEMA EVITADO: dos hilos escribiendo al socket destino simultáneamente.
        # - CONSECUENCIA EVITADA: mensajes entrelazados en el buffer del cliente.
        with self.send_semaphore:
            # for client in self.clients:
            # QUE HACE: Itera sobre todos los clientes conectados.
            # - self.clients = list[ClientState] de clientes.
            # - RANGO: 0, 1, o 2 clientes.
            # - client = cada ClientState en la iteración.
            for client in self.clients:
                # if client is sender or not client.alive:
                # QUE HACE: Verifica dos condiciones (descarta si ANY es verdadera):
                # - client is sender = ¿es el mismo cliente? (compara identidad, no igualdad).
                #   * Si es el EMISOR, omite (no le envía a sí mismo).
                # - not client.alive = ¿está inactivo? (alive=False).
                #   * Si ya se desconectó, omite.
                # - or = si CUALQUIERA es verdadera, omite.
                if client is sender or not client.alive:
                    # continue
                    # QUE HACE: Salta a la siguiente iteración del for.
                    # - No ejecuta _send_to() para este cliente.
                    continue
                
                # self._send_to(client, message)
                # QUE HACE: Envía el mensaje al cliente.
                # - ARGUMENTOS: client = cliente destino, message = mensaje.
                # - VER: método _send_to() más abajo.
                # - EFECTO: cliente recibe el mensaje en su socket.
                self._send_to(client, message)

    def _send_to(self, client: ClientState, message: str) -> None:
        """Envio protegido para un cliente.

        Args:
            client: Destinatario del mensaje.
            message: Texto en claro que sera codificado y enviado.
        """

        # try:
        # QUE HACE: Inicia bloque para manejar errores de socket.
        try:
            # client.conn.sendall(message.encode(ENCODING))
            # QUE HACE: Envía el mensaje codificado al cliente.
            # - client.conn = socket del cliente destino.
            # - message.encode(ENCODING) = convierte str a bytes.
            #   * "Hola mundo" -> b'Hola mundo' (UTF-8).
            # - sendall() envía TODOS los bytes (reintentos si es necesario).
            #   * A diferencia de send() que puede enviar parcialmente.
            # - EFECTO: cliente recibe los bytes en su socket (recv() los obtiene).
            client.conn.sendall(message.encode(ENCODING))
        
        # except OSError:
        # QUE HACE: Captura errores de socket.
        # - EJEMPLOS: conexión cerrada, cliente desconectado, timeout.
        except OSError:
            # client.alive = False
            # QUE HACE: Marca al cliente como inactivo.
            # - EFECTO: será descartado en próximas iteraciones.
            # - No cerramos socket: ya está cerrado o no responde.
            client.alive = False


def main() -> None:
    """Punto de entrada del servidor.

    Crea una instancia con valores por defecto y ejecuta el loop principal.
    """

    # ChatServer().start()
    # QUE HACE: Crea una instancia de ChatServer y ejecuta start().
    # - ChatServer() = crea instancia con valores por defecto.
    #   * host = "127.0.0.1" (localhost).
    #   * port = 50007 (puerto arbitrario).
    #   * history = [] (vacío).
    #   * Todos los locks/semáforos/barreras se inicializan automáticamente.
    # - .start() = ejecuta el método que comienza el loop de aceptación.
    # - BLOQUEANTE: comienza a escuchar y acepta clientes.
    # - RETORNA: cuando se cierren los sockets y terminen los hilos.
    ChatServer().start()


# if __name__ == "__main__":
# QUE HACE: Verifica si el script se ejecuta directamente (no como importación).
# - __name__ es un variable especial de Python.
# - Si ejecutas: python Taller1-server.py -> __name__ = "__main__".
# - Si importas: from archivo import algo -> __name__ = "archivo".
# - BENEFICIO: permite usar el archivo como ejecutable O como librería.
if __name__ == "__main__":
    # main()
    # QUE HACE: Llama a la función main() que inicia el servidor.
    # - Solo se ejecuta si el script se ejecuta directamente.
    # - No se ejecuta si el script se importa.
    main()
