"""Servidor de chat bidireccional con DOCUMENTACIÓN EXHAUSTIVA LÍNEA POR LÍNEA.

PROPÓSITO GENERAL DEL ARCHIVO:
- Implementar un servidor TCP que acepta exactamente 2 clientes.
- Sincroniza ambos clientes con una BARRERA (esperan uno al otro).
- Maneja mensajes bidireccionales usando HILOS (concurrencia).
- Protege recursos compartidos con LOCKS y SEMÁFOROS.
- Controla sobrecarga limitando mensajes en proceso.
- Mantiene historial de todos los mensajes en memoria.

CONCEPTOS CLAVE EXPLICADOS:
1. SOCKET: Punto de conexión de red. TCP/IP es el protocolo.
2. HILO: Ejecución concurrente de código dentro del mismo proceso.
3. BARRERA: Detiene N hilos hasta que TODOS lleguen a wait(). Luego avanzan juntos.
4. SEMÁFORO: Contador protegido (0 a N). acquire() decrementa, release() incrementa.
   - Si contador=0, acquire() bloquea (espera).
   - acquire(blocking=False) retorna False inmediatamente si contador=0.
5. LOCK: Semáforo especial con 1 permiso. Solo 1 hilo a la vez.
6. CONDICIÓN DE CARRERA: Dos hilos escriben al mismo recurso sin sincronización = comportamiento impredecible.

ESTRUCTURA DEL ARCHIVO:
1. Importaciones (módulos necesarios).
2. Constantes globales (HOST, PORT, ENCODING, BUFFER_SIZE).
3. Clase ClientState (dataclass que encapsula datos de un cliente).
4. Clase ChatServer (dataclass que orquesta la lógica del servidor).
5. Función main() (punto de entrada).

EJECUCIÓN:
    python Taller1-server.py
"""

# ==============================================================================
# IMPORTACIONES
# ==============================================================================

# from __future__ import annotations
# - Importa anotaciones de tipo pospuestas (forward references).
# - Beneficio: permite escribir list[str] en lugar de typing.List[str].
# - Requisito: Python 3.7+ si usas typing, Python 3.10+ nativo. Con esta línea funciona en cualquier versión.
# - EFECTO: código más legible.
from __future__ import annotations

# import socket
# - Módulo que proporciona la API para comunicación TCP/IP.
# - Métodos principales:
#   * socket.socket(AF_INET, SOCK_STREAM) = crea un socket TCP IPv4.
#   * bind((host, port)) = vincula socket a una dirección (IP, puerto).
#   * listen(n) = prepara socket para aceptar conexiones (cola de n).
#   * accept() = espera a un cliente, retorna (socket_del_cliente, (ip_remota, puerto)).
#   * sendall(bytes) = envía bytes (reintentos si es necesario).
#   * recv(n) = recibe hasta n bytes (bloquea si no hay datos).
#   * close() = cierra el socket (libera puerto).
# - AF_INET = dirección de internet (IPv4).
# - SOCK_STREAM = socket de flujo (TCP, confiable, orientado a conexión).
import socket

# import threading
# - Módulo que proporciona hilos y primitivas de sincronización.
# - Clases principales:
#   * Thread(target=func, args=(arg1,)) = crea un hilo que ejecuta func(arg1).
#   * Lock() = mutex (exclusión mutua, 1 permiso máximo).
#   * Semaphore(n) = semáforo con n permisos iniciales.
#   * BoundedSemaphore(n) = semáforo acotado (no puede crecer más allá de n).
#   * Barrier(n) = sincronizador que bloquea n hilos hasta que todos lleguen.
# - BENEFICIO: evita condiciones de carrera (múltiples hilos accediendo al mismo recurso).
import threading

# import time
# - Módulo que proporciona funciones de tiempo.
# - Funciones principales:
#   * time.sleep(n) = pausa el hilo actual n segundos.
#   * time.time() = retorna timestamp actual (segundos desde epoch).
# - BENEFICIO de sleep(): evita loops que saturan CPU.
import time

# from dataclasses import dataclass, field
# - dataclass = decorador que genera __init__, __repr__, __eq__ automáticamente.
# - field() = permite especificar valores por defecto complejos (listas, locks, semáforos).
# - BENEFICIO: menos código boilerplate (menos escritura manual).
from dataclasses import dataclass, field


# ==============================================================================
# CONSTANTES GLOBALES
# ==============================================================================

# HOST = "127.0.0.1"
# - Dirección IP donde el servidor escucha conexiones.
# - "127.0.0.1" = localhost (máquina local, NO accesible desde otra máquina).
# - "0.0.0.0" = escuchar en TODAS las interfaces de red (peligroso, accesible desde cualquier máquina).
# - Rango válido: "0.0.0.0" a "255.255.255.255" (IPv4).
# - Se pasa a socket.bind((HOST, PORT)).
HOST = "127.0.0.1"

# PORT = 50007
# - Puerto TCP donde escucha el servidor.
# - Puertos 0-1023 = reservados (requieren permisos de admin).
# - Puertos 1024-65535 = normales (sin permisos especiales).
# - Puertos comunes: 80 (HTTP), 443 (HTTPS), 22 (SSH), 3306 (MySQL), 5432 (PostgreSQL).
# - 50007 = puerto arbitrario para pruebas (fuera del rango de servicios estándar).
# - Se pasa a socket.bind((HOST, PORT)).
PORT = 50007

# ENCODING = "utf-8"
# - Codificación de caracteres para convertir entre str (texto) y bytes.
# - Proceso de envío:
#   1. Python: texto = "Hola" (str).
#   2. Encoding: "Hola".encode("utf-8") = b'Hola' (bytes).
#   3. Red: se envía b'Hola' por el socket.
# - Proceso de recepción:
#   1. Red: se recibe b'Hola' en el socket.
#   2. Decoding: b'Hola'.decode("utf-8") = "Hola" (str).
#   3. Python: procesamos texto = "Hola" (str).
# - UTF-8 = estándar universal que soporta TODOS los idiomas y símbolos.
# - Alternativas: ASCII (solo inglés), UTF-16 (menos eficiente), ISO-8859-1 (europeo).
ENCODING = "utf-8"

# BUFFER_SIZE = 2048
# - Cantidad máxima de bytes que socket.recv() lee en UNA SOLA LLAMADA.
# - 2048 bytes = 2 KB (típico para chat).
# - Si el cliente envía 3000 bytes, recv(2048) retorna 2048 bytes, hay que llamar otra vez.
# - Trade-off:
#   * Buffer más grande = menos llamadas a recv(), pero más memoria.
#   * Buffer más pequeño = más llamadas a recv(), pero menos memoria.
# - Valores comunes: 1024 (1 KB), 2048 (2 KB), 4096 (4 KB), 8192 (8 KB).
BUFFER_SIZE = 2048


# ==============================================================================
# CLASE 1: ClientState (Dataclass)
# ==============================================================================

# @dataclass
# - Decorador que genera __init__, __repr__, __eq__ automáticamente.
# - SIN @dataclass tendrías que escribir __init__ manualmente (mucho código).
# - CON @dataclass: se genera automáticamente basado en las anotaciones de tipo.
# - EFECTO: class ClientState: name: str; conn: socket.socket; addr: tuple; alive: bool = True
#   genera: __init__(self, name, conn, addr, alive=True)
@dataclass
class ClientState:
    """Encapsula el estado de un cliente conectado al servidor.

    PROPÓSITO: Agrupar name, conn, addr, alive en un solo objeto para fácil manejo.
    SIN ESTO: pasarías 4 parámetros separados entre funciones (confuso).
    CON ESTO: pasas 1 ClientState que contiene todo.

    ATRIBUTOS:

    name (str):
        Nombre identificador del cliente (ej: "Alice", "Bob").
        - Asignado por el cliente cuando se conecta (_recv_name()).
        - Se usa para etiquetar mensajes: "Alice: Hola mundo".
        - Rango típico: 1-32 caracteres alfanuméricos.
        - INVARIANTE: no cambia después de conectar (solo lectura).

    conn (socket.socket):
        Socket TCP conectado al cliente.
        - Único por cliente (no compartido con otros clientes).
        - Métodos usados:
          * conn.sendall(bytes) = envía bytes al cliente.
          * conn.recv(n) = recibe hasta n bytes del cliente (bloquea si no hay).
          * conn.close() = cierra la conexión (libera puerto).
        - INVARIANTE: no None después de conectar (hasta close()).

    addr (tuple[str, int]):
        Tupla (dirección_ip_remota, puerto_remoto) del cliente.
        - Ejemplo: ("192.168.1.100", 54321).
        - Asignada automáticamente por el SO cuando accept().
        - Se usa principalmente para logging y debugging.
        - INVARIANTE: no cambia después de conectar.

    alive (bool):
        Bandera booleana que indica si el cliente está activo.
        - Valor inicial: True (cliente acaba de conectar).
        - Se pone False cuando:
          * El cliente envía "/exit" (salida voluntaria).
          * La conexión se rompe (error de socket, socket cerrado).
          * Error al recibir o enviar datos (OSError).
        - Usado en loops: while client.alive: para salir cuando sea necesario.
        - Valor por defecto: True (si no especificas, se pone True automáticamente).
    """

    # name: str
    # - Anotación de tipo: name es de tipo str.
    # - Ejemplo: "Alice", "Bob", "Usuario123".
    # - SIN VALOR POR DEFECTO: es parámetro obligatorio en __init__.
    # - Generado por @dataclass: __init__(..., name, ...): self.name = name
    name: str

    # conn: socket.socket
    # - Anotación de tipo: conn es de tipo socket.socket.
    # - Ejemplo: <socket.socket fd=5, family=AF_INET, type=SOCK_STREAM, proto=6>
    # - SIN VALOR POR DEFECTO: es parámetro obligatorio en __init__.
    # - Generado por @dataclass: __init__(..., conn, ...): self.conn = conn
    conn: socket.socket

    # addr: tuple[str, int]
    # - Anotación de tipo: addr es una tupla de (str, int).
    # - Ejemplo: ("127.0.0.1", 12345) o ("192.168.1.100", 54321).
    # - Primer elemento: IP remota del cliente (str).
    # - Segundo elemento: puerto remoto del cliente (int).
    # - SIN VALOR POR DEFECTO: es parámetro obligatorio en __init__.
    # - Generado por @dataclass: __init__(..., addr, ...): self.addr = addr
    addr: tuple[str, int]

    # alive: bool = True
    # - Anotación de tipo: alive es de tipo bool.
    # - VALOR POR DEFECTO: True.
    # - CON VALOR POR DEFECTO: es parámetro OPCIONAL en __init__.
    # - Uso: ClientState("Alice", socket, addr) -> alive=True automáticamente.
    # - Uso: ClientState("Alice", socket, addr, alive=False) -> alive=False.
    # - Generado por @dataclass: __init__(..., alive=True): self.alive = alive
    alive: bool = True


# ==============================================================================
# CLASE 2: ChatServer (Dataclass)
# ==============================================================================

# @dataclass
# - Decorador que genera __init__ basado en las anotaciones de tipo.
# - Inicializa todos los atributos (host, port, history, history_lock, etc.).
# - BENEFICIO: no hay que escribir __init__ manualmente.
@dataclass
class ChatServer:
    """Servidor de chat bidireccional con sincronización y control de carga.

    PROPÓSITO GENERAL:
    Orquestar la lógica central del servidor:
    1. Crear socket TCP y escuchar en (HOST, PORT).
    2. Aceptar conexiones de clientes (máximo 2).
    3. Lanzar un hilo por cliente para manejar mensajes.
    4. Sincronizar ambos clientes con una barrera.
    5. Manejar recursos compartidos (historial, semáforos).
    6. Controlar sobrecarga limitando mensajes en vuelo.

    MÉTODOS (más adelante):
    - start(): Punto de entrada principal. Acepta 2 clientes.
    - _recv_name(conn): Solicitar nombre al cliente.
    - _handle_client(client): Hilo que atiende un cliente (recibe y reenvia mensajes).
    - _register_message(msg): Guardar mensaje en historial.
    - _broadcast(sender, msg): Reenviar a otros clientes.
    - _send_to(client, msg): Enviar seguro a un cliente.

    ATRIBUTOS:

    host (str):
        Dirección IP donde escucha el servidor.
        - Valor típico: "127.0.0.1" (localhost).
        - Rango: cualquier IPv4 válida ("0.0.0.0" a "255.255.255.255").
        - Valor por defecto: HOST ("127.0.0.1").
        - Se pasa a socket.bind((self.host, self.port)).

    port (int):
        Puerto TCP donde escucha el servidor.
        - Valor típico: 50007 (arbitrario para pruebas).
        - Rango válido: 1024-65535 (sin permisos de admin).
        - Valor por defecto: PORT (50007).
        - Se pasa a socket.bind((self.host, self.port)).

    history (list[str]):
        Lista que mantiene el historial de TODOS los mensajes.
        - Contenido: mensajes formateados como "Alice: Hola mundo", "Alice se desconecto".
        - Acceso: múltiples hilos pueden leer/escribir simultáneamente.
        - PROBLEMA SIN PROTECCIÓN: condición de carrera (dos hilos escriben a la vez = corrupción).
        - SOLUCIÓN: history_lock (lock) serializa acceso.
        - Crecimiento: sin límite en este servidor (en producción habría límite o archivos).
        - INICIALIZACIÓN: default_factory=list crea lista vacía para cada instancia.

    history_lock (threading.Lock):
        Mutex (exclusión mutua) que protege 'history' de acceso concurrente.
        - Lock = semáforo con 1 permiso máximo (0 o 1 hilo a la vez).
        - Uso: with self.history_lock: ... garantiza exclusión mutua dentro del bloque.
        - DENTRO del bloque with: solo 1 hilo ejecuta.
        - FUERA del bloque with: lock se libera automáticamente.
        - PROBLEMA EVITADO: dos hilos escribiendo history simultáneamente.
        - CONSECUENCIA EVITADA: datos corruptos o mensaje duplicado/perdido.
        - INICIALIZACIÓN: default_factory=threading.Lock() crea nuevo lock para cada instancia.

    send_semaphore (threading.Semaphore):
        Semáforo con 1 permiso que serializa envíos a clientes.
        - Semáforo = contador protegido (0 a N permisos).
        - Este tiene 1 permiso (similar a Lock, pero conceptualmente diferente).
        - Uso: with self.send_semaphore: ... solo 1 hilo envía dentro del bloque.
        - PROBLEMA EVITADO: dos hilos escribiendo al socket destino simultáneamente.
        - CONSECUENCIA EVITADA: mensajes entrelazados en el buffer del cliente.
        - EJEMPLO: Hilo A envía "Alice: Hola", Hilo B envía "Bob: Qué tal".
          SIN SEMÁFORO: cliente recibe "Alice: Hol Bob: Qué ata l" (entrelazado).
          CON SEMÁFORO: cliente recibe "Alice: Hola" + "Bob: Qué tal" (separado).
        - INICIALIZACIÓN: default_factory=lambda: threading.Semaphore(1).

    in_flight_limit (threading.BoundedSemaphore):
        Semáforo acotado con 5 permisos que limita sobrecarga.
        - BoundedSemaphore = semáforo que NO puede crecer más allá del límite.
        - Permisos iniciales: 5 (máximo 5 mensajes en procesamiento simultáneo).
        - Cada mensaje consume 1 permiso (acquire()).
        - Si contador=0, acquire(blocking=False) retorna False INMEDIATAMENTE (no bloquea).
        - Cuando termina el mensaje: release() devuelve 1 permiso.
        - PROBLEMA EVITADO: spammer enviar 1000 mensajes, server abruma.
        - SOLUCIÓN: descartar (DROP) mensajes cuando se agoten los 5 permisos.
        - ESTRATEGIA: permite max 5 mensajes en procesamiento, rechaza exceso.
        - INICIALIZACIÓN: default_factory=lambda: threading.BoundedSemaphore(5).

    ready_barrier (threading.Barrier):
        Barrera que sincroniza 2 participantes (los 2 clientes).
        - Barrera = sincronizador que DETIENE N hilos hasta que TODOS lleguen a wait().
        - Participantes: 2 (los 2 clientes que se conectan).
        - Uso: barrier.wait() bloquea el hilo hasta que 2 lleguen a wait().
        - COMPORTAMIENTO:
          * Hilo cliente A llama a wait() -> se bloquea.
          * Hilo cliente B llama a wait() -> se bloquea.
          * Cuando ambos llaman -> barrera se "rompe" y AMBOS continúan.
        - PROBLEMA RESUELTO: evitar que un cliente envíe antes del otro.
        - ESCENARIO SIN BARRERA:
          * Alice conecta, ve "Ambos usuarios conectados" -> empieza a enviar.
          * Pero Bob aún no conectó -> Alice envía mensajes que Bob nunca recibe.
        - ESCENARIO CON BARRERA:
          * Alice conecta -> espera en barrier.wait().
          * Bob conecta -> espera en barrier.wait().
          * Ambos listos -> barrera se rompe, AMBOS ven "Ambos usuarios conectados".
          * Ambos comienzan a chatear simultáneamente (justo).
        - EXCEPCIONES: BrokenBarrierError si un hilo se desconecta antes de wait().
        - INICIALIZACIÓN: default_factory=lambda: threading.Barrier(2).

    clients (list[ClientState]):
        Lista de clientes conectados actualmente al servidor.
        - Tamaño: 0, 1, o 2 (máximo 2 clientes).
        - Acceso: múltiples hilos LEEN la lista, pero solo start() ESCRIBE.
        - Iteración: for client in self.clients para enviar a todos (en _broadcast).
        - Verificación: len(self.clients) < 2 para saber si hay hueco (en start).
        - CONTENIDO: cada elemento es un ClientState (name, conn, addr, alive).
        - INICIALIZACIÓN: default_factory=list crea lista vacía para cada instancia.
    """

    # host: str = HOST
    # - Anotación: host es de tipo str.
    # - VALOR POR DEFECTO: HOST ("127.0.0.1").
    # - SIN ESPECIFICAR en __init__: ChatServer() -> host="127.0.0.1".
    # - ESPECIFICANDO en __init__: ChatServer(host="0.0.0.0") -> host="0.0.0.0".
    host: str = HOST

    # port: int = PORT
    # - Anotación: port es de tipo int.
    # - VALOR POR DEFECTO: PORT (50007).
    # - SIN ESPECIFICAR en __init__: ChatServer() -> port=50007.
    # - ESPECIFICANDO en __init__: ChatServer(port=8080) -> port=8080.
    port: int = PORT

    # history: list[str] = field(default_factory=list)
    # - Anotación: history es de tipo list[str].
    # - VALOR POR DEFECTO: field(default_factory=list).
    # - ¿QUÉ ES field()?
    #   * Función que permite especificar valores por defecto complejos.
    #   * default_factory=list es una función que se LLAMA para generar el valor.
    #   * Se ejecuta CADA VEZ que se crea una instancia ChatServer().
    #   * Cada instancia obtiene su PROPIA lista vacía (no compartida).
    # - ¿POR QUÉ NO simplemente history: list[str] = []?
    #   * Porque [] se evalúa UNA SOLA VEZ al definir la clase.
    #   * Todas las instancias compartirían la MISMA lista (¡bug!).
    #   * field(default_factory=list) lo hace correctamente.
    # - SIN ESPECIFICAR en __init__: ChatServer() -> history=[].
    # - INICIALMENTE VACÍA: [].
    # - CRECE CONFORME: se llama _register_message() (append).
    history: list[str] = field(default_factory=list)

    # history_lock: threading.Lock = field(default_factory=threading.Lock)
    # - Anotación: history_lock es de tipo threading.Lock.
    # - VALOR POR DEFECTO: field(default_factory=threading.Lock).
    # - ¿QUÉ ES field()?
    #   * Función que permite especificar valores por defecto complejos.
    #   * default_factory=threading.Lock es una FUNCIÓN que se LLAMA.
    #   * threading.Lock es una función que retorna un nuevo lock.
    #   * Se ejecuta CADA VEZ que se crea una instancia ChatServer().
    #   * Cada instancia obtiene su PROPIO lock (independiente).
    # - SIN ESPECIFICAR en __init__: ChatServer() -> history_lock=<Lock object>.
    # - USO: with self.history_lock: ... serializa acceso a history.
    history_lock: threading.Lock = field(default_factory=threading.Lock)

    # send_semaphore: threading.Semaphore = field(default_factory=lambda: threading.Semaphore(1))
    # - Anotación: send_semaphore es de tipo threading.Semaphore.
    # - VALOR POR DEFECTO: field(default_factory=lambda: threading.Semaphore(1)).
    # - ¿QUÉ ES lambda?
    #   * Función anónima sin argumentos.
    #   * lambda: threading.Semaphore(1) es una función que retorna semáforo(1).
    #   * Se necesita lambda porque Semaphore(1) es una LLAMADA, no una función.
    #   * default_factory espera una FUNCIÓN (callable), no un valor.
    # - EFECTO: cada instancia obtiene su propio Semaphore(1).
    # - SIN ESPECIFICAR en __init__: ChatServer() -> send_semaphore=<Semaphore object>.
    # - PERMISOS INICIALES: 1 (solo 1 hilo puede enviar a la vez).
    # - USO: with self.send_semaphore: ... serializa envíos.
    send_semaphore: threading.Semaphore = field(default_factory=lambda: threading.Semaphore(1))

    # in_flight_limit: threading.BoundedSemaphore = field(default_factory=lambda: threading.BoundedSemaphore(5))
    # - Anotación: in_flight_limit es de tipo threading.BoundedSemaphore.
    # - VALOR POR DEFECTO: field(default_factory=lambda: threading.BoundedSemaphore(5)).
    # - EFECTO: cada instancia obtiene su propio BoundedSemaphore(5).
    # - SIN ESPECIFICAR en __init__: ChatServer() -> in_flight_limit=<BoundedSemaphore object>.
    # - PERMISOS INICIALES: 5 (máximo 5 mensajes en vuelo).
    # - USO: acquire(blocking=False) para intentar tomar permiso sin bloquear.
    in_flight_limit: threading.BoundedSemaphore = field(default_factory=lambda: threading.BoundedSemaphore(5))

    # ready_barrier: threading.Barrier = field(default_factory=lambda: threading.Barrier(2))
    # - Anotación: ready_barrier es de tipo threading.Barrier.
    # - VALOR POR DEFECTO: field(default_factory=lambda: threading.Barrier(2)).
    # - EFECTO: cada instancia obtiene su propia Barrier(2).
    # - SIN ESPECIFICAR en __init__: ChatServer() -> ready_barrier=<Barrier object>.
    # - PARTICIPANTES: 2 (los 2 clientes).
    # - USO: wait() para bloquear hasta que 2 hilos lleguen.
    ready_barrier: threading.Barrier = field(default_factory=lambda: threading.Barrier(2))

    # clients: list[ClientState] = field(default_factory=list)
    # - Anotación: clients es de tipo list[ClientState].
    # - VALOR POR DEFECTO: field(default_factory=list).
    # - EFECTO: cada instancia obtiene su propia lista vacía.
    # - SIN ESPECIFICAR en __init__: ChatServer() -> clients=[].
    # - INICIALMENTE VACÍA: [].
    # - CRECE CONFORME: se aceptan clientes (append en start()).
    # - MÁXIMO TAMAÑO: 2 (mientras len(self.clients) < 2).
    clients: list[ClientState] = field(default_factory=list)

    def start(self) -> None:
        """Inicia el servidor y ejecuta el loop principal de aceptación."""

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

            while any(c.alive for c in self.clients):  # Revisa clientes activos.
                time.sleep(0.2)  # Evita loop agresivo.

        print("Servidor finalizado.")  # Log final.

    def _recv_name(self, conn: socket.socket) -> str:
        """Solicita el nombre al cliente para identificarlo en el chat."""

        try:
            conn.sendall("NOMBRE: ".encode(ENCODING))  # Pide nombre.
            raw = conn.recv(BUFFER_SIZE)  # Lee respuesta.
            return raw.decode(ENCODING).strip()  # Devuelve limpio.
        except OSError:
            return ""  # Sin nombre.

    def _handle_client(self, client: ClientState) -> None:
        """Escucha mensajes de un cliente y los reenvia al otro."""

        try:
            self.ready_barrier.wait()  # Espera barrera.
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

                if not self.in_flight_limit.acquire(blocking=False):
                    print(f"[DROP] {client.name}: sobrecarga controlada")
                    continue

                try:
                    self._register_message(f"{client.name}: {message}")
                    self._broadcast(client, f"{client.name}: {message}\n")
                finally:
                    self.in_flight_limit.release()

            except OSError:
                break

        client.alive = False
        self._register_message(f"{client.name} se desconecto")
        print(f"Desconectado: {client.name}")
        try:
            client.conn.close()
        except OSError:
            pass

    def _register_message(self, message: str) -> None:
        """Guarda un mensaje en el historial y lo imprime en consola."""

        with self.history_lock:
            self.history.append(message)
            print(f"[HIST] {message}")

    def _broadcast(self, sender: ClientState, message: str) -> None:
        """Reenvia un mensaje del emisor a todos los clientes excepto el emisor."""

        with self.send_semaphore:
            for client in self.clients:
                if client is sender or not client.alive:
                    continue
                self._send_to(client, message)

    def _send_to(self, client: ClientState, message: str) -> None:
        """Envía un mensaje a un cliente específico de forma segura."""

        try:
            client.conn.sendall(message.encode(ENCODING))
        except OSError:
            client.alive = False


def main() -> None:
    """Punto de entrada principal del script del servidor."""

    ChatServer().start()


if __name__ == "__main__":
    main()
