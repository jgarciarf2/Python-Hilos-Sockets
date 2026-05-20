"""Servidor del chat bidireccional con documentación exhaustiva línea por línea.

DESCRIPCIÓN GENERAL:
Este archivo implementa un servidor TCP que:

1. Acepta exactamente 2 clientes conectados simultáneamente.
2. Usa una BARRERA para sincronizar el inicio (ambos clientes listos).
3. Recibe mensajes de cada cliente y los reenvia al otro (bidireccional).
4. Mantiene un historial en memoria de todos los mensajes.
5. Usa SEMÁFOROS para evitar condiciones de carrera y controlar sobrecarga.
6. Usa HILOS para manejar múltiples clientes de forma concurrente.

CONCEPTOS CLAVE:

- BARRERA: Detiene hilos hasta que N participantes lleguen a wait().
- SEMÁFORO: Contador que limita acceso a recursos compartidos.
- LOCK: Exclusión mutua simple (semáforo con 1 permiso).
- HILO: Ejecución concurrente de código en el mismo proceso.
- CONDICIÓN DE CARRERA: Error cuando 2 hilos escriben al mismo recurso sin sincronización.

EJECUCIÓN:
python Taller1-server.py
"""

# IMPORTACIONES

# from **future** import annotations

# - Importar anotaciones pospuestas.

# - Permite escribir list[str] en lugar de typing.List[str].

# - Requiere Python 3.10+ sin esta línea, o cualquier versión con ella.

# - Beneficio: código más limpio y legible.

from **future** import annotations

# import socket

# - Módulo socket: interfaz para comunicación TCP/IP.

# - socket.socket() = crea un socket.

# - socket.AF_INET = IPv4 (internet).

# - socket.SOCK_STREAM = TCP (flujo confiable, orientado a conexión).

# - Métodos principales:

# - bind((host, port)) = asocia socket a una dirección.

# - listen(n) = prepara socket para aceptar conexiones (cola de n).

# - accept() = espera a un cliente, retorna (socket_cliente, (ip, puerto)).

# - sendall(bytes) = envía bytes.

# - recv(n) = recibe hasta n bytes.

# - close() = cierra socket.

import socket

# import threading

# - Módulo threading: manejo de hilos en Python.

# - threading.Thread = clase para crear hilos.

# - threading.Lock = mutex (exclusión mutua).

# - threading.Semaphore = contador protegido (acceso limitado).

# - threading.BoundedSemaphore = semáforo acotado.

# - threading.Barrier = sincronizador de N participantes.

# - threading.Event = bandera booleana para señales entre hilos.

import threading

# import time

# - Módulo time: funciones de tiempo.

# - time.time() = retorna timestamp actual (segundos desde epoch).

# - time.sleep(n) = duerme n segundos (pausa el hilo actual).

# - Beneficio de sleep(): evita loops agresivos que saturan CPU.

import time

# from dataclasses import dataclass, field

# - dataclass = decorador que genera **init**, **repr**, **eq**.

# - Beneficio: menos código boilerplate.

# - field() = permite valores por defecto complejos.

# - default_factory = función que se llama para generar el valor por defecto.

# - Ejemplo: field(default_factory=list) crea una lista nueva para cada instancia.

from dataclasses import dataclass, field

# CONSTANTES GLOBALES

# HOST: dirección IP donde escucha el servidor.

# - "127.0.0.1" = localhost (solo máquina local, no accesible desde otra máquina).

# - "0.0.0.0" = escuchar en todas las interfaces de red (más peligroso).

# - Rango típico: IPv4 = "0.0.0.0" a "255.255.255.255".

HOST = "127.0.0.1"

# PORT: puerto TCP donde escucha el servidor.

# - Puertos 0-1023 = reservados, requieren permisos de admin.

# - Puertos 1024-65535 = puertos normales, accesibles sin permisos.

# - Puertos comunes: 80 (HTTP), 443 (HTTPS), 22 (SSH), 3306 (MySQL).

# - 50007 = puerto arbitrario para pruebas (fuera del rango de servicios conocidos).

PORT = 50007

# ENCODING: codificación de caracteres para convertir entre str y bytes.

# - UTF-8 = estándar universal que soporta todos los idiomas y símbolos.

# - Proceso: str -> encode(UTF-8) -> bytes -> transmisión por red -> recv() -> bytes -> decode(UTF-8) -> str.

# - Ejemplo: "Hola".encode("utf-8") produce b'Hola' (bytes).

# - Ejemplo: b'Hola'.decode("utf-8") produce "Hola" (str).

# - Alternativas: ASCII (solo inglés), UTF-16 (menos eficiente).

ENCODING = "utf-8"

# BUFFER_SIZE: tamaño máximo que socket.recv() lee de una vez.

# - 2048 bytes = 2 KB, típico para aplicaciones de chat.

# - Si el mensaje es más grande, se recibe en múltiples recv() sucesivos.

# - Valores comunes: 1024 (1 KB), 4096 (4 KB), 8192 (8 KB).

# - Trade-off: buffer más grande = menos recv() pero más memoria.

BUFFER_SIZE = 2048

# CLASE 1: ClientState

# @dataclass = decorador que genera **init** automáticamente.

# - Genera: def **init**(self, name, conn, addr, alive=True): ...

# - También genera **repr**, **eq**, **hash** (opcionales).

@dataclass
class ClientState:
"""Encapsula el estado de un cliente conectado al servidor.

    PROPÓSITO:
    Agrupar toda la información de un cliente en un objeto para fácil manejo.
    Sin esto, pasaríamos 4 parámetros separados entre funciones.

    ATRIBUTOS:

    name (str):
        El nombre identificador único del cliente.
        - Asignado por el cliente cuando se conecta.
        - Se usa para etiquetar mensajes: "Alice: Hola mundo".
        - Rango típico: 1-32 caracteres alfanuméricos.
        - Ejemplo: "Alice", "Bob", "Usuario123".

    conn (socket.socket):
        El objeto socket TCP conectado al cliente específico.
        - Único por cliente (no compartido con otros clientes).
        - Métodos usados:
          * conn.sendall(bytes) = envía bytes al cliente.
          * conn.recv(n) = recibe hasta n bytes del cliente.
          * conn.close() = cierra la conexión.
        - Tipo: socket.socket (objeto).

    addr (tuple[str, int]):
        La tupla (dirección_ip_remota, puerto_remoto) del cliente.
        - Asignada por el SO cuando el cliente se conecta.
        - Ejemplo: ("192.168.1.100", 54321).
        - Se usa principalmente para logging y debugging.
        - No cambia durante toda la conexión.

    alive (bool):
        Bandera booleana que indica si el cliente está activo.
        - Valor inicial: True (cliente acaba de conectar).
        - Se pone False cuando:
          * El cliente envía "/exit".
          * La conexión se rompe (error de socket).
          * Error al recibir o enviar datos.
        - Usado en loops: while client.alive: para salir cuando es necesario.
        - Valor por defecto: True (si no se especifica).
    """

    # Anotación: name es de tipo str
    # Ejemplo: "Alice", "Bob"
    name: str

    # Anotación: conn es de tipo socket.socket
    # Ejemplo: <socket.socket object at 0x...>
    conn: socket.socket

    # Anotación: addr es una tupla de (str, int)
    # Ejemplo: ("127.0.0.1", 12345)
    addr: tuple[str, int]

    # Anotación: alive es bool con valor por defecto True
    # Cuando creas ClientState(name="Alice", conn=s, addr=a),
    # automáticamente alive se pone True si no lo especificas.
    alive: bool = True

# CLASE 2: ChatServer

@dataclass
class ChatServer:
"""Servidor de chat bidireccional con sincronización y control de carga.

    PROPÓSITO:
    Orquestar la lógica central del servidor:
    1. Crear socket TCP y escuchar en (HOST, PORT).
    2. Aceptar conexiones de clientes (máximo 2).
    3. Lanzar un hilo por cliente para manejar mensajes.
    4. Sincronizar ambos clientes con una barrera.
    5. Manejar recursos compartidos (historial, semáforos).
    6. Controlar sobrecarga con límites de mensajes.

    MÉTODOS:
    - start(): Punto de entrada principal.
    - _recv_name(conn): Solicitar nombre al cliente.
    - _handle_client(client): Hilo que atiende un cliente.
    - _register_message(msg): Guardar mensaje en historial.
    - _broadcast(sender, msg): Reenviar a otros clientes.
    - _send_to(client, msg): Enviar seguro a un cliente.

    ATRIBUTOS:

    host (str):
        Dirección IP donde escucha el servidor.
        - Valor típico: "127.0.0.1" (localhost).
        - Rango: cualquier IPv4 válida ("0.0.0.0" a "255.255.255.255").
        - Valor por defecto: HOST ("127.0.0.1").

    port (int):
        Puerto TCP donde escucha el servidor.
        - Valor típico: 50007 (arbitrario para pruebas).
        - Rango válido: 1024-65535 (sin permisos de admin).
        - Valor por defecto: PORT (50007).

    history (list[str]):
        Lista que mantiene el historial de todos los mensajes.
        - Contenido: mensajes formateados como "Alice: Hola mundo".
        - Acceso: múltiples hilos pueden leer/escribir simultáneamente.
        - PROBLEMA: condición de carrera si dos hilos escriben a la vez.
        - SOLUCIÓN: history_lock (lock) serializa acceso.
        - Crecimiento: sin límite en este servidor (en producción habría límite).

    history_lock (threading.Lock):
        Mutex (exclusión mutua) que protege 'history'.
        - Lock = semáforo con 1 permiso (0 o 1 hilo a la vez).
        - Uso: with self.history_lock: garantiza exclusión.
        - Dentro del bloque with: solo 1 hilo ejecuta simultáneamente.
        - Fuera del bloque: lock se libera automáticamente.
        - PROBLEMA evitado: dos hilos escribiendo history simultáneamente.

    send_semaphore (threading.Semaphore):
        Semáforo con 1 permiso que serializa envíos a clientes.
        - Semáforo = contador N protegido (0 a N permisos).
        - Este: 1 permiso (similar a Lock, pero más flexible).
        - Uso: with self.send_semaphore: solo 1 hilo envía a la vez.
        - PROBLEMA evitado: dos hilos escribiendo al socket destino simultáneamente.
        - CONSECUENCIA evitada: mensajes entrelazados en el buffer del cliente.

    in_flight_limit (threading.BoundedSemaphore):
        Semáforo acotado con 5 permisos que limita carga.
        - Acotado = no puede crecer más allá de 5.
        - Permisos iniciales: 5.
        - Cada mensaje consume 1 permiso (acquire()).
        - Si no hay permisos, acquire(blocking=False) retorna False.
        - Cuando termina el mensaje: release() devuelve 1 permiso.
        - PROBLEMA evitado: spammer saturar el servidor con muchos mensajes.
        - SOLUCIÓN: descartar (DROP) mensajes cuando se agoten permisos.

    ready_barrier (threading.Barrier):
        Barrera que sincroniza 2 participantes (los 2 clientes).
        - Barrera = sincronizador que detiene N hilos hasta que todos lleguen.
        - Participantes: 2 (los 2 clientes).
        - Uso: barrier.wait() bloquea el hilo hasta que 2 lleguen.
        - PROBLEMA resuelto: evitar que un cliente envíe antes del otro.
        - Excepciones: BrokenBarrierError si un hilo se desconecta en wait().

    clients (list[ClientState]):
        Lista de clientes conectados.
        - Tamaño: 0, 1, o 2 (máximo 2 clientes).
        - Acceso: múltiples hilos leen, pero solo start() escribe.
        - Iteración: for client in self.clients para enviar a todos.
        - Verificación: len(self.clients) < 2 para saber si hay hueco.
    """

    # host: str, valor por defecto HOST ("127.0.0.1")
    # Si creas ChatServer() sin argumentos, host = "127.0.0.1".
    # Si creas ChatServer(host="0.0.0.0"), host = "0.0.0.0".
    host: str = HOST

    # port: int, valor por defecto PORT (50007)
    # Si creas ChatServer() sin argumentos, port = 50007.
    # Si creas ChatServer(port=8080), port = 8080.
    port: int = PORT

    # history: list[str], comienza vacía
    # default_factory=list: cada instancia obtiene su propia lista vacía.
    # Alternativa INCORRECTA: history: list[str] = [] (compartida entre instancias).
    history: list[str] = field(default_factory=list)

    # history_lock: threading.Lock, se crea uno nuevo por instancia
    # default_factory=threading.Lock: llama a threading.Lock() durante __init__.
    # Resultado: cada ChatServer obtiene su propio Lock.
    history_lock: threading.Lock = field(default_factory=threading.Lock)

    # send_semaphore: threading.Semaphore(1), serializa envíos
    # default_factory=lambda: threading.Semaphore(1):
    #   - lambda = función anónima sin argumentos.
    #   - Crea un semáforo con 1 permiso.
    #   - Se ejecuta durante __init__ para cada instancia.
    send_semaphore: threading.Semaphore = field(default_factory=lambda: threading.Semaphore(1))

    # in_flight_limit: threading.BoundedSemaphore(5), limita carga
    # default_factory=lambda: threading.BoundedSemaphore(5):
    #   - Crea un semáforo acotado con 5 permisos.
    #   - Se ejecuta durante __init__ para cada instancia.
    in_flight_limit: threading.BoundedSemaphore = field(default_factory=lambda: threading.BoundedSemaphore(5))

    # ready_barrier: threading.Barrier(2), sincroniza 2 clientes
    # default_factory=lambda: threading.Barrier(2):
    #   - Crea una barrera para 2 participantes.
    #   - Se ejecuta durante __init__ para cada instancia.
    ready_barrier: threading.Barrier = field(default_factory=lambda: threading.Barrier(2))

    # clients: list[ClientState], comienza vacía
    # default_factory=list: cada instancia obtiene su propia lista vacía.
    # Se llena conforme se conectan clientes (máximo 2).
    clients: list[ClientState] = field(default_factory=list)

    # MÉTODO 1: start()
    def start(self) -> None:
        """Inicia el servidor y ejecuta el loop principal de aceptación.

        FLUJO DETALLADO:

        1. Crea socket TCP (AF_INET=IPv4, SOCK_STREAM=TCP).
        2. Configura socket: SO_REUSEADDR=1 para reusar puerto.
        3. bind((HOST, PORT)): vincula socket a dirección.
        4. listen(2): prepara socket para aceptar conexiones (cola de 2).
        5. Loop: acepta exactamente 2 clientes:
           a) accept() espera a un cliente (bloquea).
           b) _recv_name(conn) solicita nombre.
           c) Crea ClientState y lo agrega a self.clients.
           d) Lanza un hilo que ejecuta _handle_client(client).
        6. Después de 2 clientes, mantiene proceso vivo: while any(c.alive for c in self.clients).
        7. Al finalizar, imprime "Servidor finalizado."

        SINCRONIZACIÓN:
        - Los hilos de cliente se ejecutan en paralelo (concurrencia).
        - El loop principal espera a que todos salgan antes de cerrar.
        - Barrera (ready_barrier) sincroniza inicio de ambos clientes.

        MANEJO DE ERRORES:
        - Si _recv_name() retorna "", se rechaza el cliente.
        - Si cliente se desconecta, alive se pone False.
        - Si hay excepción en hilo, el hilo termina (no afecta al servidor).
        """

        # with socket.socket(...) as server:
        # - Crea un socket TCP.
        # - AF_INET = IPv4 (protocolo internet versión 4).
        # - SOCK_STREAM = TCP (flujo de bytes confiable, orientado a conexión).
        # - with = context manager: garantiza que server.close() se ejecute al salir.
        # - Beneficio: no hay que cerrar manualmente el socket.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            # server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # - setsockopt = set socket option (configurar opción de socket).
            # - SOL_SOCKET = nivel de opciones (socket).
            # - SO_REUSEADDR = opción "reusar dirección".
            # - 1 = activar la opción.
            # EFECTO: permite reusar puerto inmediatamente después de cerrar.
            # PROBLEMA EVITADO: "Address already in use" si reiniciamos rápido.
            # SIN ESTO: habría que esperar ~60 segundos antes de reusar el puerto.
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # server.bind((self.host, self.port))
            # - bind = vincular socket a una dirección (IP, puerto).
            # - Parámetro: tupla (host, port).
            # - self.host = "127.0.0.1" (IP de escucha).
            # - self.port = 50007 (puerto de escucha).
            # EFECTO: SO sabe que este proceso escucha en 127.0.0.1:50007.
            # DESPUÉS DE ESTO: cuando alguien se conecte a 127.0.0.1:50007, lo recibimos.
            # ERROR SI: otro proceso ya está escuchando en ese puerto.
            server.bind((self.host, self.port))

            # server.listen(2)
            # - listen = preparar socket para aceptar conexiones.
            # - Parámetro: tamaño de la cola de conexiones (2).
            # EFECTO: SO mantiene una cola de hasta 2 conexiones pendientes.
            # COMPORTAMIENTO: si hay 2 clientes esperando y llega un 3ro, se rechaza.
            # RANGO TÍPICO: 1-5 (valores mayores suelen ignorarse por el SO).
            server.listen(2)

            # print(f"Servidor escuchando en {self.host}:{self.port}")
            # - Imprime en consola que el servidor está activo.
            # - Formato: "Servidor escuchando en 127.0.0.1:50007"
            # - Útil para saber que el servidor arrancó correctamente.
            print(f"Servidor escuchando en {self.host}:{self.port}")

            # while len(self.clients) < 2:
            # - Loop que acepta exactamente 2 clientes.
            # - Condición: len(self.clients) < 2 = "mientras haya menos de 2 clientes".
            # - El loop termina cuando se alcancen 2 clientes.
            # PROBLEMA: ¿qué si los 2 clientes se desconectan dentro de este loop?
            # SOLUCIÓN: no es problema, porque accept() espera nuevas conexiones.
            while len(self.clients) < 2:
                # conn, addr = server.accept()
                # - accept() bloquea indefinidamente esperando a un cliente.
                # - Retorna tupla (socket_del_cliente, (ip_remota, puerto_remoto)).
                # - conn = socket del cliente (objeto único).
                # - addr = (ip_remota, puerto_remoto) = ("127.0.0.1", 12345).
                # DESPUÉS DE ESTO: tenemos un socket conectado al cliente.
                # NOTA: cada accept() retorna un socket diferente (client-side).
                conn, addr = server.accept()

                # name = self._recv_name(conn)
                # - Llama al método _recv_name(conn) que solicita nombre al cliente.
                # - Parámetro conn: socket del cliente.
                # - Retorna: string con el nombre, o "" si falla.
                # PROTOCOLO:
                #   1. Servidor envía: "NOMBRE: " (UTF-8).
                #   2. Cliente recibe y envía respuesta: "Alice" (UTF-8).
                #   3. Servidor retorna: "Alice" (limpio, sin espacios).
                # VALOR RETORNADO: nombre del cliente.
                name = self._recv_name(conn)

                # if not name:
                # - Verifica si el nombre es vacío (conexión falló).
                # - not "" = True (string vacío es falso en Python).
                # - not "Alice" = False (string no vacío es verdadero).
                # EFECTO: si falla, rechaza este cliente.
                if not name:
                    # conn.close()
                    # - Cierra el socket de este cliente rechazado.
                    # - Libera recursos del SO.
                    # EFECTO: cliente recibe conexión cerrada.
                    conn.close()

                    # continue
                    # - Salta al siguiente servidor.accept().
                    # - No ejecuta el resto del loop.
                    continue

                # client = ClientState(name=name, conn=conn, addr=addr)
                # - Crea un objeto ClientState con datos del cliente.
                # - __init__ se genera automáticamente por @dataclass.
                # - Parámetros:
                #   * name = "Alice" (obtenido de _recv_name).
                #   * conn = socket del cliente.
                #   * addr = ("127.0.0.1", 12345) (obtenido de accept()).
                #   * alive = True (valor por defecto).
                # RESULTADO: objeto ClientState que encapsula todo del cliente.
                client = ClientState(name=name, conn=conn, addr=addr)

                # self.clients.append(client)
                # - Agrega el cliente a la lista de clientes del servidor.
                # - Después: len(self.clients) aumenta en 1.
                # - Si len(self.clients) == 2, el loop while terminará en la siguiente iteración.
                self.clients.append(client)

                # print(f"Conectado: {name} desde {addr}")
                # - Imprime un log en consola.
                # - Ejemplo: "Conectado: Alice desde ('127.0.0.1', 12345)"
                # - Útil para monitoreo.
                print(f"Conectado: {name} desde {addr}")

                # thread = threading.Thread(
                #     target=self._handle_client,
                #     args=(client,),
                #     daemon=True,
                # )
                # - Crea un nuevo hilo (ejecutable concurrente).
                # - target = función a ejecutar en el hilo: self._handle_client.
                # - args = tupla de argumentos para la función: (client,).
                # - daemon = True: si el proceso principal muere, el hilo muere también.
                # EFECTO: cuando start() termina, los hilos se terminarán.
                # SIN daemon=True: el programa esperaría a que los hilos terminen.
                thread = threading.Thread(
                    target=self._handle_client,
                    args=(client,),
                    daemon=True,
                )

                # thread.start()
                # - Inicia el hilo (comienza a ejecutar _handle_client(client)).
                # - IMPORTANTE: no es bloqueante, start() retorna inmediatamente.
                # - El hilo ejecuta _handle_client() en paralelo con el resto del código.
                # - Múltiples hilos pueden ejecutarse simultáneamente en multi-core.
                thread.start()

            # Después del loop anterior, tenemos 2 clientes conectados.
            # Ambos tienen hilos ejecutando _handle_client() en paralelo.

            # while any(c.alive for c in self.clients):
            # - Loop que mantiene el proceso principal vivo.
            # - any(c.alive for c in self.clients) = True si al menos 1 cliente está vivo.
            # - EFECTO: loop se repite mientras haya clientes activos.
            # - CUÁNDO TERMINA: cuando ambos clientes se desconecten (alive=False).
            # - SIN ESTE LOOP: start() terminaría inmediatamente y el socket se cerraría.
            while any(c.alive for c in self.clients):
                # time.sleep(0.2)
                # - Pausa el loop por 0.2 segundos.
                # - EFECTO: no saturar CPU con polling constante.
                # - CADA ITERACIÓN: revisa si algún cliente está vivo.
                # - ALTERNATIVA: usar eventos/locks (más complejo).
                time.sleep(0.2)

        # Cuando salimos del bloque 'with', server.close() se ejecuta automáticamente.
        # EFECTO: el socket servidor se cierra, el puerto se libera.

        # print("Servidor finalizado.")
        # - Imprime log final.
        # - Útil para saber cuándo el servidor se apagó.
        print("Servidor finalizado.")

    # MÉTODO 2: _recv_name(conn)
    def _recv_name(self, conn: socket.socket) -> str:
        """Solicita el nombre al cliente para identificarlo en el chat.

        PROTOCOLO:
        1. Servidor envía: "NOMBRE: " (codificado en UTF-8).
        2. Cliente espera: se bloquea en input() o recv().
        3. Cliente envía: "Alice\n" (nombre + salto de línea).
        4. Servidor recibe: bytes = b'Alice\n'.
        5. Servidor decodifica: "Alice\n" (str).
        6. Servidor limpia: "Alice" (sin espacios/saltos).
        7. Servidor retorna: "Alice" (nombre limpio).

        MANEJO DE ERRORES:
        - Si hay error de socket (OSError), retorna "".
        - Si retorna "", start() rechaza este cliente.

        ARGUMENTOS:
        conn (socket.socket):
            Socket del cliente ya conectado.
            - Único para este cliente.
            - Se usa para enviar/recibir datos.

        RETORNA:
        str: El nombre del cliente, o cadena vacía si falla.
        """

        try:
            # conn.sendall("NOMBRE: ".encode(ENCODING))
            # - encode(ENCODING) convierte str a bytes.
            # - "NOMBRE: " (str) -> b'NOMBRE: ' (bytes).
            # - sendall() envía todos los bytes (reintentos si es necesario).
            # EFECTO: el cliente recibe b'NOMBRE: ' en su socket.
            # IMPORTANTE: encode() es obligatorio (sockets solo envían bytes).
            conn.sendall("NOMBRE: ".encode(ENCODING))

            # raw = conn.recv(BUFFER_SIZE)
            # - recv() bloquea esperando datos del cliente.
            # - Retorna hasta BUFFER_SIZE bytes.
            # - Si recibe 0 bytes, significa que el cliente cerró la conexión.
            # - raw es de tipo bytes.
            # EJEMPLO: si cliente envía "Alice\n", raw = b'Alice\n'.
            # NOTA: recv() puede recibir datos parciales en múltiples llamadas.
            raw = conn.recv(BUFFER_SIZE)

            # return raw.decode(ENCODING).strip()
            # - decode(ENCODING) convierte bytes a str.
            # - b'Alice\n' (bytes) -> "Alice\n" (str).
            # - strip() elimina espacios en blanco y saltos de línea al inicio/final.
            # - "Alice\n".strip() -> "Alice".
            # RESULTADO FINAL: retorna "Alice" (nombre limpio).
            return raw.decode(ENCODING).strip()

        # except OSError:
        # - OSError cubre todos los errores de socket.
        # - Ejemplos: conexión cerrada, timeout, error de red.
        except OSError:
            # return ""
            # - Retorna cadena vacía para indicar fallo.
            # - start() interpreta esto como "rechazar este cliente".
            return ""

    # MÉTODO 3: _handle_client(client)
    def _handle_client(self, client: ClientState) -> None:
        """Hilo que escucha mensajes de un cliente y los reenvia a otros.

        FLUJO DETALLADO:

        FASE 1: BARRERA (sincronización)
        1. Espera en la barrera (ready_barrier.wait()).
        2. Ambos hilos de cliente llaman a wait() simultáneamente.
        3. Cuando los 2 llaman, la barrera se rompe y ambos continúan.
        4. Evita que un cliente envíe mensajes antes del otro.

        FASE 2: LOOP DE MENSAJES (chat)
        1. Espera a recibir un mensaje: conn.recv(BUFFER_SIZE).
        2. Verifica si es "/exit" (comando de salida).
        3. Verifica sobrecarga: in_flight_limit.acquire(blocking=False).
        4. Si hay permiso: registra mensaje + reenvia a otros clientes.
        5. Libera permiso: in_flight_limit.release().
        6. Repite hasta que cliente envíe "/exit" o se desconecte.

        FASE 3: LIMPIEZA (desconexión)
        1. Marca alive=False.
        2. Registra en historial: "Alice se desconecto".
        3. Cierra socket.

        SINCRONIZACIÓN:
        - history_lock: protege acceso al historial.
        - send_semaphore: serializa envíos a otros clientes.
        - in_flight_limit: limita mensajes en procesamiento.
        - ready_barrier: sincroniza inicio de ambos clientes.

        ARGUMENTOS:
        client (ClientState):
            El estado del cliente que este hilo gestiona.
            - name, conn, addr, alive.
            - Parámetro: obtiene Cliente A en hilo 1, Cliente B en hilo 2.
        """

        try:
            # self.ready_barrier.wait()
            # - Espera en la barrera (bloquea hasta que 2 hilos lleguen).
            # - COMPORTAMIENTO:
            #   * Hilo A llama a wait() y se bloquea.
            #   * Hilo B llama a wait() y se bloquea.
            #   * Cuando ambos llaman, la barrera se rompe.
            #   * Ambos continúan simultáneamente.
            # EFECTO: garantiza que ambos clientes estén listos antes de chatear.
            # ALTERNATIVA: habría que esperar manualmente con locks/events.
            self.ready_barrier.wait()

            # self._send_to(client, "Ambos usuarios conectados. Puedes chatear.\n")
            # - Envía un mensaje de confirmación al cliente.
            # - Parámetro: cliente actual, mensaje a enviar.
            # - EFECTO: cliente ve "Ambos usuarios conectados. Puedes chatear.\n".
            # - TIMING: solo después de que ambos clientes se conecten.
            self._send_to(client, "Ambos usuarios conectados. Puedes chatear.\n")

        # except threading.BrokenBarrierError:
        # - Excepción si algo falla en la barrera.
        # - EJEMPLOS: un hilo se desconecta antes de wait(), excepción en otro hilo.
        # - Si ocurre: la barrera se "rompe" y otros hilos en wait() reciben excepción.
        except threading.BrokenBarrierError:
            # client.alive = False
            # - Marca al cliente como inactivo.
            # - EFECTO: el while client.alive no se ejecutará.
            client.alive = False

            # return
            # - Sale del método (termina el hilo).
            # - Sin esto: el hilo continuaría en el while loop.
            return

        # while client.alive:
        # - Loop principal que recibe mensajes mientras el cliente esté activo.
        # - SALE CUANDO: client.alive se ponga False.
        # - CONDICIÓN: se evalúa al inicio de cada iteración.
        while client.alive:
            try:
                # data = client.conn.recv(BUFFER_SIZE)
                # - Espera a recibir datos del cliente (bloquea indefinidamente).
                # - Retorna hasta BUFFER_SIZE bytes (ej: 2048).
                # - Si recibe 0 bytes: cliente cerró la conexión.
                # - data es de tipo bytes.
                # EJEMPLO: si cliente envía "Hola", data = b'Hola'.
                data = client.conn.recv(BUFFER_SIZE)

                # if not data:
                # - Verifica si recv() retornó bytes vacíos.
                # - not b'' = True (bytes vacíos son falsos).
                # - SIGNIFICADO: cliente cerró la conexión.
                if not data:
                    # break
                    # - Sale del while loop.
                    # - Va a la sección de limpieza (después del while).
                    break

                # message = data.decode(ENCODING).strip()
                # - Decodifica bytes a str: b'Hola' -> "Hola".
                # - strip() limpia espacios/saltos: "Hola\n" -> "Hola".
                # - message es de tipo str.
                # RESULTADO: mensaje limpio en formato texto.
                message = data.decode(ENCODING).strip()

                # if message.lower() == "/exit":
                # - Verifica si el mensaje es el comando de salida.
                # - message.lower() convierte a minúsculas: "/EXIT" -> "/exit".
                # - Si es "/exit": cliente quiere desconectarse.
                if message.lower() == "/exit":
                    # break
                    # - Sale del while loop (limpieza y cierre).
                    break

                # if not self.in_flight_limit.acquire(blocking=False):
                # - Intenta adquirir un permiso del semáforo in_flight_limit.
                # - acquire(blocking=False):
                #   * Si hay permiso: retorna True, decrementa contador, continúa.
                #   * Si no hay permiso: retorna False INMEDIATAMENTE (no bloquea).
                # - not False = True, por lo que entra al if.
                # EFECTO: si no hay permiso, entra en el bloque if (descarta mensaje).
                if not self.in_flight_limit.acquire(blocking=False):
                    # print(f"[DROP] {client.name}: sobrecarga controlada")
                    # - Imprime en consola que el mensaje fue descartado.
                    # - [DROP] = etiqueta para identificar descartes.
                    # - {client.name} = nombre del cliente ("Alice").
                    # - "sobrecarga controlada" = razón (límite alcanzado).
                    # UTILIDAD: monitoreo/debug para ver cuando se controla sobrecarga.
                    print(f"[DROP] {client.name}: sobrecarga controlada")

                    # continue
                    # - Salta al siguiente while client.alive (intenta recibir otro mensaje).
                    # - No ejecuta el resto (no registra ni reenvia).
                    continue

                try:
                    # self._register_message(f"{client.name}: {message}")
                    # - Registra el mensaje en el historial.
                    # - Parámetro: string formateado "Alice: Hola".
                    # - PROTEGIDO: history_lock evita conflictos.
                    # EFECTO: mensaje guardado en history y impreso en consola.
                    self._register_message(f"{client.name}: {message}")

                    # self._broadcast(client, f"{client.name}: {message}\n")
                    # - Reenvia el mensaje al otro cliente.
                    # - Parámetro: cliente emisor (para no enviarle a sí mismo).
                    # - Parámetro: mensaje formateado con salto de línea.
                    # EFECTO: mensaje enviado a "Alice" -> recibido por "Bob".
                    self._broadcast(client, f"{client.name}: {message}\n")

                # finally:
                # - Se ejecuta siempre, incluso si hay excepción en try.
                finally:
                    # self.in_flight_limit.release()
                    # - Libera el permiso del semáforo.
                    # - EFECTO: contador incrementa en 1 (otro mensaje puede venir).
                    # IMPORTANTE: si no hace release(), no hay más permisos (deadlock).
                    # GARANTIZADO: el finally asegura que SIEMPRE se libera.
                    self.in_flight_limit.release()

            # except OSError:
            # - Excepción si hay error de socket.
            # - EJEMPLOS: cliente cerró conexión, timeout, error de red.
            except OSError:
                # break
                # - Sale del while loop (va a limpieza).
                break

        # Limpieza después del while (cuando el cliente se desconecta).

        # client.alive = False
        # - Marca al cliente como inactivo.
        # - EFECTO: el main loop while any(c.alive...) verá esto.
        client.alive = False

        # self._register_message(f"{client.name} se desconecto")
        # - Registra en historial que el cliente se fue.
        # - EJEMPLO: "Alice se desconecto" en el historial.
        self._register_message(f"{client.name} se desconecto")

        # print(f"Desconectado: {client.name}")
        # - Imprime en consola que el cliente se desconectó.
        print(f"Desconectado: {client.name}")

        try:
            # client.conn.close()
            # - Cierra el socket del cliente.
            # - EFECTO: recursos liberados, conexión cerrada.
            client.conn.close()

        # except OSError:
        # - Si hay error al cerrar (ej: socket ya cerrado), ignora.
        except OSError:
            # pass
            # - No hacer nada, simplemente ignorar el error.
            pass

    # MÉTODO 4: _register_message(message)
    def _register_message(self, message: str) -> None:
        """Guarda un mensaje en el historial y lo imprime en consola.

        PROPÓSITO:
        Centralizar el registro de mensajes para evitar duplicación de código.
        Garantiza que history se accede siempre de forma segura (con lock).

        PROTECCIÓN:
        - history_lock: evita que dos hilos escriban history simultáneamente.
        - with self.history_lock: asegura exclusión mutua.

        EFECTOS:
        1. Mensaje se agrega a self.history (lista).
        2. Mensaje se imprime en consola (logs).

        ARGUMENTOS:
        message (str):
            El texto ya formateado a guardar.
            - EJEMPLOS: "Alice: Hola mundo", "Alice se desconecto".
            - NO incluye timestamp (se agrega aquí si es necesario).
        """

        # with self.history_lock:
        # - Context manager que adquiere el lock.
        # - Al entrar: adquiere el lock (bloquea si otro hilo lo tiene).
        # - Dentro: solo 1 hilo ejecuta simultáneamente.
        # - Al salir: libera el lock automáticamente.
        # GARANTÍA: solo 1 hilo accede a history a la vez.
        with self.history_lock:
            # self.history.append(message)
            # - Agrega el mensaje al final de la lista.
            # - PROTEGIDO: solo 1 hilo aquí por el lock.
            # EFECTO: history crece (sin límite en este servidor).
            self.history.append(message)

            # print(f"[HIST] {message}")
            # - Imprime el mensaje en consola con prefijo [HIST].
            # - PARA MONITOREO: fácil ver el historial mientras el servidor corre.
            # - EJEMPLO: "[HIST] Alice: Hola mundo"
            print(f"[HIST] {message}")

    # MÉTODO 5: _broadcast(sender, message)
    def _broadcast(self, sender: ClientState, message: str) -> None:
        """Reenvia un mensaje del emisor a todos los clientes excepto el emisor.

        PROPÓSITO:
        Implementar el chat bidireccional: cuando Alice envía un mensaje,
        Bob lo recibe.

        LÓGICA:
        1. Itera sobre self.clients (lista de clientes).
        2. Omite al cliente emisor (no se envía a sí mismo).
        3. Omite clientes inactivos (already=False).
        4. Envía al cliente restante llamando a _send_to().

        PROTECCIÓN:
        - send_semaphore: serializa envíos para evitar entrelazamiento.
        - with self.send_semaphore: solo 1 hilo envía a la vez.

        ARGUMENTOS:
        sender (ClientState):
            El cliente que envió el mensaje.
            - Se usa para no enviarle el mensaje a sí mismo (client is sender).

        message (str):
            El texto a reenviar.
            - YA formateado: "Alice: Hola mundo\n".
            - Incluye salto de línea al final.
        """

        # with self.send_semaphore:
        # - Context manager que adquiere el semáforo.
        # - Al entrar: adquiere 1 permiso (se bloquea si no hay).
        # - Dentro: solo 1 hilo ejecuta el envío.
        # - Al salir: libera el permiso automáticamente.
        # GARANTÍA: solo 1 hilo envía a la vez (evita entrelazamiento).
        with self.send_semaphore:
            # for client in self.clients:
            # - Itera sobre todos los clientes conectados.
            # - RANGO: 0, 1, o 2 clientes.
            # - VARIABLE: client es un ClientState.
            for client in self.clients:
                # if client is sender or not client.alive:
                # - CONDICIÓN 1: client is sender = ¿es el mismo cliente?
                #   * is = compara objetos (identidad, no igualdad).
                #   * Si es el cliente emisor, omite (no se envía a sí mismo).
                # - CONDICIÓN 2: not client.alive = ¿está inactivo?
                #   * Si already=False (desconectado), omite.
                # - COMBINADA: si CUALQUIERA es verdadera, omite.
                if client is sender or not client.alive:
                    # continue
                    # - Salta a la siguiente iteración del for.
                    # - No ejecuta _send_to() para este cliente.
                    continue

                # self._send_to(client, message)
                # - Envía el mensaje al cliente.
                # - Parámetro: cliente destino.
                # - Parámetro: mensaje a enviar.
                # EFECTO: cliente recibe el mensaje en su socket.
                self._send_to(client, message)

    # MÉTODO 6: _send_to(client, message)
    def _send_to(self, client: ClientState, message: str) -> None:
        """Envía un mensaje a un cliente específico de forma segura.

        PROPÓSITO:
        Abstrae el envío de bytes y maneja errores de socket.

        FLUJO:
        1. Convierte message (str) a bytes usando encode().
        2. Envía bytes usando conn.sendall().
        3. Si falla: marca cliente como inactivo.

        MANEJO DE ERRORES:
        - Si hay OSError (socket cerrado, timeout, etc.): marca alive=False.
        - El cliente será descartado en el siguiente loop.

        ARGUMENTOS:
        client (ClientState):
            El cliente destino del mensaje.
            - Se obtiene client.conn (socket) para enviar.

        message (str):
            El texto en claro a enviar.
            - YA debe estar formateado.
            - EJEMPLO: "Alice: Hola mundo\n"
        """

        try:
            # client.conn.sendall(message.encode(ENCODING))
            # - message.encode(ENCODING) convierte str a bytes.
            #   * "Hola mundo" -> b'Hola mundo'.
            #   * UTF-8 es la codificación (soporta todos los idiomas).
            # - sendall() envía TODOS los bytes (reintentos si es necesario).
            #   * A diferencia de send(): que puede enviar parcialmente.
            # - client.conn es el socket del cliente destino.
            # EFECTO: cliente recibe los bytes en su socket (recv() los obtiene).
            client.conn.sendall(message.encode(ENCODING))

        # except OSError:
        # - OSError cubre todos los errores de socket.
        # - EJEMPLOS: conexión cerrada, cliente desconectado, timeout.
        except OSError:
            # client.alive = False
            # - Marca al cliente como inactivo.
            # - EFECTO: será descartado en próximas iteraciones.
            # - NO CERRAMOS SOCKET: ya está cerrado o no responde.
            client.alive = False

# PUNTO DE ENTRADA

def main() -> None:
"""Punto de entrada principal del script del servidor.

    RESPONSABILIDAD:
    Crear una instancia de ChatServer y ejecutar el loop principal.

    VALORES POR DEFECTO:
    - host = "127.0.0.1" (localhost).
    - port = 50007 (puerto arbitrario).
    - Todos los atributos se inicializan automáticamente (@dataclass).

    ALTERNATIVA (para parámetros personalizados):
    ChatServer(host="0.0.0.0", port=8080).start()
    """

    # ChatServer()
    # - Crea una instancia de ChatServer con valores por defecto.
    # - @dataclass genera __init__ con todos los parámetros nombrados.
    # - RESULTADO: objeto listo para usar (host, port, etc. configurados).

    # .start()
    # - Ejecuta el método start() que comienza el loop de aceptación.
    # - BLOQUEANTE: comienza a escuchar y acepta clientes.
    # - RETORNA: cuando se cierren los sockets y terminen los hilos.
    ChatServer().start()

# if **name** == "**main**":

# - Convención de Python: esta línea solo se ejecuta si el script se ejecuta directamente.

# - NO se ejecuta si el script se importa como módulo en otro archivo.

# - BENEFICIO: permite usar este archivo como ejecutable O como librería importable.

if **name** == "**main**": # main() # - Llama a la función main() que inicia el servidor.
main()
