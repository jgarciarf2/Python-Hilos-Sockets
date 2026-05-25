"""
=============================================================================
SERVIDOR TCP CONCURRENTE - MODIFICACIÓN 7: TIMEOUT EN CLIENTE (LADO SERVIDOR)
=============================================================================

Archivo   : servidor.py
Módulo    : mod07_timeout_cliente
Propósito : Gestión de un servidor TCP multi-hilo con control de TIMEOUT de
            inactividad por cliente.

---------------------------------------------------------------------------
¿QUÉ ES EL TIMEOUT DE INACTIVIDAD DE CLIENTE?
---------------------------------------------------------------------------
En servidores de producción, un cliente puede conectarse pero nunca enviar
datos (por error de red, bug en el cliente, ataque lento, etc.).  Sin timeout,
el hilo que lo atiende queda bloqueado en recv() **para siempre**:

    - El hilo ocupa memoria (stack, objetos Python).
    - El semáforo no se libera → otros clientes no pueden conectarse.
    - El socket del cliente queda abierto → consume descriptor de archivo.
    - Se habla de "clientes zombis" o "conexiones colgadas".

La solución es asignarle al socket de cada cliente un timeout (settimeout).
Si el cliente no envía datos en N segundos, recv() lanza socket.timeout y el
servidor puede limpiar recursos ordena­damente.

---------------------------------------------------------------------------
MODIFICACIONES RESPECTO AL SERVIDOR BASE
---------------------------------------------------------------------------
  1. Constante TIMEOUT_CLIENTE_INACTIVO = 15
       Define cuántos segundos espera el servidor antes de considerar que un
       cliente está inactivo y cortar la conexión.

  2. conexion_cliente.settimeout(TIMEOUT_CLIENTE_INACTIVO)
       Se aplica justo al aceptar la conexión, antes de pasarla al hilo.
       Cualquier recv() sobre ese socket lanzará socket.timeout si pasan
       más de 15 s sin recibir datos.

  3. Función recibir_con_timeout(socket_cliente)
       Envuelve la llamada recv() con captura específica de socket.timeout
       para separar la lógica de red del resto del protocolo.

  4. Captura de socket.timeout en atender_cliente()
       Cuando recibir_con_timeout detecta timeout, retorna None.  El hilo
       interpreta None como "cliente inactivo" y cierra la conexión con un
       mensaje de log claro: "Cliente inactivo: tiempo de espera agotado".

---------------------------------------------------------------------------
PRIMITIVAS DE SINCRONIZACIÓN USADAS
---------------------------------------------------------------------------
  - threading.Lock       → protege lecturas/escrituras al stock_productos.
  - threading.Semaphore  → limita el número máximo de clientes simultáneos.
  - threading.Barrier    → sincroniza a los procesadores cada N pedidos.
  - threading.Event      → señal de apagado ordenado del servidor.

=============================================================================
"""

import socket        # API de sockets TCP/IP
import threading     # Hilos y primitivas de sincronización
import json          # Serialización del protocolo de mensajes
import logging       # Registro de eventos con nivel y timestamp
import time          # Medición de tiempos y retardos

# ---------------------------------------------------------------------------
# CONFIGURACIÓN DE LOGGING
# ---------------------------------------------------------------------------
# Formato: [timestamp] [nivel] mensaje
# Esto permite rastrear el flujo concurrente identificando qué hilo actúa.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ===========================================================================
# CONSTANTES DE CONFIGURACIÓN DE RED
# ===========================================================================

HOST = "127.0.0.1"
"""Dirección IP de escucha. 127.0.0.1 = loopback, solo conexiones locales."""

PORT = 65000
"""Puerto TCP. Debe estar libre; valores > 1024 no requieren privilegios root."""

ENCODING = "utf-8"
"""Codificación de texto para serializar/deserializar mensajes JSON."""

BUFFER_SIZE = 4096
"""Tamaño máximo (bytes) del buffer de recepción por mensaje."""

CAPACIDAD_MAXIMA_COLA = 10
"""Número máximo de conexiones en espera (backlog) en el socket del servidor."""

NUM_PROCESADORES = 3
"""Cantidad de hilos "procesadores" que procesan pedidos de la cola interna."""

PEDIDOS_MINIMOS_PARA_BARRERA = 5
"""
La barrera sincroniza procesadores cada vez que se acumulan esta cantidad
de pedidos completados.  Sirve para coordinar puntos de control colectivos
(reportes, auditorías, vaciado de caché, etc.).
"""

MAX_CLIENTES = 5
"""
Número máximo de clientes que pueden ser atendidos simultáneamente.
Implementado con un Semaphore; el cliente número MAX_CLIENTES+1 recibe
un mensaje de "servidor lleno" y se desconecta.
"""

# ===========================================================================
# CONSTANTE CLAVE DE ESTA MODIFICACIÓN: TIMEOUT DE INACTIVIDAD
# ===========================================================================

TIMEOUT_CLIENTE_INACTIVO = 15
"""
MODIFICACIÓN 7 - TIMEOUT DE INACTIVIDAD DE CLIENTE (lado servidor).

Cuántos segundos puede estar un cliente conectado sin enviar ningún mensaje
antes de que el servidor considere la conexión "muerta" y la cierre.

¿Por qué 15 segundos?
  - Lo suficientemente largo para tolerar latencias de red normales.
  - Lo suficientemente corto para liberar recursos antes de que el servidor
    se llene de conexiones zombis.
  - El cliente tiene un timeout de 10 s para recibir respuesta del servidor,
    así que si el cliente está vivo, nunca tardará 15 s en enviar algo.

Problema que resuelve:
  - Sin timeout: un cliente que se congela o pierde red bloquea su hilo
    indefinidamente.  Con MAX_CLIENTES=5, basta con 5 clientes zombis para
    denegar el servicio a todos los clientes legítimos.
  - Con timeout: el hilo detecta inactividad, cierra limpiamente el socket
    y libera el semáforo, permitiendo que otro cliente entre.
"""

# ===========================================================================
# INVENTARIO COMPARTIDO
# ===========================================================================

stock_productos = {
    "Laptop":      10,
    "Mouse":       25,
    "Teclado":     20,
    "Monitor":     8,
    "Auriculares": 15,
    "USB":         30,
    "Cargador":    18,
    "Webcam":      12,
}
"""
Diccionario mutable compartido entre todos los hilos.
Cualquier modificación debe realizarse bajo lock_stock para garantizar
consistencia (evitar race conditions en decrementos concurrentes).
"""

# ===========================================================================
# PRIMITIVAS DE SINCRONIZACIÓN
# ===========================================================================

lock_stock = threading.Lock()
"""
Mutex que serializa el acceso a stock_productos.

Sin este lock, dos hilos podrían leer el mismo stock=1, ambos decidir
"hay stock" y ambos decrementarlo, dejando el valor en -1 (venta doble).
"""

semaforo_clientes = threading.Semaphore(MAX_CLIENTES)
"""
Semáforo contador que limita la concurrencia a MAX_CLIENTES hilos.

  - acquire() → "entra" un cliente (decrementa contador interno).
  - release() → "sale" un cliente (incrementa contador interno).

Si el contador llega a 0, el próximo acquire() bloquea hasta que algún
cliente libere.  Aquí usamos acquire(blocking=False) para no bloquear al
hilo principal y poder responder al cliente que el servidor está lleno.
"""

barrera_procesadores = threading.Barrier(NUM_PROCESADORES)
"""
Barrera de sincronización para los hilos procesadores.

Cuando NUM_PROCESADORES hilos llegan a barrier.wait(), todos se desbloquean
al mismo tiempo.  Útil para coordinar checkpoints colectivos (ej.: generar
un reporte de ventas acumuladas antes de continuar).
"""

evento_apagado = threading.Event()
"""
Bandera de apagado del servidor.

  - evento_apagado.set()   → señala a todos los hilos que deben terminar.
  - evento_apagado.is_set() → los hilos lo comprueban para salir de sus bucles.

Permite un apagado ordenado sin matar hilos abruptamente (que dejaría locks
adquiridos y sockets abiertos).
"""

cola_pedidos = []
"""
Cola simple (lista) de pedidos pendientes de procesar.
Los hilos procesadores extraen de aquí; protegida también con lock_stock
para simplificar (en producción se usaría queue.Queue).
"""

lock_cola = threading.Lock()
"""Mutex independiente para la cola de pedidos."""

contador_pedidos = 0
"""Contador global de pedidos procesados; protegido por lock_cola."""


# ===========================================================================
# FUNCIÓN AUXILIAR: recibir_con_timeout
# ===========================================================================

def recibir_con_timeout(socket_cliente: socket.socket) -> bytes | None:
    """
    Recibe datos de un socket con manejo explícito de timeout.

    MODIFICACIÓN 7 - FUNCIÓN NUEVA.

    Esta función encapsula la llamada a recv() separando tres escenarios:

      1. Datos recibidos correctamente → retorna los bytes.
      2. Timeout de inactividad        → retorna None (el llamador decide).
      3. Error de red u otro           → propaga la excepción.

    ¿Por qué una función separada?
      - Centraliza el manejo de timeout: si el umbral cambia o se quiere
        añadir lógica de reintento, solo se modifica aquí.
      - Hace que atender_cliente() sea más legible: en lugar de un try/except
        anidado dentro del bucle principal, se llama a esta función y se
        comprueba si el resultado es None.
      - Facilita las pruebas unitarias: se puede mockear esta función sin
        modificar el hilo completo.

    Args:
        socket_cliente: Socket TCP ya configurado con settimeout().
                        El timeout ya está establecido en el socket antes
                        de llamar a esta función.

    Returns:
        bytes : datos crudos recibidos del cliente (puede ser b'' si cerró).
        None  : si expiró el timeout de inactividad (TIMEOUT_CLIENTE_INACTIVO).

    Raises:
        OSError: si hay un error de red diferente al timeout (conexión rota,
                 socket cerrado por el SO, etc.).
    """
    try:
        # recv() bloqueará hasta que:
        #   a) Lleguen datos       → retorna bytes con contenido.
        #   b) El cliente cierre   → retorna b'' (cadena vacía).
        #   c) Expire el timeout   → lanza socket.timeout.
        datos = socket_cliente.recv(BUFFER_SIZE)
        return datos

    except socket.timeout:
        # El cliente no envió nada en TIMEOUT_CLIENTE_INACTIVO segundos.
        # Retornamos None como señal de inactividad; el llamador limpiará.
        # NO propagamos la excepción porque este caso es esperado y controlado.
        log.warning(
            "recibir_con_timeout: timeout expirado (%d s), cliente inactivo.",
            TIMEOUT_CLIENTE_INACTIVO,
        )
        return None


# ===========================================================================
# FUNCIÓN: enviar_respuesta
# ===========================================================================

def enviar_respuesta(socket_cliente: socket.socket, respuesta: dict) -> None:
    """
    Serializa un diccionario como JSON y lo envía al cliente.

    Args:
        socket_cliente : Socket del cliente destino.
        respuesta      : Diccionario con los campos de la respuesta.

    Raises:
        OSError: si el socket ya no está disponible (cliente desconectado).
    """
    mensaje_json = json.dumps(respuesta, ensure_ascii=False)
    datos_bytes  = mensaje_json.encode(ENCODING)
    socket_cliente.sendall(datos_bytes)
    log.debug("Respuesta enviada: %s", mensaje_json)


# ===========================================================================
# FUNCIÓN: procesar_pedido
# ===========================================================================

def procesar_pedido(pedido: dict) -> dict:
    """
    Evalúa un pedido de compra y actualiza el stock si corresponde.

    Esta función se ejecuta dentro del hilo de atención al cliente y está
    protegida por lock_stock para garantizar consistencia.

    Args:
        pedido: Diccionario con claves 'producto' y 'cantidad'.

    Returns:
        Diccionario con 'status' ('ok' o 'error') y un 'mensaje' descriptivo.
    """
    producto  = pedido.get("producto", "")
    cantidad  = pedido.get("cantidad", 0)

    with lock_stock:
        # --- Validación de producto ---
        if producto not in stock_productos:
            return {
                "status":  "error",
                "mensaje": f"Producto '{producto}' no existe en el catálogo.",
            }

        # --- Validación de cantidad ---
        if not isinstance(cantidad, int) or cantidad <= 0:
            return {
                "status":  "error",
                "mensaje": "La cantidad debe ser un entero positivo.",
            }

        stock_actual = stock_productos[producto]

        # --- Verificación de stock suficiente ---
        if stock_actual < cantidad:
            return {
                "status":  "error",
                "mensaje": (
                    f"Stock insuficiente para '{producto}'. "
                    f"Disponible: {stock_actual}, solicitado: {cantidad}."
                ),
            }

        # --- Confirmar venta y decrementar stock ---
        stock_productos[producto] -= cantidad
        log.info(
            "Venta: %d x %s | Stock restante: %d",
            cantidad, producto, stock_productos[producto],
        )
        return {
            "status":  "ok",
            "mensaje": (
                f"Compra de {cantidad} x '{producto}' confirmada. "
                f"Stock restante: {stock_productos[producto]}."
            ),
        }


# ===========================================================================
# FUNCIÓN: atender_cliente  (hilo de atención)
# ===========================================================================

def atender_cliente(
    conexion_cliente: socket.socket,
    direccion_cliente: tuple,
) -> None:
    """
    Hilo de atención: gestiona la sesión completa de un cliente.

    Flujo:
      1. Bienvenida inicial.
      2. Bucle de recepción de mensajes:
           a. Llama a recibir_con_timeout().
           b. Si None → timeout: desconectar con mensaje de inactividad.
           c. Si b''  → cliente cerró la conexión limpiamente.
           d. Si datos válidos → deserializar JSON y procesar pedido.
      3. Cierre limpio del socket y liberación del semáforo.

    MODIFICACIÓN 7 - CAMBIOS EN ESTA FUNCIÓN:
      - Se llama a recibir_con_timeout() en vez de recv() directo.
      - Se añade la rama "if datos is None" para manejar el timeout.
      - El mensaje de desconexión por inactividad es:
            "Cliente inactivo: tiempo de espera agotado"

    Args:
        conexion_cliente  : Socket TCP ya conectado con el cliente.
                            Ya tiene settimeout(TIMEOUT_CLIENTE_INACTIVO)
                            aplicado antes de llamar a esta función.
        direccion_cliente : Tupla (IP, puerto) del cliente remoto.
    """
    id_cliente = f"{direccion_cliente[0]}:{direccion_cliente[1]}"
    log.info("Hilo iniciado para cliente %s", id_cliente)

    try:
        # --- Mensaje de bienvenida ---
        bienvenida = {
            "status":  "bienvenida",
            "mensaje": (
                f"Bienvenido al servidor de ventas. "
                f"Timeout de inactividad: {TIMEOUT_CLIENTE_INACTIVO} s."
            ),
        }
        enviar_respuesta(conexion_cliente, bienvenida)

        # -------------------------------------------------------------------
        # BUCLE PRINCIPAL DE ATENCIÓN
        # -------------------------------------------------------------------
        while not evento_apagado.is_set():

            # ---------------------------------------------------------------
            # MODIFICACIÓN 7: usar recibir_con_timeout() en vez de recv()
            # ---------------------------------------------------------------
            # recv() bloqueaba indefinidamente si el cliente no enviaba nada.
            # recibir_con_timeout() retorna None al expirar el timeout, lo
            # que permite al servidor liberar recursos en lugar de quedar colgado.
            datos = recibir_con_timeout(conexion_cliente)

            # ---------------------------------------------------------------
            # CASO 1: TIMEOUT DE INACTIVIDAD
            # ---------------------------------------------------------------
            if datos is None:
                # El cliente no envió datos en TIMEOUT_CLIENTE_INACTIVO segundos.
                # Enviamos un mensaje de despedida y cerramos la conexión.
                # Esto evita que el hilo y el semáforo queden bloqueados
                # indefinidamente por un cliente "zombi" o con red caída.
                log.warning(
                    "Cliente %s inactivo por %d s. Cerrando conexión.",
                    id_cliente, TIMEOUT_CLIENTE_INACTIVO,
                )
                # Intentamos notificar al cliente (puede fallar si su red cayó)
                try:
                    aviso_timeout = {
                        "status":  "timeout",
                        "mensaje": "Cliente inactivo: tiempo de espera agotado",
                    }
                    enviar_respuesta(conexion_cliente, aviso_timeout)
                except OSError:
                    # Si el socket ya no está disponible, ignoramos el error.
                    pass
                # Salimos del bucle → el bloque finally cerrará el socket.
                break

            # ---------------------------------------------------------------
            # CASO 2: CLIENTE CERRÓ LA CONEXIÓN LIMPIAMENTE
            # ---------------------------------------------------------------
            if datos == b"":
                # recv() retorna b'' cuando el peer cierra su lado del socket
                # (FIN TCP).  Es un cierre "ordenado".
                log.info("Cliente %s desconectado limpiamente.", id_cliente)
                break

            # ---------------------------------------------------------------
            # CASO 3: DATOS RECIBIDOS → PROCESAR PEDIDO
            # ---------------------------------------------------------------
            try:
                mensaje = json.loads(datos.decode(ENCODING))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                log.error(
                    "Cliente %s envió datos mal formados: %s", id_cliente, exc
                )
                enviar_respuesta(
                    conexion_cliente,
                    {"status": "error", "mensaje": "Formato JSON inválido."},
                )
                continue  # Seguir esperando mensajes válidos

            # Determinar tipo de mensaje
            tipo = mensaje.get("tipo", "")

            if tipo == "pedido":
                respuesta = procesar_pedido(mensaje)
                enviar_respuesta(conexion_cliente, respuesta)

            elif tipo == "stock":
                # Consulta de catálogo (operación de solo lectura)
                with lock_stock:
                    catalogo = dict(stock_productos)
                enviar_respuesta(
                    conexion_cliente,
                    {"status": "ok", "catalogo": catalogo},
                )

            elif tipo == "desconectar":
                log.info("Cliente %s solicitó desconexión.", id_cliente)
                enviar_respuesta(
                    conexion_cliente,
                    {"status": "ok", "mensaje": "Hasta luego."},
                )
                break

            else:
                enviar_respuesta(
                    conexion_cliente,
                    {
                        "status":  "error",
                        "mensaje": f"Tipo de mensaje desconocido: '{tipo}'.",
                    },
                )

    except OSError as exc:
        # Error de red no relacionado con timeout (cable desconectado, etc.)
        log.error("Error de red con cliente %s: %s", id_cliente, exc)

    finally:
        # -------------------------------------------------------------------
        # LIMPIEZA GARANTIZADA
        # -------------------------------------------------------------------
        # El bloque finally siempre se ejecuta, sin importar cómo salimos
        # del bucle (timeout, desconexión limpia, error, apagado del servidor).

        # 1. Cerrar el socket del cliente → libera el descriptor de archivo.
        try:
            conexion_cliente.close()
        except OSError:
            pass

        # 2. Liberar el semáforo → permite que otro cliente se conecte.
        #    Si no se llama a release(), el semáforo nunca sube y
        #    eventualmente ningún cliente nuevo podrá entrar.
        semaforo_clientes.release()
        log.info("Recursos liberados para cliente %s.", id_cliente)


# ===========================================================================
# FUNCIÓN: hilo_procesador  (worker de cola)
# ===========================================================================

def hilo_procesador(id_procesador: int) -> None:
    """
    Hilo de procesamiento secundario que consume pedidos de la cola interna.

    Estos hilos son independientes de los hilos de atención al cliente.
    Sirven para demostrar el uso de Barrier: cada NUM_PROCESADORES pedidos,
    todos los procesadores se sincronizan en un punto de control colectivo.

    Args:
        id_procesador: Identificador numérico del procesador (para logs).
    """
    global contador_pedidos

    log.info("Procesador #%d iniciado.", id_procesador)

    while not evento_apagado.is_set():
        pedido_local = None

        with lock_cola:
            if cola_pedidos:
                pedido_local = cola_pedidos.pop(0)
                log(f"[COLA] Estado: {cola_pedidos}")
                contador_pedidos += 1

        if pedido_local is not None:
            log.info(
                "Procesador #%d procesando: %s", id_procesador, pedido_local
            )
            # Simular trabajo de procesamiento
            time.sleep(0.1)

            # --- BARRERA: sincronización colectiva ---
            if contador_pedidos % PEDIDOS_MINIMOS_PARA_BARRERA == 0:
                log.info(
                    "Procesador #%d llegó a la barrera (pedido #%d). "
                    "Esperando a los demás...",
                    id_procesador, contador_pedidos,
                )
                try:
                    barrera_procesadores.wait(timeout=5.0)
                    log.info(
                        "Procesador #%d cruzó la barrera.", id_procesador
                    )
                except threading.BrokenBarrierError:
                    log.warning(
                        "Procesador #%d: barrera rota (apagado).", id_procesador
                    )
                    break
        else:
            # Sin pedidos pendientes: esperar un poco antes de reintentar
            time.sleep(0.05)

    log.info("Procesador #%d finalizado.", id_procesador)


# ===========================================================================
# FUNCIÓN PRINCIPAL: iniciar_servidor
# ===========================================================================

def iniciar_servidor() -> None:
    """
    Punto de entrada del servidor TCP.

    Pasos:
      1. Crear y configurar el socket del servidor.
      2. Lanzar los hilos procesadores de cola.
      3. Bucle de aceptación de conexiones:
           a. Verificar semáforo (capacidad disponible).
           b. Aplicar settimeout(TIMEOUT_CLIENTE_INACTIVO) al socket del cliente.
           c. Lanzar hilo de atención.
      4. Apagado ordenado al recibir KeyboardInterrupt.
    """
    # -----------------------------------------------------------------------
    # Lanzar hilos procesadores en segundo plano
    # -----------------------------------------------------------------------
    procesadores = []
    for i in range(NUM_PROCESADORES):
        t = threading.Thread(
            target=hilo_procesador,
            args=(i + 1,),
            daemon=True,   # Se detienen cuando el hilo principal termina
            name=f"Procesador-{i+1}",
        )
        t.start()
        procesadores.append(t)

    # -----------------------------------------------------------------------
    # Configurar el socket del servidor
    # -----------------------------------------------------------------------
    servidor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # SO_REUSEADDR: permite reutilizar el puerto inmediatamente después de
    # cerrar el servidor, evitando el error "Address already in use" durante
    # el estado TIME_WAIT de TCP.
    servidor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Timeout en el socket del servidor para que accept() no bloquee
    # indefinidamente y el bucle pueda comprobar evento_apagado.
    servidor_socket.settimeout(1.0)

    servidor_socket.bind((HOST, PORT))
    servidor_socket.listen(CAPACIDAD_MAXIMA_COLA)

    log.info(
        "Servidor escuchando en %s:%d | Max clientes: %d | "
        "Timeout inactividad cliente: %d s",
        HOST, PORT, MAX_CLIENTES, TIMEOUT_CLIENTE_INACTIVO,
    )

    # -----------------------------------------------------------------------
    # Bucle de aceptación de conexiones
    # -----------------------------------------------------------------------
    try:
        while not evento_apagado.is_set():
            try:
                conexion, direccion = servidor_socket.accept()
            except socket.timeout:
                # No hay conexiones pendientes en este segundo; reintentamos.
                # Este timeout del servidor (1 s) es distinto al timeout de
                # inactividad del cliente (TIMEOUT_CLIENTE_INACTIVO = 15 s).
                continue

            log.info("Nueva conexión desde %s:%d", *direccion)

            # --- Comprobar capacidad (semáforo) ---
            if not semaforo_clientes.acquire(blocking=False):
                # Servidor lleno: rechazamos educadamente sin bloquear el bucle.
                log.warning("Servidor lleno. Rechazando %s:%d", *direccion)
                try:
                    rechazo = {
                        "status":  "error",
                        "mensaje": (
                            f"Servidor lleno (máx. {MAX_CLIENTES} clientes). "
                            "Intente más tarde."
                        ),
                    }
                    conexion.sendall(
                        json.dumps(rechazo, ensure_ascii=False).encode(ENCODING)
                    )
                finally:
                    conexion.close()
                continue

            # -----------------------------------------------------------------
            # MODIFICACIÓN 7: Aplicar timeout de inactividad al socket del cliente
            # -----------------------------------------------------------------
            # settimeout() configura el socket en modo "timeout":
            #   - Las operaciones de E/S (recv, send) esperan como máximo
            #     TIMEOUT_CLIENTE_INACTIVO segundos.
            #   - Si se supera ese tiempo, lanzan socket.timeout.
            #
            # Aplicamos este timeout ANTES de lanzar el hilo para que toda
            # la sesión del cliente esté cubierta desde el primer momento.
            #
            # Sin este timeout, un cliente que se conecta y luego "congela"
            # (red caída, proceso suspendido, bug) mantiene el hilo bloqueado
            # en recv() indefinidamente → "cliente zombi".
            conexion.settimeout(TIMEOUT_CLIENTE_INACTIVO)
            log.info(
                "Timeout de inactividad configurado: %d s para %s:%d",
                TIMEOUT_CLIENTE_INACTIVO, *direccion,
            )

            # --- Lanzar hilo de atención ---
            hilo = threading.Thread(
                target=atender_cliente,
                args=(conexion, direccion),
                daemon=True,
                name=f"Cliente-{direccion[0]}:{direccion[1]}",
            )
            hilo.start()

    except KeyboardInterrupt:
        log.info("Apagado solicitado por el usuario (Ctrl+C).")

    finally:
        # -------------------------------------------------------------------
        # APAGADO ORDENADO
        # -------------------------------------------------------------------
        log.info("Iniciando apagado ordenado del servidor...")
        evento_apagado.set()                  # Señalar a todos los hilos
        barrera_procesadores.abort()          # Desbloquear hilos en barrera
        servidor_socket.close()               # Cerrar socket de escucha
        log.info("Servidor detenido correctamente.")


# ===========================================================================
# PUNTO DE ENTRADA
# ===========================================================================

if __name__ == "__main__":
    iniciar_servidor()
