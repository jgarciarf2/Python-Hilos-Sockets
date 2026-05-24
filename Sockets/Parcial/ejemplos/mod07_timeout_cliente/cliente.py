"""
=============================================================================
CLIENTE TCP - MODIFICACIÓN 7: TIMEOUT EN EL CLIENTE (no espera infinitamente)
=============================================================================

Archivo   : cliente.py
Módulo    : mod07_timeout_cliente
Propósito : Cliente TCP que establece un TIMEOUT en su socket para que
            recv() no espere indefinidamente la respuesta del servidor.

---------------------------------------------------------------------------
¿QUÉ PROBLEMA RESUELVE EL TIMEOUT EN EL CLIENTE?
---------------------------------------------------------------------------
Sin timeout en el socket del cliente, la llamada a recv() puede bloquearse
para siempre en los siguientes escenarios:

  1. El servidor está caído y nadie atiende la conexión ya establecida.
  2. El servidor procesa el pedido pero su respuesta se pierde en la red.
  3. El servidor tiene un bug interno y no responde a ciertos mensajes.
  4. La red experimenta alta latencia o pérdida de paquetes.
  5. El servidor está sobrecargado y tarda mucho en responder.

En todos estos casos, sin timeout el cliente queda "congelado" sin
posibilidad de recuperarse, lo que en aplicaciones de producción se
traduce en hilos bloqueados, interfaces de usuario no responsivas o
procesos zombie.

---------------------------------------------------------------------------
SOLUCIÓN: cliente_socket.settimeout(TIMEOUT_SOCKET)
---------------------------------------------------------------------------
Al configurar un timeout en el socket:

  - recv() espera como máximo TIMEOUT_SOCKET segundos la respuesta.
  - Si no llega respuesta en ese tiempo, se lanza socket.timeout.
  - El cliente captura la excepción, informa al usuario y puede:
      a) Reintentar la operación.
      b) Intentar conectar a un servidor alternativo (failover).
      c) Terminar limpiamente sin quedar bloqueado.

---------------------------------------------------------------------------
MODIFICACIONES RESPECTO AL CLIENTE BASE
---------------------------------------------------------------------------
  1. Constante TIMEOUT_SOCKET = 10
       Define cuántos segundos espera el cliente la respuesta del servidor
       antes de lanzar socket.timeout.

  2. cliente_socket.settimeout(TIMEOUT_SOCKET)
       Se aplica al socket justo después de crearlo, antes de connect().
       Esto cubre también la fase de conexión: si el servidor no responde
       al SYN TCP en 10 s, connect() también lanzará socket.timeout.

  3. Captura de socket.timeout en recibir_respuesta() y enviar_pedido()
       Cada operación de red puede expirar; se captura en el punto más
       cercano para dar mensajes de error específicos al usuario.

  4. Mensaje al usuario: 'TIMEOUT: El servidor no respondió en 10 segundos'
       Mensaje claro que distingue el timeout de otros errores de red.

=============================================================================
"""

import socket   # API de sockets TCP/IP
import json     # Serialización del protocolo de mensajes
import time     # Retardos opcionales entre reintentos

# ===========================================================================
# CONSTANTES DE CONFIGURACIÓN
# ===========================================================================

HOST = "127.0.0.1"
"""Dirección IP del servidor al que se conecta el cliente."""

PORT = 65000
"""Puerto TCP del servidor. Debe coincidir con servidor.py."""

ENCODING = "utf-8"
"""Codificación de texto para mensajes JSON."""

BUFFER_SIZE = 4096
"""Tamaño máximo (bytes) del buffer de recepción."""

# ===========================================================================
# CONSTANTE CLAVE DE ESTA MODIFICACIÓN: TIMEOUT DEL SOCKET DEL CLIENTE
# ===========================================================================

TIMEOUT_SOCKET = 10
"""
MODIFICACIÓN 7 - TIMEOUT DEL SOCKET DEL CLIENTE.

Número de segundos que el cliente espera la respuesta del servidor
antes de lanzar socket.timeout y mostrar el mensaje de error.

¿Por qué 10 segundos?
  - En una red local (loopback), el servidor debería responder en
    milisegundos.  10 s es un margen generoso para tolerar carga.
  - El servidor tiene un timeout de inactividad de 15 s.  Usando un
    valor menor (10 < 15), el cliente siempre sabrá que "algo fue mal"
    antes de que el servidor lo desconecte por inactividad.
  - Si el cliente legítimo tarda más de 10 s en recibir respuesta, es
    señal de un problema grave en el servidor o la red.

¿Qué operaciones cubre este timeout?
  - connect()  → Si el servidor no acepta la conexión en 10 s.
  - recv()     → Si el servidor no envía respuesta en 10 s.
  - send()     → En teoría también, aunque es raro que send() bloquee.

Relación con el timeout del servidor:
  - El servidor espera hasta 15 s sin datos del cliente antes de
    desconectar.  El cliente enviará su pedido mucho antes de ese límite
    (el timeout del cliente solo aplica a recv(), no entre pedidos).
"""

# ===========================================================================
# FUNCIÓN: crear_socket_con_timeout
# ===========================================================================

def crear_socket_con_timeout() -> socket.socket:
    """
    Crea un socket TCP y le aplica el timeout de TIMEOUT_SOCKET segundos.

    MODIFICACIÓN 7 - FUNCIÓN NUEVA.

    Separar la creación del socket en su propia función tiene dos ventajas:
      1. Documenta claramente que el timeout se aplica antes de connect().
      2. Facilita la reutilización si se quiere reconectar al servidor tras
         un fallo (se crea un nuevo socket limpio con su timeout ya listo).

    ¿Por qué aplicar el timeout antes de connect() y no después?
      - Si el servidor está caído o no existe, connect() puede bloquearse
        hasta que el SO agote su propio timeout TCP (hasta 75 s en Linux).
      - Con settimeout(TIMEOUT_SOCKET) aplicado antes de connect(), la
        conexión fallará en exactamente TIMEOUT_SOCKET segundos si el
        servidor no responde al handshake inicial.

    Returns:
        socket.socket: Socket TCP configurado con timeout de TIMEOUT_SOCKET s.
    """
    # Crear socket TCP/IPv4
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # -----------------------------------------------------------------
    # MODIFICACIÓN 7: Aplicar timeout al socket del cliente
    # -----------------------------------------------------------------
    # settimeout(n) pone el socket en modo "timeout":
    #   - Modo bloqueante (default):  las operaciones esperan indefinidamente.
    #   - Modo no bloqueante:         las operaciones fallan inmediatamente si
    #                                 no hay datos disponibles.
    #   - Modo timeout:               las operaciones esperan hasta n segundos;
    #                                 si se agota, lanzan socket.timeout.
    #
    # Usamos el modo timeout porque queremos esperar una respuesta razonable
    # pero no para siempre.
    sock.settimeout(TIMEOUT_SOCKET)
    print(f"[INFO] Timeout del socket configurado: {TIMEOUT_SOCKET} segundos.")

    return sock


# ===========================================================================
# FUNCIÓN: recibir_respuesta
# ===========================================================================

def recibir_respuesta(cliente_socket: socket.socket) -> dict | None:
    """
    Recibe y deserializa la respuesta JSON del servidor.

    MODIFICACIÓN 7 - MANEJO DE TIMEOUT AÑADIDO.

    Si el servidor no responde en TIMEOUT_SOCKET segundos, recv() lanza
    socket.timeout.  Esta función captura esa excepción, imprime el mensaje
    de timeout estándar y retorna None para que el llamador pueda decidir
    qué hacer (reintentar, salir, etc.).

    Args:
        cliente_socket: Socket TCP conectado al servidor.

    Returns:
        dict : Diccionario con la respuesta del servidor si se recibió.
        None : Si expiró el timeout o hubo un error de red.
    """
    try:
        # recv() bloqueará hasta que:
        #   a) El servidor envíe datos → retorna bytes.
        #   b) El servidor cierre → retorna b''.
        #   c) Expire TIMEOUT_SOCKET → lanza socket.timeout.
        datos = cliente_socket.recv(BUFFER_SIZE)

        if not datos:
            # El servidor cerró la conexión (envió FIN TCP sin datos).
            print("[AVISO] El servidor cerró la conexión.")
            return None

        # Decodificar bytes → string → dict
        respuesta = json.loads(datos.decode(ENCODING))
        return respuesta

    except socket.timeout:
        # -----------------------------------------------------------------
        # MODIFICACIÓN 7: Captura del timeout del cliente
        # -----------------------------------------------------------------
        # Este es el mensaje exacto requerido por la especificación.
        # Se muestra cuando recv() espera más de TIMEOUT_SOCKET segundos
        # sin recibir ningún byte del servidor.
        #
        # Posibles causas:
        #   - El servidor está procesando una operación muy lenta.
        #   - El servidor cayó justo después de recibir el pedido.
        #   - Hay un problema de red entre cliente y servidor.
        #   - El servidor tiene un deadlock interno (bug de concurrencia).
        print(f"TIMEOUT: El servidor no respondió en {TIMEOUT_SOCKET} segundos")
        return None

    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        # El servidor envió datos, pero no son JSON válido.
        # Poco probable con nuestro servidor, pero debe manejarse.
        print(f"[ERROR] Respuesta mal formada del servidor: {exc}")
        return None

    except OSError as exc:
        # Error de red general (socket cerrado, conexión rota, etc.)
        print(f"[ERROR] Error de red al recibir: {exc}")
        return None


# ===========================================================================
# FUNCIÓN: enviar_mensaje
# ===========================================================================

def enviar_mensaje(cliente_socket: socket.socket, mensaje: dict) -> bool:
    """
    Serializa y envía un mensaje JSON al servidor.

    Args:
        cliente_socket : Socket TCP conectado.
        mensaje        : Diccionario a enviar como JSON.

    Returns:
        True  si el mensaje se envió correctamente.
        False si hubo un error (timeout o error de red).
    """
    try:
        datos = json.dumps(mensaje, ensure_ascii=False).encode(ENCODING)
        cliente_socket.sendall(datos)
        return True

    except socket.timeout:
        # Aunque send() raramente se bloquea en loopback, puede ocurrir
        # si los buffers TCP del kernel están llenos (servidor muy lento).
        print(f"TIMEOUT: El servidor no respondió en {TIMEOUT_SOCKET} segundos")
        return False

    except OSError as exc:
        print(f"[ERROR] No se pudo enviar el mensaje: {exc}")
        return False


# ===========================================================================
# FUNCIÓN: consultar_stock
# ===========================================================================

def consultar_stock(cliente_socket: socket.socket) -> None:
    """
    Solicita al servidor el catálogo de productos con su stock actual.

    Demuestra el ciclo request-response con manejo de timeout:
      1. Enviar mensaje de tipo 'stock'.
      2. Esperar respuesta (sujeta a TIMEOUT_SOCKET).
      3. Mostrar catálogo o mensaje de error/timeout.

    Args:
        cliente_socket: Socket conectado al servidor.
    """
    print("\n--- Consultando catálogo de productos ---")

    if not enviar_mensaje(cliente_socket, {"tipo": "stock"}):
        # enviar_mensaje ya imprimió el mensaje de error o timeout
        return

    respuesta = recibir_respuesta(cliente_socket)
    if respuesta is None:
        # recibir_respuesta ya imprimió "TIMEOUT: ..." u otro error
        return

    if respuesta.get("status") == "ok":
        catalogo = respuesta.get("catalogo", {})
        print(f"{'Producto':<15} {'Stock':>8}")
        print("-" * 25)
        for producto, cantidad in catalogo.items():
            print(f"{producto:<15} {cantidad:>8}")
    else:
        print(f"[ERROR] {respuesta.get('mensaje', 'Error desconocido.')}")


# ===========================================================================
# FUNCIÓN: realizar_pedido
# ===========================================================================

def realizar_pedido(
    cliente_socket: socket.socket,
    producto: str,
    cantidad: int,
) -> None:
    """
    Envía un pedido de compra al servidor y muestra el resultado.

    Ciclo completo con manejo de timeout en ambos sentidos (envío y recepción).

    Args:
        cliente_socket : Socket conectado.
        producto       : Nombre del producto a comprar.
        cantidad       : Número de unidades solicitadas.
    """
    print(f"\n--- Pedido: {cantidad} x {producto} ---")

    mensaje = {
        "tipo":     "pedido",
        "producto": producto,
        "cantidad": cantidad,
    }

    if not enviar_mensaje(cliente_socket, mensaje):
        return  # Error ya reportado por enviar_mensaje

    # Esperar respuesta del servidor (timeout aplica aquí)
    respuesta = recibir_respuesta(cliente_socket)
    if respuesta is None:
        return  # Timeout o error ya reportado

    estado   = respuesta.get("status", "?")
    msg_resp = respuesta.get("mensaje", "Sin mensaje.")

    if estado == "ok":
        print(f"[OK] {msg_resp}")
    else:
        print(f"[ERROR] {msg_resp}")


# ===========================================================================
# FUNCIÓN: demostrar_timeout
# ===========================================================================

def demostrar_timeout(cliente_socket: socket.socket) -> None:
    """
    Demuestra el comportamiento del timeout del cliente.

    MODIFICACIÓN 7 - FUNCIÓN DE DEMOSTRACIÓN.

    Esta función ilustra cómo el cliente espera activamente la respuesta
    del servidor hasta que expira TIMEOUT_SOCKET.

    En un escenario real de timeout, el servidor no respondería.  Aquí
    simplemente añadimos un sleep() en el servidor para simularlo.
    Como el servidor de esta demo sí responde rápido, esta función muestra
    el flujo normal; para ver el timeout real, detenga el servidor y
    observe cómo recv() espera exactamente TIMEOUT_SOCKET segundos antes
    de mostrar el mensaje de error.

    Args:
        cliente_socket: Socket conectado al servidor.
    """
    print("\n=== DEMOSTRACIÓN DE TIMEOUT ===")
    print(f"El cliente esperará hasta {TIMEOUT_SOCKET} s la respuesta.")
    print("Si el servidor no responde, verá el mensaje de TIMEOUT.")
    print("Para probar: detenga el servidor (Ctrl+C) y observe el resultado.")

    # Realizamos un pedido normal; si el servidor está activo, responde rápido.
    # Si el servidor está caído, recv() expira y muestra el mensaje de timeout.
    realizar_pedido(cliente_socket, "Mouse", 1)


# ===========================================================================
# FUNCIÓN PRINCIPAL: main
# ===========================================================================

def main() -> None:
    """
    Punto de entrada del cliente TCP con timeout.

    Flujo completo:
      1. Crear socket con timeout configurado.
      2. Intentar conexión al servidor (cubierta por el timeout).
      3. Recibir mensaje de bienvenida.
      4. Ejecutar casos de uso: consulta de stock y pedidos.
      5. Demostrar el comportamiento del timeout.
      6. Desconectarse limpiamente.
      7. Cerrar el socket en el bloque finally.
    """
    print("=" * 60)
    print("CLIENTE TCP - MODIFICACIÓN 7: TIMEOUT EN EL CLIENTE")
    print(f"Servidor destino : {HOST}:{PORT}")
    print(f"Timeout del socket: {TIMEOUT_SOCKET} segundos")
    print("=" * 60)

    # Crear el socket con timeout ya configurado (MODIFICACIÓN 7)
    cliente_socket = crear_socket_con_timeout()

    try:
        # -------------------------------------------------------------------
        # PASO 1: Conectar al servidor
        # -------------------------------------------------------------------
        # connect() también está cubierta por el timeout: si el servidor no
        # responde al handshake TCP en TIMEOUT_SOCKET segundos, se lanza
        # socket.timeout en este punto (antes de recv() o send()).
        print(f"\n[INFO] Conectando a {HOST}:{PORT}...")
        try:
            cliente_socket.connect((HOST, PORT))
            print("[INFO] Conexión establecida.")
        except socket.timeout:
            # El servidor no completó el handshake TCP en TIMEOUT_SOCKET s.
            # Puede estar caído, sobrecargado o la dirección es incorrecta.
            print(f"TIMEOUT: El servidor no respondió en {TIMEOUT_SOCKET} segundos")
            return  # No hay nada más que hacer sin conexión
        except ConnectionRefusedError:
            # El servidor rechazó activamente la conexión (no está escuchando
            # en ese puerto).  Distinto del timeout: aquí la respuesta es
            # inmediata (RST TCP), no un silencio.
            print(
                f"[ERROR] Conexión rechazada: {HOST}:{PORT} no disponible. "
                "¿Está el servidor ejecutándose?"
            )
            return

        # -------------------------------------------------------------------
        # PASO 2: Recibir mensaje de bienvenida del servidor
        # -------------------------------------------------------------------
        print("\n[INFO] Esperando mensaje de bienvenida...")
        bienvenida = recibir_respuesta(cliente_socket)

        if bienvenida is None:
            # Si hay timeout aquí: el servidor aceptó la conexión pero no
            # envió bienvenida en 10 s → comportamiento inesperado del servidor.
            print("[ERROR] No se recibió bienvenida del servidor. Abortando.")
            return

        # El servidor también puede haber rechazado (servidor lleno)
        if bienvenida.get("status") == "error":
            print(f"[SERVIDOR] {bienvenida.get('mensaje', 'Error.')}")
            return

        print(f"[SERVIDOR] {bienvenida.get('mensaje', '')}")

        # -------------------------------------------------------------------
        # PASO 3: Consultar catálogo de productos
        # -------------------------------------------------------------------
        consultar_stock(cliente_socket)

        # -------------------------------------------------------------------
        # PASO 4: Realizar pedidos de prueba
        # -------------------------------------------------------------------
        # Pedido válido: producto y cantidad existentes
        realizar_pedido(cliente_socket, "Laptop", 2)

        # Pedido de producto inexistente
        realizar_pedido(cliente_socket, "Drone", 1)

        # Pedido con cantidad excesiva
        realizar_pedido(cliente_socket, "Monitor", 100)

        # Pedido válido de menor precio
        realizar_pedido(cliente_socket, "USB", 5)

        # -------------------------------------------------------------------
        # PASO 5: Demostración de timeout
        # -------------------------------------------------------------------
        demostrar_timeout(cliente_socket)

        # -------------------------------------------------------------------
        # PASO 6: Desconexión limpia
        # -------------------------------------------------------------------
        # Enviamos un mensaje de tipo 'desconectar' para que el servidor
        # cierre su hilo de atención de forma ordenada, en lugar de detectar
        # un cierre abrupto del socket.
        print("\n[INFO] Enviando solicitud de desconexión...")
        if enviar_mensaje(cliente_socket, {"tipo": "desconectar"}):
            despedida = recibir_respuesta(cliente_socket)
            if despedida:
                print(f"[SERVIDOR] {despedida.get('mensaje', '')}")

    except KeyboardInterrupt:
        # El usuario presionó Ctrl+C durante la sesión.
        print("\n[INFO] Sesión interrumpida por el usuario.")

    finally:
        # -------------------------------------------------------------------
        # LIMPIEZA GARANTIZADA
        # -------------------------------------------------------------------
        # El bloque finally cierra el socket en cualquier caso de salida:
        #   - Flujo normal completado.
        #   - Timeout de conexión o recv.
        #   - KeyboardInterrupt del usuario.
        #   - Cualquier otra excepción no capturada.
        #
        # Cerrar el socket envía FIN TCP al servidor, lo que le permite
        # detectar la desconexión incluso si no se envió el mensaje 'desconectar'.
        print("[INFO] Cerrando socket del cliente.")
        try:
            cliente_socket.close()
        except OSError:
            pass
        print("[INFO] Cliente finalizado.")


# ===========================================================================
# PUNTO DE ENTRADA
# ===========================================================================

if __name__ == "__main__":
    main()
