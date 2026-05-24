"""
================================================================================
ARCHIVO: cliente.py
MODIFICACIÓN 3: Guardar LOG en archivo de texto
================================================================================

DESCRIPCIÓN GENERAL:
    Cliente TCP que se conecta al servidor de almacén, envía pedidos de
    productos usando el protocolo JSON definido, y procesa las respuestas.

    Puede ejecutarse en modo INTERACTIVO (menú en consola) o en modo
    AUTOMÁTICO (ejecuta una lista predefinida de pedidos de prueba).

    MODIFICACIÓN 3 - NOVEDAD PRINCIPAL:
    ====================================
    El cliente también guarda su propio log en un archivo 'cliente_log.txt',
    con el MISMO formato que el servidor. Esto permite revisar, después de
    una sesión, qué pedidos se enviaron, cuándo, y qué respuestas se recibieron,
    sin depender de la terminal.

    Dos handlers simultáneos:
        1. FileHandler   → 'cliente_log.txt' (registro persistente)
        2. StreamHandler → consola (feedback en tiempo real)

PROTOCOLO:
    Petición  JSON: {"accion": "CONSULTAR"|"PEDIR", "producto": "<nombre>", "cantidad": <int>}
    Respuesta JSON: {"estado": "OK"|"ERROR"|"STOCK", "mensaje": "<texto>", "cantidad": <int>}

AUTOR:      Programación Concurrente - Modificación 3
FECHA:      2026
PYTHON:     3.8+
================================================================================
"""

# ──────────────────────────────────────────────────────────────────────────────
# IMPORTACIONES
# ──────────────────────────────────────────────────────────────────────────────

import socket    # Comunicación TCP/IP
import json      # Serialización de mensajes
import logging   # NUEVO (MOD-3): Sistema de logs dual (archivo + consola)
import os        # Para obtener la ruta absoluta del log
import sys       # Para sys.stdout en StreamHandler
import time      # Para pausas entre pedidos automáticos

# ──────────────────────────────────────────────────────────────────────────────
# CONSTANTES DE CONFIGURACIÓN
# ──────────────────────────────────────────────────────────────────────────────

HOST        = "127.0.0.1"   # Dirección del servidor al que se conecta el cliente
PORT        = 65000          # Puerto TCP del servidor
ENCODING    = "utf-8"        # Codificación de texto para mensajes JSON
BUFFER_SIZE = 4096           # Bytes máximos por lectura de socket

# ──────────────────────────────────────────────────────────────────────────────
# CONSTANTE DE LOG DEL CLIENTE (MODIFICACIÓN 3)
# ──────────────────────────────────────────────────────────────────────────────

# Archivo donde el cliente persiste su historial de pedidos y respuestas.
# Al igual que el servidor, se crea en el directorio de trabajo actual.
LOG_FILENAME_CLIENTE = "cliente_log.txt"

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN DEL LOGGER DEL CLIENTE (MODIFICACIÓN 3)
# ──────────────────────────────────────────────────────────────────────────────

def configurar_logging_cliente() -> logging.Logger:
    """
    Configura el logger del cliente con doble destino: archivo y consola.

    MODIFICACIÓN 3:
    ===============
    Idéntica filosofía al servidor:
        - FileHandler  → guarda cada pedido/respuesta en 'cliente_log.txt'
        - StreamHandler → imprime en consola para feedback inmediato

    ¿Por qué el CLIENTE también necesita log en archivo?
    ─────────────────────────────────────────────────────
    En sistemas distribuidos y concurrentes, el cliente también es parte del
    sistema y sus acciones son igual de importantes para el diagnóstico:

        1. AUDITORÍA: Saber exactamente qué pidió cada cliente y cuándo.
        2. DEBUGGING: Si hay discrepancias entre lo pedido y lo recibido,
                       el log del cliente y del servidor se comparan.
        3. REPRODUCCIÓN: Con el log se pueden reproducir exactamente los
                          pedidos de una sesión para pruebas o análisis.
        4. CONCURRENCIA: Si múltiples clientes corren en paralelo, cada uno
                          con su propio archivo de log (ej: cliente_1.txt,
                          cliente_2.txt), se puede reconstruir el historial
                          completo del sistema.

    FORMATO DEL LOG:
        '%(asctime)s [%(threadName)s] %(message)s'
        → Mismo formato que el servidor para facilitar la correlación
          cruzada de eventos entre los dos archivos de log.

    Returns:
        logging.Logger: Logger del cliente configurado con ambos handlers.
    """
    # Logger con nombre "cliente" para diferenciarlo del logger del servidor
    # si ambos se importaran en el mismo proceso (ej: tests de integración).
    logger = logging.getLogger("cliente")
    logger.setLevel(logging.INFO)

    # ── FORMATO COMPARTIDO (igual al servidor para facilitar correlación) ──────
    # Al usar el mismo formato en cliente y servidor, podemos unir ambos logs
    # cronológicamente y ver la historia completa de cada pedido:
    #   1. Cliente envía pedido  (cliente_log.txt)
    #   2. Servidor lo procesa   (servidor_log.txt)
    #   3. Cliente recibe respuesta (cliente_log.txt)
    formato = logging.Formatter(
        fmt="%(asctime)s [%(threadName)s] %(message)s"
    )

    # ── HANDLER 1: ARCHIVO DEL CLIENTE (MODIFICACIÓN 3) ──────────────────────
    # Cada cliente guarda su propio archivo. Si se ejecutan múltiples instancias
    # de cliente simultáneamente, todas escribirían en el mismo archivo
    # (lo cual es aceptable porque logging serializa las escrituras).
    # Para instancias separadas, se podría usar un nombre único por proceso:
    #   filename=f"cliente_{os.getpid()}_log.txt"
    archivo_handler = logging.FileHandler(
        filename=LOG_FILENAME_CLIENTE,
        mode="a",           # Append: conservamos el historial entre sesiones
        encoding=ENCODING,
    )
    archivo_handler.setLevel(logging.INFO)
    archivo_handler.setFormatter(formato)

    # ── HANDLER 2: CONSOLA ────────────────────────────────────────────────────
    consola_handler = logging.StreamHandler(sys.stdout)
    consola_handler.setLevel(logging.INFO)
    consola_handler.setFormatter(formato)

    logger.addHandler(archivo_handler)
    logger.addHandler(consola_handler)

    return logger


# Instancia global del logger del cliente.
logger_cliente = configurar_logging_cliente()


# ──────────────────────────────────────────────────────────────────────────────
# FUNCIONES AUXILIARES DE LOGGING DEL CLIENTE (MODIFICACIÓN 3)
# ──────────────────────────────────────────────────────────────────────────────

def log_info(mensaje: str) -> None:
    """
    Registra un mensaje INFO en el log del cliente (archivo + consola).

    MODIFICACIÓN 3:
        A diferencia del servidor, el cliente es monohilo en su flujo principal,
        por lo que el lock_log del servidor no es estrictamente necesario aquí.
        Sin embargo, si se instanciaran múltiples clientes en el mismo proceso
        (ej: pruebas de carga con threads), el logger de Python ya los serializa.

    Args:
        mensaje (str): Texto del evento a registrar.
    """
    logger_cliente.info(mensaje)


def log_error(mensaje: str) -> None:
    """
    Registra un mensaje ERROR en el log del cliente (archivo + consola).

    Los errores del cliente incluyen: conexión rechazada, respuestas de error
    del servidor, timeouts, problemas de serialización JSON.

    Args:
        mensaje (str): Descripción del error.
    """
    logger_cliente.error(mensaje)


def log_warning(mensaje: str) -> None:
    """
    Registra un mensaje WARNING en el log del cliente.

    Usado para situaciones como stock insuficiente (estado='ERROR' del servidor)
    o respuestas inesperadas.

    Args:
        mensaje (str): Descripción de la situación de alerta.
    """
    logger_cliente.warning(mensaje)


# ──────────────────────────────────────────────────────────────────────────────
# FUNCIÓN PRINCIPAL DE COMUNICACIÓN
# ──────────────────────────────────────────────────────────────────────────────

def enviar_pedido(
    sock: socket.socket,
    accion: str,
    producto: str,
    cantidad: int = 1
) -> dict:
    """
    Envía un pedido al servidor y espera la respuesta.

    MODIFICACIÓN 3:
        Tanto el pedido enviado como la respuesta recibida se registran
        en el archivo de log del cliente, con marca de tiempo y nombre del hilo.
        Esto permite reconstruir exactamente la conversación cliente↔servidor.

    PROTOCOLO:
        1. Serializa el pedido a JSON.
        2. Envía los bytes al servidor via TCP (sendall garantiza envío completo).
        3. Espera la respuesta (recv bloquea hasta recibir datos).
        4. Deserializa la respuesta JSON.
        5. Registra ambos en el log (MODIFICACIÓN 3).

    Args:
        sock     (socket.socket): Socket conectado al servidor.
        accion   (str)          : "CONSULTAR" o "PEDIR".
        producto (str)          : Nombre del producto del catálogo.
        cantidad (int)          : Cantidad a pedir (ignorada en CONSULTAR).

    Returns:
        dict: Respuesta del servidor con campos 'estado', 'mensaje', 'cantidad'.
              Devuelve dict vacío si hay error de comunicación.
    """
    # ── CONSTRUCCIÓN DEL PEDIDO ───────────────────────────────────────────────
    pedido = {
        "accion"  : accion.upper(),
        "producto": producto,
        "cantidad": cantidad,
    }

    # MODIFICACIÓN 3: Registramos el pedido ANTES de enviarlo.
    # Esto garantiza que, incluso si el envío falla, queda constancia del intento.
    log_info(f"Enviando pedido → accion={accion}, producto='{producto}', cantidad={cantidad}")

    try:
        # ── SERIALIZACIÓN Y ENVÍO ─────────────────────────────────────────────
        # json.dumps → convierte el dict Python a string JSON
        # .encode()  → convierte el string a bytes (necesario para el socket)
        # sendall()  → garantiza que TODOS los bytes se envían, incluso si
        #              el buffer del SO los divide en múltiples paquetes TCP.
        datos_pedido = json.dumps(pedido).encode(ENCODING)
        sock.sendall(datos_pedido)

        # ── RECEPCIÓN DE RESPUESTA ────────────────────────────────────────────
        # recv() bloquea hasta que llegan datos del servidor.
        # BUFFER_SIZE debe ser suficientemente grande para la respuesta completa.
        # Si la respuesta pudiera ser mayor (ej: lista de productos), se necesitaría
        # un protocolo de delimitación (longitud prefijada, newline, etc.).
        datos_respuesta = sock.recv(BUFFER_SIZE)

        if not datos_respuesta:
            # El servidor cerró la conexión sin enviar respuesta.
            log_error("El servidor cerró la conexión sin enviar respuesta.")
            return {}

        # ── DESERIALIZACIÓN DE LA RESPUESTA ───────────────────────────────────
        respuesta = json.loads(datos_respuesta.decode(ENCODING))

        # MODIFICACIÓN 3: Registramos la respuesta recibida del servidor.
        # El estado de la respuesta determina el nivel de log:
        #   OK    → INFO (todo bien)
        #   STOCK → INFO (consulta exitosa)
        #   ERROR → WARNING o ERROR (problema con el pedido)
        estado = respuesta.get("estado", "DESCONOCIDO")
        mensaje_resp = respuesta.get("mensaje", "Sin mensaje")

        if estado == "OK":
            log_info(f"Respuesta recibida ← estado={estado} | {mensaje_resp}")
        elif estado == "STOCK":
            log_info(
                f"Respuesta recibida ← estado={estado} | "
                f"'{producto}' tiene {respuesta.get('cantidad', 0)} unidades."
            )
        else:
            # estado == "ERROR" u otro valor inesperado
            log_warning(f"Respuesta recibida ← estado={estado} | {mensaje_resp}")

        return respuesta

    except json.JSONDecodeError as e:
        log_error(f"Error al deserializar respuesta JSON del servidor: {e}")
        return {}
    except (ConnectionResetError, BrokenPipeError) as e:
        log_error(f"Conexión interrumpida durante el pedido: {e}")
        return {}
    except OSError as e:
        log_error(f"Error de socket al enviar/recibir: {e}")
        return {}


# ──────────────────────────────────────────────────────────────────────────────
# MODO AUTOMÁTICO DE PRUEBA
# ──────────────────────────────────────────────────────────────────────────────

def ejecutar_pedidos_automaticos(sock: socket.socket) -> None:
    """
    Ejecuta una secuencia predefinida de pedidos para probar el servidor.

    MODIFICACIÓN 3:
        Todos los pedidos y respuestas quedan registrados en 'cliente_log.txt'.
        Al finalizar, el usuario puede abrir el archivo y revisar la sesión
        completa sin depender de la salida de terminal (que puede perderse).

    Esta función es útil para:
        - Pruebas de integración: verificar que el servidor responde correctamente.
        - Pruebas de carga: lanzar múltiples clientes con esta función en paralelo.
        - Demostración: mostrar el sistema en funcionamiento automáticamente.

    Args:
        sock (socket.socket): Socket conectado al servidor.
    """
    # Lista de pedidos de prueba: (accion, producto, cantidad)
    # Diseñada para probar casos normales, de error y consultas.
    pedidos_prueba = [
        ("CONSULTAR", "Laptop",      0),   # Consulta de stock (cantidad ignorada)
        ("PEDIR",     "Mouse",       3),   # Pedido normal con stock suficiente
        ("PEDIR",     "Laptop",      2),   # Pedido normal
        ("CONSULTAR", "Monitor",     0),   # Consulta antes de pedir
        ("PEDIR",     "Monitor",     5),   # Pedido que puede agotar stock
        ("PEDIR",     "USB",        10),   # Pedido de alta cantidad
        ("PEDIR",     "Cargador",    1),   # Pedido mínimo
        ("PEDIR",     "ProductoXYZ", 1),   # Producto inexistente → ERROR esperado
        ("PEDIR",     "Webcam",     20),   # Cantidad mayor que el stock → ERROR esperado
        ("CONSULTAR", "Mouse",       0),   # Verificar stock después de pedidos
    ]

    log_info(f"Iniciando secuencia automática: {len(pedidos_prueba)} pedidos.")
    print(f"\n{'─'*60}")
    print(f"  MODO AUTOMÁTICO: {len(pedidos_prueba)} pedidos")
    print(f"{'─'*60}")

    for i, (accion, producto, cantidad) in enumerate(pedidos_prueba, start=1):
        print(f"\n[{i}/{len(pedidos_prueba)}] {accion} {cantidad}x '{producto}'")

        respuesta = enviar_pedido(sock, accion, producto, cantidad)

        if respuesta:
            estado  = respuesta.get("estado", "?")
            mensaje = respuesta.get("mensaje", "")
            # Usamos símbolos visuales para facilitar la lectura en consola.
            icono = "✅" if estado == "OK" else ("📦" if estado == "STOCK" else "❌")
            print(f"  {icono} [{estado}] {mensaje}")
        else:
            print(f"  ⚠️  Sin respuesta del servidor.")

        # Pausa entre pedidos para no saturar el servidor y simular uso real.
        # En pruebas de carga se puede reducir o eliminar esta pausa.
        time.sleep(0.3)

    log_info(f"Secuencia automática completada: {len(pedidos_prueba)} pedidos enviados.")


# ──────────────────────────────────────────────────────────────────────────────
# MODO INTERACTIVO
# ──────────────────────────────────────────────────────────────────────────────

def ejecutar_modo_interactivo(sock: socket.socket) -> None:
    """
    Modo interactivo: el usuario elige qué pedidos enviar desde el menú.

    MODIFICACIÓN 3:
        Cada acción del usuario queda registrada en 'cliente_log.txt'.
        Esto es especialmente valioso en sesiones de demostración o
        evaluación, donde se quiere revisar exactamente qué se hizo.

    Args:
        sock (socket.socket): Socket conectado al servidor.
    """
    # Catálogo local (solo para mostrar al usuario; el servidor tiene la verdad).
    productos_disponibles = [
        "Laptop", "Mouse", "Teclado", "Monitor",
        "Auriculares", "USB", "Cargador", "Webcam"
    ]

    log_info("Iniciando modo interactivo.")

    while True:
        # ── MENÚ PRINCIPAL ────────────────────────────────────────────────────
        print(f"\n{'═'*50}")
        print("  CLIENTE DE ALMACÉN - Menú")
        print(f"{'═'*50}")
        print("  1. Consultar stock de un producto")
        print("  2. Realizar pedido de un producto")
        print("  3. Salir")
        print(f"{'─'*50}")

        opcion = input("  Seleccione una opción [1-3]: ").strip()

        if opcion == "3":
            log_info("Usuario seleccionó salir del modo interactivo.")
            print("  Cerrando cliente...")
            break

        if opcion not in ("1", "2"):
            print("  ⚠️  Opción inválida. Ingrese 1, 2 o 3.")
            continue

        # ── SELECCIÓN DE PRODUCTO ─────────────────────────────────────────────
        print(f"\n  Productos disponibles:")
        for idx, prod in enumerate(productos_disponibles, start=1):
            print(f"    {idx:2}. {prod}")

        nombre_producto = input("\n  Nombre del producto: ").strip()
        if not nombre_producto:
            print("  ⚠️  Debe ingresar un nombre de producto.")
            continue

        if opcion == "1":
            # ── CONSULTA DE STOCK ─────────────────────────────────────────────
            log_info(f"Usuario solicitó consulta de stock: '{nombre_producto}'")
            respuesta = enviar_pedido(sock, "CONSULTAR", nombre_producto, cantidad=0)

            if respuesta:
                estado  = respuesta.get("estado", "?")
                mensaje = respuesta.get("mensaje", "")
                cant    = respuesta.get("cantidad", 0)
                if estado == "STOCK":
                    print(f"\n  📦 Stock de '{nombre_producto}': {cant} unidades.")
                else:
                    print(f"\n  ❌ Error: {mensaje}")
            else:
                print(f"\n  ⚠️  Sin respuesta del servidor.")

        elif opcion == "2":
            # ── PEDIDO DE PRODUCTO ────────────────────────────────────────────
            try:
                cantidad_str = input("  Cantidad a pedir: ").strip()
                cantidad = int(cantidad_str)
                if cantidad <= 0:
                    print("  ⚠️  La cantidad debe ser un número positivo.")
                    continue
            except ValueError:
                print("  ⚠️  Cantidad inválida. Ingrese un número entero.")
                continue

            log_info(f"Usuario solicitó pedido: {cantidad}x '{nombre_producto}'")
            respuesta = enviar_pedido(sock, "PEDIR", nombre_producto, cantidad)

            if respuesta:
                estado  = respuesta.get("estado", "?")
                mensaje = respuesta.get("mensaje", "")
                if estado == "OK":
                    print(f"\n  ✅ {mensaje}")
                else:
                    print(f"\n  ❌ {mensaje}")
            else:
                print(f"\n  ⚠️  Sin respuesta del servidor.")

    log_info("Modo interactivo terminado.")


# ──────────────────────────────────────────────────────────────────────────────
# FUNCIÓN PRINCIPAL DEL CLIENTE
# ──────────────────────────────────────────────────────────────────────────────

def iniciar_cliente() -> None:
    """
    Función principal del cliente. Establece la conexión y gestiona los modos.

    MODIFICACIÓN 3:
        1. Al iniciar, registra en el log la ruta del archivo de log.
        2. Toda la sesión (conexión, pedidos, respuestas, cierre) queda en el log.
        3. Al terminar, imprime la ruta del archivo de log para que el usuario
           sepa dónde revisar el historial completo.

    FLUJO:
        1. Calcula y muestra la ruta del archivo de log.
        2. Crea un socket TCP y se conecta al servidor.
        3. Pregunta al usuario el modo: automático o interactivo.
        4. Ejecuta el modo elegido.
        5. Al terminar, cierra el socket, hace flush de los handlers y
           muestra la ruta del log.
    """

    # ── RUTA DEL ARCHIVO DE LOG (MODIFICACIÓN 3) ──────────────────────────────
    ruta_log = os.path.abspath(LOG_FILENAME_CLIENTE)

    # Separador de sesión en el archivo de log: útil cuando mode='a' acumula
    # múltiples ejecuciones en el mismo archivo.
    logger_cliente.info("=" * 70)
    logger_cliente.info("CLIENTE INICIANDO - MODIFICACIÓN 3: LOG EN ARCHIVO")
    logger_cliente.info(f"Servidor objetivo: {HOST}:{PORT}")
    logger_cliente.info(f"Archivo de log del cliente: {ruta_log}")
    logger_cliente.info("=" * 70)

    print(f"\n{'='*60}")
    print(f"  CLIENTE DE ALMACÉN - Modificación 3 (Log en Archivo)")
    print(f"  Servidor: {HOST}:{PORT}")
    print(f"  📄 Log del cliente: {ruta_log}")
    print(f"{'='*60}\n")

    # ── CREACIÓN Y CONEXIÓN DEL SOCKET ────────────────────────────────────────
    # AF_INET + SOCK_STREAM → socket TCP IPv4, igual que el servidor.
    cliente_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Timeout de conexión: si el servidor no responde en 10 segundos,
    # lanza socket.timeout en lugar de bloquear indefinidamente.
    cliente_socket.settimeout(10)

    try:
        log_info(f"Intentando conectar a {HOST}:{PORT}...")
        cliente_socket.connect((HOST, PORT))

        # Una vez conectado, removemos el timeout para las operaciones de
        # envío/recepción (el servidor puede tardar en responder si hay carga).
        # Si se quisiera mantener timeout en recv, se puede dejar o ajustar.
        cliente_socket.settimeout(30)  # 30 segundos para recibir respuesta

        log_info(f"Conexión establecida con el servidor {HOST}:{PORT}.")
        print(f"  ✅ Conectado al servidor {HOST}:{PORT}\n")

        # ── SELECCIÓN DE MODO ─────────────────────────────────────────────────
        print("  Modos disponibles:")
        print("    A - Automático (ejecuta pedidos de prueba predefinidos)")
        print("    I - Interactivo (menú para ingresar pedidos manualmente)")
        modo = input("\n  Seleccione modo [A/I]: ").strip().upper()

        if modo == "A":
            log_info("Modo seleccionado: AUTOMÁTICO")
            ejecutar_pedidos_automaticos(cliente_socket)
        elif modo == "I":
            log_info("Modo seleccionado: INTERACTIVO")
            ejecutar_modo_interactivo(cliente_socket)
        else:
            log_warning(f"Modo '{modo}' no reconocido. Ejecutando modo automático.")
            print(f"  ⚠️  Modo no reconocido. Ejecutando modo automático.")
            ejecutar_pedidos_automaticos(cliente_socket)

    except ConnectionRefusedError:
        # El servidor no está escuchando en HOST:PORT.
        log_error(
            f"Conexión rechazada en {HOST}:{PORT}. "
            f"¿Está el servidor en ejecución?"
        )
        print(f"\n  ❌ Error: No se pudo conectar al servidor.")
        print(f"     Verifique que servidor.py esté ejecutándose en {HOST}:{PORT}.")

    except socket.timeout:
        log_error(f"Timeout al intentar conectar a {HOST}:{PORT}.")
        print(f"\n  ❌ Error: Tiempo de espera agotado al conectar.")

    except KeyboardInterrupt:
        log_info("Cliente interrumpido por el usuario (Ctrl+C).")
        print("\n\n  Cliente interrumpido.")

    except OSError as e:
        log_error(f"Error de socket: {e}")
        print(f"\n  ❌ Error de socket: {e}")

    finally:
        # ── CIERRE ORDENADO ───────────────────────────────────────────────────
        # El bloque finally garantiza que siempre se cierra el socket y
        # se vuelcan los logs al disco.

        try:
            cliente_socket.close()
            log_info("Socket del cliente cerrado correctamente.")
        except OSError:
            pass  # El socket ya podría estar cerrado

        # ── RESUMEN FINAL (MODIFICACIÓN 3) ────────────────────────────────────
        logger_cliente.info("=" * 70)
        logger_cliente.info("CLIENTE TERMINANDO")
        logger_cliente.info(f"Archivo de log guardado en: {ruta_log}")
        logger_cliente.info("=" * 70)

        # Forzamos el vaciado de todos los handlers para garantizar que
        # las últimas líneas de log se escribieron al disco antes de salir.
        # logging.shutdown() cierra todos los handlers globalmente.
        logging.shutdown()

        # ── MENSAJE FINAL AL USUARIO (MODIFICACIÓN 3) ─────────────────────────
        # Igual que en el servidor, informamos la ruta del archivo de log.
        print(f"\n{'='*60}")
        print(f"  CLIENTE DESCONECTADO")
        print(f"  📄 Historial de pedidos guardado en:")
        print(f"     {ruta_log}")
        print(f"{'='*60}\n")


# ──────────────────────────────────────────────────────────────────────────────
# PUNTO DE ENTRADA
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    """
    Punto de entrada del cliente. Solo se ejecuta al correr directamente.

    USO:
        python cliente.py

    MODIFICACIÓN 3:
        Antes de conectarse al servidor, ya está configurado el logging
        dual (archivo + consola). El primer mensaje de log se emite al
        inicio de iniciar_cliente(), marcando el comienzo de la sesión.
    """
    iniciar_cliente()
