"""
================================================================================
CLIENTE - CENTRAL DE PEDIDOS (PRODUCTOR REMOTO)
================================================================================

PROPÓSITO GENERAL DEL ARCHIVO:
- Implementar un CLIENTE TCP que se conecta al servidor de la central de pedidos.
- El cliente actúa como PRODUCTOR: genera pedidos y los envía al servidor.
- Cada ejecución de este archivo es un cliente INDEPENDIENTE.
  Para simular múltiples clientes, se ejecuta este archivo en varias terminales.
- El cliente:
    1. Se conecta al servidor vía socket TCP.
    2. Recibe la lista de productos disponibles.
    3. Genera un número ALEATORIO de pedidos (entre 1 y 5).
    4. Cada pedido tiene un producto aleatorio y una cantidad aleatoria.
    5. Envía los pedidos al servidor y recibe confirmaciones.
    6. Envía una señal de FIN y se desconecta.

EJECUCIÓN:
    python cliente.py

    Para simular múltiples clientes, abrir varias terminales y ejecutar
    este comando en cada una. Cada ejecución es un cliente diferente.

PROTOCOLO DE COMUNICACIÓN (JSON sobre TCP):
    Cliente → Servidor (pedido):
        {"tipo": "pedido", "producto": "Laptop", "cantidad": 2}

    Cliente → Servidor (fin):
        {"tipo": "fin"}

    Servidor → Cliente (bienvenida):
        {"tipo": "bienvenida", "mensaje": "...", "productos_disponibles": [...], "tu_id": "..."}

    Servidor → Cliente (confirmación):
        {"tipo": "confirmacion", "mensaje": "...", "estado": "en_cola"}

    Servidor → Cliente (error):
        {"tipo": "error", "mensaje": "...", "estado": "rechazado"}

    Servidor → Cliente (fin confirmado):
        {"tipo": "fin_confirmado", "mensaje": "..."}
================================================================================
"""

# ==============================================================================
# IMPORTACIONES
# ==============================================================================

# import socket
# - Módulo estándar de Python para comunicación de red.
# - En el CLIENTE usaremos:
#     * socket.socket(AF_INET, SOCK_STREAM) → crea un socket TCP IPv4.
#     * .connect((host, port)) → conecta el socket al servidor en esa dirección.
#       A diferencia del servidor (que usa bind + listen + accept), el cliente
#       solo necesita connect().
#     * .sendall(bytes) → envía datos al servidor.
#     * .recv(n) → recibe datos del servidor.
#     * .close() → cierra la conexión.
import socket

# import json
# - Módulo para serializar/deserializar datos JSON.
# - El cliente y el servidor se comunican enviando diccionarios Python
#   convertidos a strings JSON (json.dumps) y viceversa (json.loads).
# - Ejemplo de flujo:
#     Python dict → json.dumps() → string → .encode() → bytes → socket.sendall()
#     socket.recv() → bytes → .decode() → string → json.loads() → Python dict
import json

# import random
# - Módulo para generar números aleatorios.
# - Usaremos random.randint(a, b) para:
#     * Decidir CUÁNTOS pedidos enviar (entre 1 y 5).
#     * Elegir QUÉ producto pedir (índice aleatorio de la lista).
#     * Decidir CUÁNTA cantidad pedir de cada producto (entre 1 y 3).
import random

# import time
# - Módulo para funciones de tiempo.
# - Usaremos time.sleep() para pausar entre pedidos, simulando que el
#   cliente "piensa" antes de hacer el siguiente pedido.
# - También time.strftime() para mostrar marcas de tiempo en los logs.
import time

# ==============================================================================
# CONSTANTES
# ==============================================================================

# HOST = "127.0.0.1"
# - Dirección IP del SERVIDOR al que nos queremos conectar.
# - "127.0.0.1" (localhost) = el servidor está en la misma máquina.
# - Si el servidor estuviera en otra máquina de la red, pondríamos su IP
#   (ej: "192.168.1.100").
HOST = "127.0.0.1"

# PORT = 65000
# - Puerto del SERVIDOR al que nos conectamos.
# - DEBE coincidir con el puerto donde el servidor está escuchando.
# - Si el servidor usa el puerto 65000, el cliente también debe usar 65000.
PORT = 65000

# ENCODING = "utf-8"
# - Codificación para convertir strings ↔ bytes.
# - DEBE ser la misma que usa el servidor, sino los mensajes se corrompen.
ENCODING = "utf-8"

# BUFFER_SIZE = 4096
# - Tamaño máximo del buffer de recepción.
# - DEBE ser suficiente para recibir los mensajes del servidor.
BUFFER_SIZE = 4096


# ==============================================================================
# FUNCIONES DEL CLIENTE
# ==============================================================================


def log(mensaje):
    """
    Imprime un mensaje con marca de tiempo en consola.

    Parámetros:
        mensaje (str): El texto a imprimir.

    Retorna:
        None (solo imprime).

    ¿Por qué?
        - Ayuda a ver el orden cronológico de los eventos.
        - time.strftime("%H:%M:%S") retorna la hora actual formateada.
    """
    hora_actual = time.strftime("%H:%M:%S")
    print(f"[{hora_actual}] [CLIENTE] {mensaje}")


def conectar_al_servidor():
    """
    Crea un socket TCP y lo conecta al servidor.

    No recibe parámetros (usa las constantes globales HOST y PORT).

    Retorna:
        socket.socket: El socket conectado al servidor, listo para enviar
                       y recibir datos.

    Excepciones:
        - ConnectionRefusedError: El servidor no está corriendo o rechazó la conexión.
        - OSError: Error de red (IP inválida, puerto inválido, etc.).

    Proceso:
        1. Crea un socket TCP/IP con socket.socket(AF_INET, SOCK_STREAM).
        2. Conecta el socket al servidor con .connect((HOST, PORT)).
        3. Retorna el socket conectado.
    """
    # Crear el socket TCP.
    # socket.AF_INET: dirección IPv4.
    # socket.SOCK_STREAM: tipo TCP (conexión confiable y ordenada).
    cliente_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Conectar al servidor.
    # .connect() intenta establecer una conexión TCP con el servidor.
    # Si el servidor no está corriendo, lanza ConnectionRefusedError.
    # Si la conexión se establece, el socket queda listo para comunicarse.
    log(f"Conectando al servidor en {HOST}:{PORT}...")
    cliente_socket.connect((HOST, PORT))
    log("¡Conectado exitosamente!")

    return cliente_socket


def recibir_mensaje(cliente_socket):
    """
    Recibe un mensaje del servidor y lo decodifica de bytes a dict Python.

    Parámetros:
        cliente_socket (socket.socket): El socket conectado al servidor.

    Retorna:
        dict: El mensaje del servidor como diccionario Python.
              Retorna None si la conexión se cerró.

    Proceso:
        1. Llama a .recv(BUFFER_SIZE) para recibir bytes del servidor.
        2. Si recibe bytes vacíos → el servidor cerró la conexión → retorna None.
        3. Decodifica los bytes a string con .decode(ENCODING).
        4. Parsea el string JSON a diccionario Python con json.loads().
        5. Retorna el diccionario.
    """
    # recv() es BLOQUEANTE: el programa se pausa aquí hasta que el servidor
    # envíe algo o cierre la conexión.
    datos = cliente_socket.recv(BUFFER_SIZE)

    # Si recibimos bytes vacíos, la conexión fue cerrada por el servidor.
    if not datos:
        return None

    # Decodificar bytes → string → diccionario.
    mensaje_texto = datos.decode(ENCODING)    # bytes → string
    mensaje_dict = json.loads(mensaje_texto)   # string JSON → dict Python
    return mensaje_dict


def enviar_pedido(cliente_socket, producto, cantidad):
    """
    Envía un pedido al servidor en formato JSON.

    Parámetros:
        cliente_socket (socket.socket): El socket conectado al servidor.
        producto (str): Nombre del producto a pedir (ej: "Laptop").
        cantidad (int): Cantidad a pedir (ej: 2).

    Retorna:
        dict: La respuesta del servidor (confirmación o error).
              Retorna None si la conexión se cerró.

    Proceso:
        1. Construye un diccionario con los datos del pedido.
        2. Lo convierte a JSON → bytes.
        3. Lo envía al servidor con sendall().
        4. Espera y retorna la respuesta del servidor.
    """
    # Construir el diccionario del pedido.
    pedido = {
        "tipo": "pedido",       # Tipo de mensaje para que el servidor lo identifique.
        "producto": producto,   # Nombre del producto.
        "cantidad": cantidad    # Cantidad solicitada.
    }

    # Convertir el pedido a string JSON y luego a bytes.
    # json.dumps(pedido) → '{"tipo": "pedido", "producto": "Laptop", "cantidad": 2}'
    # .encode(ENCODING) → b'{"tipo": "pedido", "producto": "Laptop", "cantidad": 2}'
    mensaje_bytes = json.dumps(pedido).encode(ENCODING)

    # Enviar el pedido al servidor.
    # sendall() garantiza que TODOS los bytes se envíen (reintenta si es necesario).
    cliente_socket.sendall(mensaje_bytes)

    log(f"Pedido enviado: {cantidad}x {producto}")

    # Esperar la respuesta del servidor.
    respuesta = recibir_mensaje(cliente_socket)
    return respuesta


def enviar_fin(cliente_socket):
    """
    Envía la señal de FIN al servidor, indicando que no habrá más pedidos.

    Parámetros:
        cliente_socket (socket.socket): El socket conectado al servidor.

    Retorna:
        dict: La respuesta de confirmación del servidor.
              Retorna None si la conexión se cerró.

    ¿Por qué enviar FIN?
        - El servidor necesita saber cuándo un cliente terminó de enviar pedidos.
        - Sin esta señal, el servidor seguiría esperando más datos indefinidamente.
        - Es parte del PROTOCOLO de comunicación que definimos.
    """
    # Construir el mensaje de fin.
    mensaje_fin = {"tipo": "fin"}

    # Convertir a bytes y enviar.
    cliente_socket.sendall(json.dumps(mensaje_fin).encode(ENCODING))
    log("Señal de FIN enviada al servidor.")

    # Esperar confirmación del servidor.
    respuesta = recibir_mensaje(cliente_socket)
    return respuesta


def ejecutar_cliente():
    """
    Función principal que ejecuta toda la lógica del cliente.

    No recibe parámetros.

    Retorna:
        None (ejecuta el flujo completo del cliente y termina).

    Flujo:
        1. Se conecta al servidor.
        2. Recibe el mensaje de bienvenida (con lista de productos).
        3. Genera un número aleatorio de pedidos (1 a 5).
        4. Por cada pedido:
           a. Elige un producto aleatorio de la lista.
           b. Elige una cantidad aleatoria (1 a 3).
           c. Envía el pedido al servidor.
           d. Muestra la respuesta del servidor.
           e. Espera un tiempo aleatorio antes del siguiente pedido.
        5. Envía la señal de FIN.
        6. Cierra la conexión.
    """
    # Variable para el socket (declarada aquí para poder cerrarla en finally).
    cliente_socket = None

    try:
        # --- PASO 1: Conectar al servidor ---
        cliente_socket = conectar_al_servidor()

        # --- PASO 2: Recibir mensaje de bienvenida ---
        bienvenida = recibir_mensaje(cliente_socket)

        # Verificar que recibimos la bienvenida correctamente.
        if bienvenida is None:
            log("Error: No se recibió respuesta del servidor.")
            return

        # Mostrar la bienvenida.
        log(f"Servidor dice: {bienvenida['mensaje']}")

        # Extraer la lista de productos disponibles.
        productos = bienvenida["productos_disponibles"]
        mi_id = bienvenida["tu_id"]

        log(f"Mi ID asignado: {mi_id}")
        log(f"Productos disponibles: {', '.join(productos)}")
        # ', '.join(productos) une la lista con comas:
        # ["Laptop", "Mouse", "Teclado"] → "Laptop, Mouse, Teclado"

        # --- PASO 3: Generar número aleatorio de pedidos ---
        # random.randint(1, 5): entre 1 y 5 pedidos (inclusive).
        num_pedidos = random.randint(1, 5)
        log(f"Voy a realizar {num_pedidos} pedido(s).")

        print("-" * 50)

        # --- PASO 4: Enviar pedidos ---
        for i in range(1, num_pedidos + 1):
            # Elegir un producto ALEATORIO de la lista.
            # random.choice(lista) retorna un elemento aleatorio de la lista.
            producto_elegido = random.choice(productos)

            # Elegir una cantidad ALEATORIA entre 1 y 3.
            cantidad_elegida = random.randint(1, 3)

            log(f"--- Pedido {i}/{num_pedidos} ---")

            # Enviar el pedido y recibir la respuesta.
            respuesta = enviar_pedido(cliente_socket, producto_elegido, cantidad_elegida)

            # Mostrar la respuesta del servidor.
            if respuesta:
                # Determinar si fue exitoso o no por el tipo de respuesta.
                if respuesta["tipo"] == "confirmacion":
                    log(f"✓ Servidor confirmó: {respuesta['mensaje']}")
                elif respuesta["tipo"] == "error":
                    log(f"✗ Servidor rechazó: {respuesta['mensaje']}")
                else:
                    log(f"Servidor respondió: {respuesta['mensaje']}")
            else:
                log("⚠ No se recibió respuesta del servidor.")

            # Esperar un tiempo aleatorio antes del siguiente pedido.
            # Esto simula que el cliente "piensa" o "navega" entre pedidos.
            if i < num_pedidos:  # No esperar después del último pedido.
                pausa = random.uniform(0.5, 2.0)
                # random.uniform(a, b) retorna un float aleatorio entre a y b.
                log(f"Esperando {pausa:.1f}s antes del siguiente pedido...")
                # :.1f formatea el float con 1 decimal.
                time.sleep(pausa)

        print("-" * 50)

        # --- PASO 5: Enviar señal de FIN ---
        log("Todos los pedidos enviados. Cerrando sesión...")
        respuesta_fin = enviar_fin(cliente_socket)

        if respuesta_fin:
            log(f"Servidor confirma cierre: {respuesta_fin['mensaje']}")

    except ConnectionRefusedError:
        # El servidor no está corriendo o rechazó la conexión.
        # Esto pasa si el servidor no se ha iniciado aún.
        log("ERROR: No se pudo conectar al servidor. ¿Está el servidor corriendo?")
        log(f"Asegúrate de que el servidor esté escuchando en {HOST}:{PORT}")

    except ConnectionResetError:
        # El servidor cerró la conexión inesperadamente.
        log("ERROR: El servidor cerró la conexión inesperadamente.")

    except Exception as e:
        # Cualquier otro error inesperado.
        log(f"ERROR inesperado: {e}")

    finally:
        # SIEMPRE cerrar el socket al terminar, sin importar qué pasó.
        if cliente_socket:
            cliente_socket.close()
            log("Conexión cerrada. ¡Hasta luego!")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

# if __name__ == "__main__":
# - Verifica que este archivo se está ejecutando directamente (no importado).
# - Si ejecutamos "python cliente.py", __name__ será "__main__".
# - Si otro archivo hace "import cliente", __name__ será "cliente" y esta
#   condición será False (no se ejecuta el cliente automáticamente).
if __name__ == "__main__":
    ejecutar_cliente()
