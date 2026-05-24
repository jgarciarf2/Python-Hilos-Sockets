"""
================================================================================
CLIENTE - CENTRAL DE PEDIDOS (PRODUCTOR REMOTO)
MODIFICACIÓN 5: ESTADÍSTICAS DEL CLIENTE AL FINALIZAR
================================================================================

PROPÓSITO GENERAL DEL ARCHIVO:
- Implementar un CLIENTE TCP que se conecta al servidor de la central de pedidos.
- El cliente actúa como PRODUCTOR: genera pedidos y los envía al servidor.
- Cada ejecución de este archivo es un cliente INDEPENDIENTE.
  Para simular múltiples clientes, ejecutar este archivo en varias terminales.

QUÉ AGREGA ESTA MODIFICACIÓN (MOD-05) RESPECTO AL ORIGINAL:
    ─────────────────────────────────────────────────────────────────────────
    PROBLEMA QUE RESUELVE:
        El cliente original envía pedidos y muestra respuestas en pantalla,
        pero al finalizar no sabe cuántos pedidos fueron aceptados en cola
        (confirmados) y cuántos fueron rechazados (cola llena). Esta
        información es útil para que el operador del cliente sepa si debe
        reintentar pedidos o ajustar su frecuencia de envío.

    SOLUCIÓN IMPLEMENTADA:
        En el cliente, las estadísticas son simples (no necesitan threading
        porque el cliente es de un solo hilo). Se mantienen como variables
        locales en ejecutar_cliente():
            - pedidos_enviados    → cuántos pedidos intentó enviar.
            - pedidos_exitosos    → cuántos fueron confirmados por el servidor.
            - pedidos_rechazados  → cuántos fueron rechazados (cola llena).
            - tiempo_inicio       → datetime.datetime al inicio de la sesión.
            - tiempo_fin          → datetime.datetime al finalizar.
        Al terminar, mostrar_estadisticas_cliente() imprime el resumen.

    ¿POR QUÉ NO USAR THREADING EN EL CLIENTE?
        El cliente es SECUENCIAL por diseño: envía un pedido, espera la
        respuesta, envía el siguiente. No hay concurrencia en el cliente,
        por lo que no se necesitan locks ni variables globales para las stats.
        Las variables locales dentro de ejecutar_cliente() son suficientes.
    ─────────────────────────────────────────────────────────────────────────

FLUJO DEL CLIENTE:
    1. Se conecta al servidor.
    2. Recibe el mensaje de bienvenida (con lista de productos).
    3. Registra tiempo_inicio.
    4. Genera un número aleatorio de pedidos (1 a 5).
    5. Por cada pedido:
       a. Elige un producto aleatorio de la lista.
       b. Elige una cantidad aleatoria (1 a 3).
       c. Envía el pedido al servidor.
       d. Recibe la respuesta y actualiza contadores locales.
       e. Espera un tiempo aleatorio antes del siguiente pedido.
    6. Envía la señal de FIN.
    7. Registra tiempo_fin.
    8. Imprime el resumen estadístico del cliente.
    9. Cierra la conexión.

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

    Servidor → Cliente (error/rechazo):
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
#     * .connect((host, port)) → conecta el socket al servidor.
#       A diferencia del servidor (bind + listen + accept), el cliente
#       solo necesita connect().
#     * .sendall(bytes) → envía datos al servidor.
#     * .recv(n)        → recibe datos del servidor.
#     * .close()        → cierra la conexión.
import socket

# import json
# - Módulo para serializar/deserializar datos JSON.
# - Flujo de envío:    dict → json.dumps() → str → .encode() → bytes → sendall()
# - Flujo de recepción: recv() → bytes → .decode() → str → json.loads() → dict
import json

# import random
# - Módulo para generar números aleatorios.
# - Usaremos:
#     * random.randint(a, b) → entero aleatorio entre a y b (inclusive).
#     * random.choice(lista) → elemento aleatorio de una lista.
#     * random.uniform(a, b) → float aleatorio entre a y b.
import random

# import time
# - Módulo para funciones de tiempo.
# - Usaremos:
#     * time.sleep(segundos) → pausa el hilo actual.
#     * time.strftime(formato) → hora actual formateada como string.
import time

# ─────────────────────────────────────────────────────────────────────────────
# MOD-05: import datetime
# ─────────────────────────────────────────────────────────────────────────────
# ¿POR QUÉ se importa datetime en el cliente?
#   Al igual que en el servidor, queremos registrar cuándo comenzó y cuándo
#   terminó la sesión del cliente para:
#     1. Mostrar las marcas de tiempo en el resumen estadístico.
#     2. Calcular la duración total de la sesión del cliente.
#   datetime.datetime.now() captura el instante actual como objeto,
#   y la resta (fin - inicio) produce un timedelta con la duración exacta.
# ─────────────────────────────────────────────────────────────────────────────
import datetime


# ==============================================================================
# CONSTANTES
# ==============================================================================

# HOST = "127.0.0.1"
# - Dirección IP del SERVIDOR al que nos conectamos.
# - "127.0.0.1" (localhost) = el servidor está en la misma máquina.
# - Cambiar a la IP del servidor si está en otra máquina de la red.
HOST = "127.0.0.1"

# PORT = 65000
# - Puerto del SERVIDOR. DEBE coincidir con el que usa el servidor.
PORT = 65000

# ENCODING = "utf-8"
# - Codificación para convertir strings ↔ bytes.
# - DEBE ser la misma que usa el servidor.
ENCODING = "utf-8"

# BUFFER_SIZE = 4096
# - Tamaño máximo del buffer de recepción (4 KB).
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
        Ayuda a ver el orden cronológico de los eventos del cliente.
        time.strftime("%H:%M:%S") retorna la hora actual formateada HH:MM:SS.
    """
    hora_actual = time.strftime("%H:%M:%S")
    print(f"[{hora_actual}] [CLIENTE] {mensaje}")


def conectar_al_servidor():
    """
    Crea un socket TCP y lo conecta al servidor.

    No recibe parámetros (usa las constantes globales HOST y PORT).

    Retorna:
        socket.socket: El socket conectado al servidor, listo para comunicarse.

    Excepciones que puede lanzar:
        - ConnectionRefusedError: El servidor no está corriendo.
        - OSError: Error de red (IP inválida, puerto inválido, etc.).

    Proceso:
        1. Crea socket TCP con socket.socket(AF_INET, SOCK_STREAM).
        2. Conecta al servidor con .connect((HOST, PORT)).
        3. Retorna el socket conectado.
    """
    # socket.AF_INET: dirección IPv4.
    # socket.SOCK_STREAM: tipo TCP (conexión confiable y ordenada).
    cliente_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # .connect() establece la conexión TCP con el servidor.
    # Si el servidor no está corriendo, lanza ConnectionRefusedError.
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
        1. recv(BUFFER_SIZE) recibe hasta BUFFER_SIZE bytes (BLOQUEANTE).
        2. Si bytes vacíos → conexión cerrada → retorna None.
        3. Decodifica: bytes → str → dict.
    """
    # recv() es BLOQUEANTE: el programa espera aquí hasta que lleguen datos.
    datos = cliente_socket.recv(BUFFER_SIZE)

    if not datos:
        # Bytes vacíos = el servidor cerró la conexión.
        return None

    # Decodificar: bytes → string → diccionario Python.
    mensaje_texto = datos.decode(ENCODING)      # bytes → str
    mensaje_dict = json.loads(mensaje_texto)    # str JSON → dict
    return mensaje_dict


def enviar_pedido(cliente_socket, producto, cantidad):
    """
    Envía un pedido al servidor en formato JSON y espera la respuesta.

    Parámetros:
        cliente_socket (socket.socket): El socket conectado al servidor.
        producto (str): Nombre del producto a pedir (ej: "Laptop").
        cantidad (int): Cantidad a pedir (ej: 2).

    Retorna:
        dict: La respuesta del servidor (confirmación o error).
              Retorna None si la conexión se cerró.

    Proceso:
        1. Construye el diccionario del pedido.
        2. Convierte a JSON → bytes y lo envía.
        3. Espera y retorna la respuesta del servidor.
    """
    # Construir el diccionario del pedido.
    pedido = {
        "tipo": "pedido",       # Identifica el tipo de mensaje.
        "producto": producto,   # Nombre del producto solicitado.
        "cantidad": cantidad    # Cantidad deseada.
    }

    # Convertir el pedido a bytes y enviar.
    # json.dumps()  → dict a str JSON.
    # .encode()     → str a bytes.
    # sendall()     → garantiza el envío completo de todos los bytes.
    mensaje_bytes = json.dumps(pedido).encode(ENCODING)
    cliente_socket.sendall(mensaje_bytes)

    log(f"Pedido enviado: {cantidad}x {producto}")

    # Esperar la respuesta del servidor (confirmación o rechazo).
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
        El servidor necesita saber cuándo un cliente terminó de enviar pedidos.
        Sin esta señal, el servidor seguiría bloqueado en recv() esperando más
        datos. Es parte del PROTOCOLO de comunicación definido.
    """
    mensaje_fin = {"tipo": "fin"}
    cliente_socket.sendall(json.dumps(mensaje_fin).encode(ENCODING))
    log("Señal de FIN enviada al servidor.")

    respuesta = recibir_mensaje(cliente_socket)
    return respuesta


# ──────────────────────────────────────────────────────────────────────────────
# MOD-05: FUNCIÓN mostrar_estadisticas_cliente()
# ──────────────────────────────────────────────────────────────────────────────
def mostrar_estadisticas_cliente(pedidos_enviados, pedidos_exitosos,
                                  pedidos_rechazados, tiempo_inicio, tiempo_fin,
                                  mi_id, desglose_pedidos):
    """
    Imprime un resumen formateado de las estadísticas de la sesión del cliente.

    PROPÓSITO:
        Al terminar su sesión, el cliente muestra un resumen que le permite al
        operador saber:
            1. Cuántos pedidos intentó enviar.
            2. Cuántos fueron aceptados por el servidor (en cola).
            3. Cuántos fueron rechazados (cola llena en el servidor).
            4. El detalle de cada pedido y su resultado.
            5. La duración de su sesión.

    ¿POR QUÉ COMO FUNCIÓN SEPARADA Y NO INLINE?
        Separar la presentación de la lógica (principio de separación de
        responsabilidades). Si en el futuro queremos cambiar el formato del
        reporte (ej: guardarlo en un archivo CSV), solo modificamos esta
        función sin tocar el flujo principal del cliente.

    Parámetros:
        pedidos_enviados (int):
            Total de pedidos que el cliente intentó enviar.

        pedidos_exitosos (int):
            Pedidos que el servidor confirmó como "en_cola" (aceptados).
            Nota: "aceptado en cola" ≠ "despachado". El servidor aún puede
            rechazarlo por falta de stock en el procesamiento posterior.

        pedidos_rechazados (int):
            Pedidos que el servidor rechazó porque la cola estaba llena.
            El cliente recibió un mensaje de tipo "error" con estado "rechazado".

        tiempo_inicio (datetime.datetime):
            Instante en que el cliente comenzó a enviar pedidos.

        tiempo_fin (datetime.datetime):
            Instante en que el cliente envió el último pedido.

        mi_id (str):
            El identificador asignado por el servidor (ej: "Cliente-3").

        desglose_pedidos (list[dict]):
            Lista de dicts con el detalle de cada pedido enviado.
            Cada dict tiene:
                {
                    "numero": int,     # Número de pedido (1, 2, 3...)
                    "producto": str,   # Nombre del producto
                    "cantidad": int,   # Cantidad solicitada
                    "resultado": str   # "✓ En cola" | "✗ Rechazado" | "⚠ Sin respuesta"
                }

    Retorna:
        None (solo imprime en consola).

    CÁLCULO DE DURACIÓN:
        Si tiempo_inicio y tiempo_fin no son None:
            duracion = tiempo_fin - tiempo_inicio  → timedelta
            str(timedelta) → "H:MM:SS.ffffff"
        Si alguno es None (sesión interrumpida), se muestra "N/D".

    TASA DE ÉXITO:
        porcentaje_exito = (pedidos_exitosos / pedidos_enviados) * 100
        Si pedidos_enviados == 0, se evita la división por cero mostrando "N/A".
    """
    # ── Calcular duración ──────────────────────────────────────────────────────
    if tiempo_inicio is not None and tiempo_fin is not None:
        duracion = tiempo_fin - tiempo_inicio
        duracion_str = str(duracion)
        inicio_str = tiempo_inicio.strftime("%H:%M:%S")
        fin_str = tiempo_fin.strftime("%H:%M:%S")
    else:
        duracion_str = "N/D"
        inicio_str = "N/D"
        fin_str = "N/D"

    # ── Calcular tasa de éxito ─────────────────────────────────────────────────
    if pedidos_enviados > 0:
        # La tasa de éxito indica qué porcentaje de pedidos fue aceptado en cola.
        tasa_exito = (pedidos_exitosos / pedidos_enviados) * 100
        tasa_str = f"{tasa_exito:.1f}%"  # :.1f → un decimal (ej: "66.7%")
    else:
        tasa_str = "N/A"  # No se enviaron pedidos, no hay tasa.

    # ── Imprimir el reporte ───────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"   📊  ESTADÍSTICAS DEL CLIENTE  📊")
    print(f"   Sesión: {mi_id}")
    print("=" * 60)

    # ── Sección: Tiempo ────────────────────────────────────────────────────────
    print("\n  ⏱  TIEMPO DE SESIÓN:")
    print(f"     Inicio   : {inicio_str}")
    print(f"     Fin      : {fin_str}")
    print(f"     Duración : {duracion_str}")

    # ── Sección: Resumen de pedidos ───────────────────────────────────────────
    print("\n  📦  RESUMEN DE PEDIDOS:")
    print(f"     Total enviados   : {pedidos_enviados}")
    print(f"     Aceptados (cola) : {pedidos_exitosos}  ✓")
    print(f"     Rechazados       : {pedidos_rechazados}  ✗")
    print(f"     Tasa de éxito    : {tasa_str}")

    # ── Sección: Detalle de cada pedido ───────────────────────────────────────
    if desglose_pedidos:
        print("\n  🔍  DETALLE POR PEDIDO:")
        print(f"     {'#':<4} {'Producto':<15} {'Cant':>5}   {'Resultado'}")
        print(f"     {'─'*4} {'─'*15} {'─'*5}   {'─'*20}")
        for item in desglose_pedidos:
            print(f"     {item['numero']:<4} "
                  f"{item['producto']:<15} "
                  f"{item['cantidad']:>5}   "
                  f"{item['resultado']}")

    print("\n" + "=" * 60 + "\n")


def ejecutar_cliente():
    """
    Función principal que ejecuta toda la lógica del cliente.

    No recibe parámetros.

    Retorna:
        None (ejecuta el flujo completo del cliente y termina).

    MOD-05 — CAMBIOS EN ESTA FUNCIÓN:
    ─────────────────────────────────────────────────────────────────────────
    Se agregan variables locales para rastrear estadísticas:

        pedidos_enviados (int):
            Se incrementa en 1 por cada pedido enviado al servidor,
            independientemente del resultado.

        pedidos_exitosos (int):
            Se incrementa cuando el servidor responde con tipo="confirmacion"
            y estado="en_cola". Significa que el pedido fue aceptado en la
            cola de procesamiento del servidor.

        pedidos_rechazados (int):
            Se incrementa cuando el servidor responde con tipo="error"
            (la cola del servidor estaba llena cuando se intentó agregar
            el pedido).

        tiempo_inicio (datetime.datetime):
            Se registra con datetime.datetime.now() justo ANTES del primer
            pedido (después de recibir la bienvenida del servidor).

        tiempo_fin (datetime.datetime):
            Se registra con datetime.datetime.now() justo DESPUÉS del último
            pedido (antes de enviar la señal de FIN).

        desglose_pedidos (list[dict]):
            Lista que acumula el detalle de cada pedido para el reporte.

    ¿POR QUÉ VARIABLES LOCALES Y NO GLOBALES?
        El cliente es de un solo hilo, no hay concurrencia. Las variables
        locales son más limpias: cada llamada a ejecutar_cliente() tiene su
        propio conjunto de estadísticas. Si alguien ejecuta el cliente dos
        veces (en el mismo proceso), las estadísticas no se mezclan.
    ─────────────────────────────────────────────────────────────────────────

    Flujo:
        1. Se conecta al servidor.
        2. Recibe el mensaje de bienvenida.
        3. Genera un número aleatorio de pedidos (1 a 5).
        4. Por cada pedido:
           a. Elige un producto y cantidad aleatorios.
           b. Envía el pedido y recibe la respuesta.
           c. Actualiza contadores locales de estadísticas.
           d. Agrega el detalle al desglose.
           e. Espera un tiempo aleatorio antes del siguiente.
        5. Envía la señal de FIN.
        6. Llama a mostrar_estadisticas_cliente() con los datos recopilados.
        7. Cierra la conexión.
    """
    # Variable para el socket (declarada aquí para poder cerrarla en finally).
    cliente_socket = None

    # ── MOD-05: Variables de estadísticas ─────────────────────────────────────
    # Se inicializan aquí para que sean accesibles en el bloque finally.
    pedidos_enviados = 0    # Total de pedidos intentados.
    pedidos_exitosos = 0    # Aceptados en cola por el servidor.
    pedidos_rechazados = 0  # Rechazados por cola llena en el servidor.
    tiempo_inicio = None    # Se asignará antes del primer pedido.
    tiempo_fin = None       # Se asignará después del último pedido.
    mi_id = "Desconocido"   # Se actualizará con el ID asignado por el servidor.
    desglose_pedidos = []   # Lista de dicts con el detalle de cada pedido.
    # ──────────────────────────────────────────────────────────────────────────

    try:
        # ── PASO 1: Conectar al servidor ───────────────────────────────────────
        cliente_socket = conectar_al_servidor()

        # ── PASO 2: Recibir mensaje de bienvenida ──────────────────────────────
        bienvenida = recibir_mensaje(cliente_socket)

        if bienvenida is None:
            log("Error: No se recibió respuesta del servidor.")
            return

        log(f"Servidor dice: {bienvenida['mensaje']}")

        # Extraer la lista de productos y el ID asignado.
        productos = bienvenida["productos_disponibles"]
        mi_id = bienvenida["tu_id"]  # ← Se actualiza para el reporte.

        log(f"Mi ID asignado: {mi_id}")
        log(f"Productos disponibles: {', '.join(productos)}")
        # ', '.join(lista) une elementos con comas: ["A","B"] → "A, B"

        # ── PASO 3: Generar número aleatorio de pedidos ────────────────────────
        num_pedidos = random.randint(1, 5)
        log(f"Voy a realizar {num_pedidos} pedido(s).")

        # ── MOD-05: Registrar tiempo de inicio ────────────────────────────────
        # Se registra AQUÍ (después de recibir la bienvenida, antes del primer
        # pedido) para medir solo el tiempo activo de envío de pedidos,
        # excluyendo el tiempo de conexión y handshake inicial.
        tiempo_inicio = datetime.datetime.now()
        # ──────────────────────────────────────────────────────────────────────

        print("-" * 50)

        # ── PASO 4: Enviar pedidos ─────────────────────────────────────────────
        for i in range(1, num_pedidos + 1):
            # Elegir un producto aleatorio de la lista recibida del servidor.
            # random.choice() retorna un elemento aleatorio de la lista.
            producto_elegido = random.choice(productos)

            # Elegir cantidad aleatoria (1 a 3 unidades).
            cantidad_elegida = random.randint(1, 3)

            log(f"--- Pedido {i}/{num_pedidos} ---")

            # Enviar el pedido y recibir la respuesta.
            respuesta = enviar_pedido(cliente_socket, producto_elegido, cantidad_elegida)

            # ── MOD-05: Actualizar contadores según la respuesta ───────────────
            # Siempre incrementar pedidos_enviados, sin importar el resultado.
            pedidos_enviados += 1

            # Determinar el resultado para el contador y el desglose.
            if respuesta:
                if respuesta["tipo"] == "confirmacion":
                    # El servidor aceptó el pedido en la cola.
                    pedidos_exitosos += 1
                    resultado_str = "✓ En cola"
                    log(f"✓ Servidor confirmó: {respuesta['mensaje']}")

                elif respuesta["tipo"] == "error":
                    # El servidor rechazó el pedido (cola llena).
                    pedidos_rechazados += 1
                    resultado_str = "✗ Rechazado (cola llena)"
                    log(f"✗ Servidor rechazó: {respuesta['mensaje']}")

                else:
                    # Respuesta de tipo desconocido.
                    resultado_str = f"? Tipo: {respuesta['tipo']}"
                    log(f"Servidor respondió: {respuesta['mensaje']}")

            else:
                # No se recibió respuesta (conexión cerrada).
                resultado_str = "⚠ Sin respuesta"
                log("⚠ No se recibió respuesta del servidor.")

            # Agregar el detalle de este pedido al desglose.
            # El desglose se usará luego en mostrar_estadisticas_cliente().
            desglose_pedidos.append({
                "numero": i,
                "producto": producto_elegido,
                "cantidad": cantidad_elegida,
                "resultado": resultado_str
            })
            # ──────────────────────────────────────────────────────────────────

            # Esperar un tiempo aleatorio antes del siguiente pedido.
            # Simula que el cliente "piensa" o "navega" entre pedidos.
            if i < num_pedidos:  # No esperar después del último pedido.
                pausa = random.uniform(0.5, 2.0)
                # random.uniform(a, b) retorna un float aleatorio entre a y b.
                log(f"Esperando {pausa:.1f}s antes del siguiente pedido...")
                # :.1f → formatea el float con 1 decimal.
                time.sleep(pausa)

        print("-" * 50)

        # ── MOD-05: Registrar tiempo de fin ───────────────────────────────────
        # Se registra DESPUÉS del último pedido, ANTES de enviar FIN.
        # Así medimos el tiempo de envío activo de pedidos.
        tiempo_fin = datetime.datetime.now()
        # ──────────────────────────────────────────────────────────────────────

        # ── PASO 5: Enviar señal de FIN ────────────────────────────────────────
        log("Todos los pedidos enviados. Cerrando sesión...")
        respuesta_fin = enviar_fin(cliente_socket)

        if respuesta_fin:
            log(f"Servidor confirma cierre: {respuesta_fin['mensaje']}")

    except ConnectionRefusedError:
        # El servidor no está corriendo o rechazó la conexión.
        log("ERROR: No se pudo conectar al servidor. ¿Está el servidor corriendo?")
        log(f"Asegúrate de que el servidor esté escuchando en {HOST}:{PORT}")

    except ConnectionResetError:
        # El servidor cerró la conexión inesperadamente.
        log("ERROR: El servidor cerró la conexión inesperadamente.")

    except Exception as e:
        # Cualquier otro error inesperado.
        log(f"ERROR inesperado: {e}")

    finally:
        # SIEMPRE cerrar el socket al terminar.
        if cliente_socket:
            cliente_socket.close()
            log("Conexión cerrada. ¡Hasta luego!")

        # ── MOD-05: Mostrar estadísticas del cliente ───────────────────────────
        # Se llama en finally para que se ejecute SIEMPRE, incluso si hubo
        # una excepción. Así el operador siempre ve un resumen de lo que ocurrió,
        # aunque la sesión haya fallado a mitad de camino.
        # Si tiempo_fin es None (falló antes de terminar), se registra ahora.
        if tiempo_fin is None and tiempo_inicio is not None:
            # El cliente falló antes de registrar tiempo_fin. Registramos el
            # momento de la falla para tener al menos un tiempo aproximado.
            tiempo_fin = datetime.datetime.now()

        # Solo mostrar estadísticas si al menos intentamos enviar algo.
        # (Si falló la conexión, pedidos_enviados == 0, pero igual mostramos
        # el resumen para que el operador sepa que no se pudo conectar.)
        mostrar_estadisticas_cliente(
            pedidos_enviados=pedidos_enviados,
            pedidos_exitosos=pedidos_exitosos,
            pedidos_rechazados=pedidos_rechazados,
            tiempo_inicio=tiempo_inicio,
            tiempo_fin=tiempo_fin,
            mi_id=mi_id,
            desglose_pedidos=desglose_pedidos
        )
        # ──────────────────────────────────────────────────────────────────────


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

# if __name__ == "__main__":
# - Verifica que este archivo se esté ejecutando directamente, no importado.
# - __name__ == "__main__" → True si se ejecuta directamente (python cliente.py).
# - __name__ == "cliente"  → True si se importa desde otro módulo.
# - ¿Por qué? Para que el cliente solo se ejecute cuando el archivo se lanza
#   directamente, no si alguien hace "import cliente" en otro programa.
if __name__ == "__main__":
    ejecutar_cliente()
