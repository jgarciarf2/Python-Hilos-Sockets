"""
=============================================================================
ARCHIVO: cliente.py
MÓDULO:  mod02_limite_pedidos_por_cliente
AUTOR:   Programación Concurrente — Python
FECHA:   2026
=============================================================================

DESCRIPCIÓN GENERAL
-------------------
Cliente TCP que se conecta al servidor de la MODIFICACIÓN 2 e intenta
enviar hasta 5 pedidos por cliente simulado.

COMPORTAMIENTO DEL CLIENTE EN MOD-02
--------------------------------------
- El cliente NO conoce el límite MAX_PEDIDOS_POR_CLIENTE del servidor.
- El cliente intenta enviar TOTAL_PEDIDOS_POR_CLIENTE = 5 pedidos.
- El servidor aceptará los primeros 3 y rechazará los últimos 2.
- Cuando el servidor rechaza un pedido, el cliente:
    1. Muestra el mensaje de rechazo en consola con el motivo.
    2. Registra cuántos pedidos fueron rechazados.
    3. Continúa con el siguiente pedido del ciclo.
- Al finalizar, el cliente imprime un resumen de aceptados/rechazados.

PROPÓSITO DIDÁCTICO
--------------------
Demostrar que el límite de pedidos es completamente gestionado por el
servidor: el cliente no necesita conocer ni respetar el límite. El servidor
es quien aplica la política de fairness de forma transparente. Esto refleja
el patrón de diseño "Policy Enforcement at the Server" común en sistemas
distribuidos y APIs RESTful (HTTP 429 Too Many Requests).

ARQUITECTURA DE CONEXIÓN
-------------------------
Este cliente usa una conexión TCP por pedido (connect → enviar → recibir →
close), en lugar de mantener una conexión persistente. Esto simplifica el
manejo de errores y es apropiado para el contexto académico.

En producción, se usaría connection pooling o keep-alive para reducir la
latencia de establecimiento de conexión (TCP handshake × N pedidos).

PROTOCOLO JSON SOBRE TCP
-------------------------
Solicitud:
  {"tipo": "pedido", "cliente": "<nombre>", "producto": "<nombre>",
   "cantidad": <int>}

Posibles respuestas del servidor:
  {"tipo": "confirmacion", "estado": "aceptado",
   "mensaje": "Pedido encolado correctamente"}

  {"tipo": "error", "mensaje": "Límite de pedidos alcanzado",
   "estado": "rechazado"}

  {"tipo": "error", "mensaje": "Stock insuficiente",
   "estado": "rechazado"}

  {"tipo": "error", "mensaje": "Servidor ocupado, intente más tarde",
   "estado": "rechazado"}
"""

# =============================================================================
# IMPORTACIONES
# =============================================================================
import socket    # Comunicación TCP: socket, connect, sendall, recv, close
import json      # Serialización/deserialización del protocolo de mensajes
import time      # sleep() para simular tiempo entre pedidos
import random    # Selección aleatoria de productos para variedad en los pedidos
import threading # Thread para ejecutar múltiples clientes en paralelo

# =============================================================================
# CONSTANTES DE CONFIGURACIÓN DE RED
# Deben coincidir exactamente con las del servidor para establecer conexión.
# =============================================================================

HOST = "127.0.0.1"
"""str: Dirección IP del servidor. Debe ser idéntica a la del servidor."""

PORT = 65000
"""int: Puerto TCP del servidor. Debe coincidir con el del servidor."""

ENCODING = "utf-8"
"""str: Codificación de mensajes. Debe coincidir con la del servidor."""

BUFFER_SIZE = 4096
"""int: Tamaño del buffer de recepción. 4096 bytes es más que suficiente
   para las respuestas JSON del servidor."""

TIMEOUT_CONEXION = 5.0
"""float: Segundos máximos para esperar una respuesta del servidor.
   Si el servidor no responde en este tiempo, se considera un error de red."""

# =============================================================================
# CONSTANTES DE COMPORTAMIENTO DEL CLIENTE
# =============================================================================

TOTAL_PEDIDOS_POR_CLIENTE = 5
"""int: Número total de pedidos que CADA cliente simulado intentará enviar.

   RELACIÓN CON MAX_PEDIDOS_POR_CLIENTE DEL SERVIDOR:
   ---------------------------------------------------
   El servidor acepta solo MAX_PEDIDOS_POR_CLIENTE = 3 pedidos por cliente.
   Este cliente intenta 5. Resultado esperado:
   - Pedidos 1, 2, 3 → ACEPTADOS por el servidor.
   - Pedidos 4, 5 → RECHAZADOS con "Límite de pedidos alcanzado".

   Esta discrepancia ES INTENCIONAL y demuestra que:
   1. La política de límite es server-side, no client-side.
   2. El cliente maneja graciosamente el rechazo (no se cuelga ni crashea).
   3. El servidor es el guardián del límite aunque el cliente ignore la regla.
"""

PAUSA_ENTRE_PEDIDOS = 0.5
"""float: Segundos de pausa entre pedidos consecutivos del mismo cliente.
   Simula el tiempo de usuario entre acciones y facilita la lectura de logs."""

# Catálogo de productos disponibles para solicitar.
# El cliente selecciona productos de esta lista aleatoriamente.
PRODUCTOS_DISPONIBLES = [
    "Laptop", "Mouse", "Teclado", "Monitor",
    "Auriculares", "USB", "Cargador", "Webcam"
]
"""list[str]: Lista de productos que el cliente puede solicitar.
   Debe ser un subconjunto de los productos en stock_productos del servidor."""

# =============================================================================
# FUNCIÓN: enviar_pedido
# =============================================================================

def enviar_pedido(nombre_cliente: str, producto: str, cantidad: int) -> dict:
    """Envía un único pedido al servidor y retorna la respuesta.

    Esta función encapsula el ciclo completo de una solicitud TCP:
    1. Crear un socket TCP nuevo.
    2. Conectar al servidor (TCP handshake: SYN → SYN-ACK → ACK).
    3. Serializar el pedido como JSON y enviarlo.
    4. Esperar y recibir la respuesta del servidor.
    5. Deserializar y retornar la respuesta como diccionario Python.
    6. Cerrar el socket (FIN → FIN-ACK → ACK).

    UNA CONEXIÓN POR PEDIDO:
    ------------------------
    Cada llamada a esta función abre y cierra una conexión TCP independiente.
    Esto es menos eficiente que una conexión persistente pero más simple
    y adecuado para el contexto académico.

    Parameters
    ----------
    nombre_cliente : str
        Identificador del cliente (p.ej. "Alice", "Bob"). Este nombre es el
        que el servidor usa para llevar el conteo de pedidos.
    producto : str
        Nombre del producto solicitado (debe existir en el catálogo del servidor).
    cantidad : int
        Cantidad de unidades solicitadas.

    Returns
    -------
    dict
        Respuesta del servidor como diccionario Python. Ejemplos:
        {"tipo": "confirmacion", "estado": "aceptado", "mensaje": "..."}
        {"tipo": "error", "mensaje": "Límite de pedidos alcanzado", "estado": "rechazado"}

    Raises
    ------
    ConnectionRefusedError
        Si el servidor no está escuchando en HOST:PORT.
    TimeoutError
        Si el servidor no responde dentro de TIMEOUT_CONEXION segundos.
    OSError
        Para otros errores de red (red caída, etc.).
    """
    # Crear un socket TCP/IPv4 nuevo para esta conexión.
    # context manager (with) garantiza que el socket se cierre aunque
    # ocurra una excepción en cualquier punto del bloque.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # Configurar timeout: si el servidor no responde en TIMEOUT_CONEXION
        # segundos, se lanza socket.timeout (subclase de OSError).
        sock.settimeout(TIMEOUT_CONEXION)

        # Establecer la conexión TCP con el servidor (handshake de 3 vías).
        sock.connect((HOST, PORT))

        # Construir el mensaje JSON del pedido.
        mensaje = {
            "tipo":     "pedido",
            "cliente":  nombre_cliente,
            "producto": producto,
            "cantidad": cantidad
        }

        # Serializar el diccionario Python → cadena JSON → bytes UTF-8.
        datos_enviados = json.dumps(mensaje).encode(ENCODING)

        # sendall() garantiza que todos los bytes sean enviados, incluso si
        # el sistema operativo los fragmenta en múltiples paquetes TCP.
        sock.sendall(datos_enviados)

        # Esperar y recibir la respuesta del servidor.
        # recv(BUFFER_SIZE) retorna hasta BUFFER_SIZE bytes.
        datos_recibidos = sock.recv(BUFFER_SIZE)

        # Deserializar la respuesta: bytes UTF-8 → cadena JSON → dict Python.
        respuesta = json.loads(datos_recibidos.decode(ENCODING))

        return respuesta
    # El with cierra automáticamente el socket aquí (equivale a sock.close()).


# =============================================================================
# FUNCIÓN: simular_cliente
# =============================================================================

def simular_cliente(nombre_cliente: str) -> None:
    """Simula un cliente que intenta enviar TOTAL_PEDIDOS_POR_CLIENTE pedidos.

    Esta función es ejecutada por un hilo independiente para cada cliente
    simulado, permitiendo que múltiples clientes interactúen con el servidor
    concurrentemente.

    COMPORTAMIENTO DETALLADO (MOD-02):
    -----------------------------------
    - Intenta enviar 5 pedidos (TOTAL_PEDIDOS_POR_CLIENTE).
    - El servidor aceptará los primeros 3 y rechazará los últimos 2.
    - Para cada respuesta, clasifica el resultado como aceptado o rechazado.
    - Al finalizar, imprime el resumen de aceptados/rechazados.

    MANEJO DE RECHAZOS:
    -------------------
    El cliente NO distingue el motivo del rechazo en su lógica:
    simplemente muestra el mensaje del servidor y continúa.
    Esto simula un cliente "honesto" que respeta las respuestas del servidor
    pero que no sabe de antemano cuántos pedidos puede hacer.

    Parameters
    ----------
    nombre_cliente : str
        Nombre del cliente simulado. Usado como identificador en el protocolo
        y en los mensajes de log.
    """
    print(f"\n{'='*55}")
    print(f"  CLIENTE: {nombre_cliente}")
    print(f"  Intentará enviar {TOTAL_PEDIDOS_POR_CLIENTE} pedidos al servidor")
    print(f"  Servidor: {HOST}:{PORT}")
    print(f"{'='*55}")

    # Contadores locales para el resumen final del cliente.
    pedidos_aceptados  = 0  # Pedidos que el servidor aceptó y encoló.
    pedidos_rechazados = 0  # Pedidos que el servidor rechazó (por cualquier motivo).

    # Bucle de envío de pedidos.
    for numero_pedido in range(1, TOTAL_PEDIDOS_POR_CLIENTE + 1):
        # Seleccionar un producto y cantidad aleatoriamente.
        producto = random.choice(PRODUCTOS_DISPONIBLES)
        cantidad = random.randint(1, 3)  # Entre 1 y 3 unidades.

        print(f"\n[{nombre_cliente}] Pedido #{numero_pedido}/{TOTAL_PEDIDOS_POR_CLIENTE}: "
              f"{cantidad}x '{producto}'")

        # =====================================================================
        # INTENTO DE ENVÍO: conectar al servidor y enviar el pedido.
        # =====================================================================
        try:
            respuesta = enviar_pedido(nombre_cliente, producto, cantidad)

            # Analizar la respuesta del servidor.
            tipo_resp   = respuesta.get("tipo",    "desconocido")
            estado_resp = respuesta.get("estado",  "desconocido")
            mensaje_srv = respuesta.get("mensaje", "Sin mensaje")

            if tipo_resp == "confirmacion" and estado_resp == "aceptado":
                # ─────────────────────────────────────────────────────────────
                # PEDIDO ACEPTADO
                # El servidor aceptó el pedido y lo encoló para procesamiento.
                # ─────────────────────────────────────────────────────────────
                pedidos_aceptados += 1
                print(f"[{nombre_cliente}] ✅ Pedido #{numero_pedido} ACEPTADO: {mensaje_srv}")

            elif tipo_resp == "error" and estado_resp == "rechazado":
                # ─────────────────────────────────────────────────────────────
                # PEDIDO RECHAZADO
                # El servidor rechazó el pedido. El cliente muestra el motivo
                # tal como lo informa el servidor.
                #
                # CASOS POSIBLES (MOD-02):
                # - "Límite de pedidos alcanzado" → pedidos 4 y 5.
                # - "Stock insuficiente" → stock agotado.
                # - "Servidor ocupado, intente más tarde" → cola llena.
                # - "Producto '...' no encontrado" → producto inválido.
                #
                # El cliente NO distingue el caso de límite de los demás
                # en su lógica: simplemente registra el rechazo y continúa.
                # ─────────────────────────────────────────────────────────────
                pedidos_rechazados += 1
                print(f"[{nombre_cliente}] ❌ Pedido #{numero_pedido} RECHAZADO: "
                      f"{mensaje_srv}")

                # Identificar si el rechazo fue específicamente por límite
                # de pedidos (solo para información en el log).
                if mensaje_srv == "Límite de pedidos alcanzado":
                    print(f"[{nombre_cliente}]    ℹ️  El servidor aplicó el límite "
                          f"de pedidos por cliente (MOD-02).")

            else:
                # Respuesta desconocida o malformada del servidor.
                pedidos_rechazados += 1
                print(f"[{nombre_cliente}] ⚠️  Respuesta inesperada del servidor: "
                      f"{respuesta}")

        except ConnectionRefusedError:
            # El servidor no está escuchando en HOST:PORT.
            pedidos_rechazados += 1
            print(f"[{nombre_cliente}] ❌ Pedido #{numero_pedido} FALLIDO: "
                  f"No se pudo conectar al servidor en {HOST}:{PORT}. "
                  f"¿Está el servidor ejecutándose?")

        except socket.timeout:
            # El servidor no respondió dentro del tiempo límite.
            pedidos_rechazados += 1
            print(f"[{nombre_cliente}] ❌ Pedido #{numero_pedido} FALLIDO: "
                  f"Timeout esperando respuesta del servidor ({TIMEOUT_CONEXION}s).")

        except (OSError, json.JSONDecodeError) as e:
            # Otros errores de red o de parsing de la respuesta.
            pedidos_rechazados += 1
            print(f"[{nombre_cliente}] ❌ Pedido #{numero_pedido} FALLIDO "
                  f"(error inesperado): {type(e).__name__}: {e}")

        # Pausa entre pedidos para no saturar el servidor inmediatamente
        # y para que los logs sean más legibles.
        if numero_pedido < TOTAL_PEDIDOS_POR_CLIENTE:
            time.sleep(PAUSA_ENTRE_PEDIDOS)

    # =========================================================================
    # RESUMEN FINAL DEL CLIENTE
    # =========================================================================
    print(f"\n{'─'*55}")
    print(f"  RESUMEN FINAL — {nombre_cliente}")
    print(f"{'─'*55}")
    print(f"  Pedidos intentados:  {TOTAL_PEDIDOS_POR_CLIENTE}")
    print(f"  ✅ Aceptados:        {pedidos_aceptados}")
    print(f"  ❌ Rechazados:       {pedidos_rechazados}")
    print(f"  Nota: El servidor acepta máx. 3 pedidos por cliente (MOD-02).")
    print(f"{'─'*55}\n")


# =============================================================================
# FUNCIÓN: main
# =============================================================================

def main() -> None:
    """Función principal que lanza múltiples clientes concurrentes.

    Lanza un hilo por cada cliente simulado y espera a que todos terminen.
    Esto permite observar cómo el servidor gestiona el límite de pedidos
    para varios clientes simultáneos.

    CLIENTES SIMULADOS:
    -------------------
    - Alice: intentará 5 pedidos → el servidor aceptará 3, rechazará 2.
    - Bob:   intentará 5 pedidos → el servidor aceptará 3, rechazará 2.
    - Carol: intentará 5 pedidos → el servidor aceptará 3, rechazará 2.

    Cada cliente tiene su propio contador independiente en el servidor
    (contadores_por_cliente["Alice"], contadores_por_cliente["Bob"], etc.).
    """
    print("=" * 65)
    print("   CLIENTE TCP — MOD-02: LÍMITE MÁXIMO DE PEDIDOS POR CLIENTE")
    print("=" * 65)
    print(f"   Servidor objetivo:      {HOST}:{PORT}")
    print(f"   Pedidos por cliente:    {TOTAL_PEDIDOS_POR_CLIENTE}")
    print(f"   Límite en el servidor:  3 (desconocido para el cliente)")
    print("=" * 65)
    print()

    # Lista de clientes a simular.
    # Cada nombre es un identificador único que el servidor usa para el conteo.
    clientes = ["Alice", "Bob", "Carol"]

    # =========================================================================
    # LANZAR HILOS DE CLIENTES
    # Cada cliente se ejecuta en un hilo propio para simular concurrencia real
    # (varios usuarios conectándose al mismo tiempo al servidor).
    # =========================================================================
    hilos = []
    for nombre in clientes:
        hilo = threading.Thread(
            target=simular_cliente,
            args=(nombre,),
            name=f"Hilo-{nombre}"
        )
        hilos.append(hilo)

    # Iniciar todos los hilos (casi simultáneamente).
    print("[MAIN] Iniciando clientes concurrentes...\n")
    for hilo in hilos:
        hilo.start()

    # Esperar a que todos los hilos terminen antes de imprimir el resumen.
    # join() bloquea el hilo principal hasta que el hilo hijo termine.
    for hilo in hilos:
        hilo.join()

    # =========================================================================
    # RESUMEN GLOBAL
    # =========================================================================
    print("\n" + "=" * 65)
    print("   TODOS LOS CLIENTES COMPLETARON SUS PEDIDOS")
    print("=" * 65)
    print("   Resultado esperado por cliente:")
    print(f"   - Pedidos intentados: {TOTAL_PEDIDOS_POR_CLIENTE}")
    print(f"   - Pedidos aceptados:  3 (límite del servidor, MOD-02)")
    print(f"   - Pedidos rechazados: {TOTAL_PEDIDOS_POR_CLIENTE - 3}")
    print("=" * 65)


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================

if __name__ == "__main__":
    """Ejecutar el cliente cuando el script se lanza directamente.

    INSTRUCCIONES DE USO:
    ---------------------
    1. Iniciar primero el servidor:
         python servidor.py

    2. En otra terminal, ejecutar el cliente:
         python cliente.py

    3. Observar en la consola del servidor cómo rechaza los pedidos 4 y 5
       de cada cliente con "Límite de pedidos alcanzado".

    4. Observar en la consola del cliente el resumen de aceptados/rechazados.
    """
    main()
