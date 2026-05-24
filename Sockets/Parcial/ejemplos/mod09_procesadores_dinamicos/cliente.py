"""
==============================================================================
MODIFICACIÓN 9: Número DINÁMICO de procesadores según la carga
==============================================================================

Archivo : cliente.py
Carpeta : mod09_procesadores_dinamicos/
Autores : Programación Concurrente — Ejemplo de hilos y sockets con Python

------------------------------------------------------------------------------
DESCRIPCIÓN GENERAL
------------------------------------------------------------------------------
Este cliente TCP simula múltiples compradores enviando pedidos al servidor
de manera concurrente. Su propósito es:

  1. Demostrar el protocolo JSON sobre TCP utilizado por el servidor.
  2. Generar carga variable (ráfagas de pedidos) para poder observar el
     comportamiento del monitor_carga del servidor (Modificación 9):
       - Ráfaga alta → la cola supera UMBRAL_ESCALAR → el servidor crea
         procesadores dinámicos.
       - Carga baja  → la cola baja de UMBRAL_REDUCIR → el servidor elimina
         procesadores sobrantes.

------------------------------------------------------------------------------
MODOS DE OPERACIÓN DEL CLIENTE
------------------------------------------------------------------------------
  1. Modo INTERACTIVO (por defecto al ejecutar sin argumentos):
       El usuario escribe el producto y cantidad desde la terminal.
       Un único hilo envía el pedido y muestra la respuesta.

  2. Modo CARGA (ejecutar con argumento "carga"):
       Lanza NUM_CLIENTES_CARGA hilos simultáneos, cada uno enviando
       PEDIDOS_POR_CLIENTE pedidos aleatorios con pausas PAUSA_ENTRE_PEDIDOS.
       Diseñado para activar el monitor de carga del servidor.

  3. Modo RAFAGA (ejecutar con argumento "rafaga"):
       Lanza todos los pedidos sin pausa para saturar la cola del servidor
       y forzar el escalado de procesadores.

------------------------------------------------------------------------------
PROTOCOLO DE COMUNICACIÓN
------------------------------------------------------------------------------
  Petición  : {"producto": "<nombre>", "cantidad": <int>}
  Respuesta : {"estado": "ok"|"error"|"sin_stock",
               "mensaje": "<texto>",
               "stock_restante": <int>|null}

Cada pedido usa una conexión TCP independiente (connect → send → recv → close).
Esto es intencional: muestra cómo el servidor crea un hilo por conexión y
encola el pedido.

==============================================================================
"""

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTACIONES
# ─────────────────────────────────────────────────────────────────────────────
import socket       # Comunicación TCP con el servidor
import threading    # Hilos para simular clientes concurrentes
import json         # Serialización/deserialización del protocolo
import time         # Pausas entre pedidos (modo carga)
import random       # Selección aleatoria de producto y cantidad
import sys          # Lectura de argumentos de línea de comandos (sys.argv)
import logging      # Mensajes de log con nivel y timestamp

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN DEL LOGGER
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-20s] %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("cliente")

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTES DE CONEXIÓN
# ─────────────────────────────────────────────────────────────────────────────
HOST        = "127.0.0.1"   # Misma dirección que el servidor
PORT        = 65000          # Mismo puerto que el servidor
ENCODING    = "utf-8"        # Misma codificación que el servidor
BUFFER_SIZE = 4096           # Bytes máximos para recv()
TIMEOUT_CONEXION = 5         # Segundos antes de abandonar la conexión

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTES MODO CARGA / RÁFAGA
# ─────────────────────────────────────────────────────────────────────────────
NUM_CLIENTES_CARGA   = 8    # Hilos simultáneos en modo "carga" y "rafaga"
PEDIDOS_POR_CLIENTE  = 3    # Pedidos que cada hilo enviará en modo "carga"
PAUSA_ENTRE_PEDIDOS  = 0.5  # Segundos entre pedidos (modo "carga")
                             # En modo "rafaga" esta pausa es 0.

# Catálogo de productos disponibles en el servidor (para generar pedidos
# aleatorios verosímiles sin consultar al servidor previamente).
CATALOGO_PRODUCTOS = [
    "Laptop", "Mouse", "Teclado", "Monitor",
    "Auriculares", "USB", "Cargador", "Webcam",
]

# ─────────────────────────────────────────────────────────────────────────────
# Lock de impresión
# ─────────────────────────────────────────────────────────────────────────────
# Cuando varios hilos imprimen a la vez, las líneas se entremezclan.
# Este lock garantiza que cada print() completo se ejecute sin interrupción.
lock_print = threading.Lock()


def imprimir(*args, **kwargs) -> None:
    """Imprime con exclusión mutua para evitar salida mezclada entre hilos."""
    with lock_print:
        print(*args, **kwargs)


# ─────────────────────────────────────────────────────────────────────────────
# FUNCIÓN: enviar_pedido
# ─────────────────────────────────────────────────────────────────────────────
def enviar_pedido(producto: str, cantidad: int) -> dict | None:
    """
    Abre una conexión TCP con el servidor, envía un pedido JSON y retorna
    la respuesta deserializada.

    Cada llamada a esta función representa un "cliente" independiente desde
    la perspectiva del servidor: establece su propia conexión y la cierra al
    terminar.

    Parámetros
    ----------
    producto : str   → Nombre del producto solicitado.
    cantidad : int   → Unidades solicitadas (debe ser > 0).

    Retorna
    -------
    dict
        Diccionario con la respuesta del servidor:
        {"estado": ..., "mensaje": ..., "stock_restante": ...}
    None
        Si ocurre un error de red o de protocolo.

    Excepciones manejadas internamente
    -----------------------------------
    ConnectionRefusedError → el servidor no está escuchando en HOST:PORT.
    socket.timeout         → el servidor tardó más de TIMEOUT_CONEXION segundos.
    json.JSONDecodeError   → la respuesta no es JSON válido.
    OSError                → error genérico de socket.
    """
    pedido = {"producto": producto, "cantidad": cantidad}

    try:
        # ── Crear socket TCP ───────────────────────────────────────────────
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(TIMEOUT_CONEXION)

            # ── Conectar al servidor ───────────────────────────────────────
            s.connect((HOST, PORT))
            log.debug("Conectado a %s:%d", HOST, PORT)

            # ── Enviar pedido serializado ──────────────────────────────────
            mensaje = json.dumps(pedido, ensure_ascii=False)
            s.sendall(mensaje.encode(ENCODING))
            log.debug("Pedido enviado: %s", mensaje)

            # ── Recibir respuesta ──────────────────────────────────────────
            datos_raw = s.recv(BUFFER_SIZE)
            if not datos_raw:
                log.warning("El servidor cerró la conexión sin responder.")
                return None

            # ── Deserializar respuesta ─────────────────────────────────────
            respuesta = json.loads(datos_raw.decode(ENCODING))
            return respuesta

    except ConnectionRefusedError:
        log.error("Conexión rechazada. ¿Está el servidor corriendo en %s:%d?", HOST, PORT)
    except socket.timeout:
        log.error("Tiempo de espera agotado conectando a %s:%d.", HOST, PORT)
    except json.JSONDecodeError as e:
        log.error("Respuesta inválida del servidor: %s", e)
    except OSError as e:
        log.error("Error de red: %s", e)

    return None


# ─────────────────────────────────────────────────────────────────────────────
# FUNCIÓN: mostrar_respuesta
# ─────────────────────────────────────────────────────────────────────────────
def mostrar_respuesta(respuesta: dict | None, producto: str, cantidad: int) -> None:
    """
    Formatea e imprime la respuesta del servidor de forma legible.

    Parámetros
    ----------
    respuesta : dict | None
        Respuesta del servidor (None si hubo error de red).
    producto  : str   → Producto que se solicitó.
    cantidad  : int   → Cantidad que se solicitó.
    """
    if respuesta is None:
        imprimir(f"  ✗ Sin respuesta del servidor para '{producto}' x{cantidad}.")
        return

    estado = respuesta.get("estado", "desconocido")
    mensaje = respuesta.get("mensaje", "—")
    stock   = respuesta.get("stock_restante")

    # Elegir ícono según estado
    iconos = {"ok": "✓", "sin_stock": "⚠", "error": "✗"}
    icono  = iconos.get(estado, "?")

    imprimir(
        f"  {icono} [{estado.upper():9s}] {mensaje}"
        + (f" | Stock: {stock}" if stock is not None else "")
    )


# ─────────────────────────────────────────────────────────────────────────────
# FUNCIÓN: hilo_cliente_carga
# ─────────────────────────────────────────────────────────────────────────────
def hilo_cliente_carga(id_cliente: int, pausa: float) -> None:
    """
    Función ejecutada por cada hilo en modo "carga" y "rafaga".

    Cada hilo envía PEDIDOS_POR_CLIENTE pedidos aleatorios al servidor,
    esperando `pausa` segundos entre cada uno.

    El uso de pedidos aleatorios es deliberado:
      - Estresa el stock de distintos productos simultáneamente.
      - Provoca situaciones de "sin_stock" naturalmente (sin configuración manual).
      - Hace que la cola del servidor suba rápidamente (activando monitor_carga).

    Parámetros
    ----------
    id_cliente : int   → Identificador numérico del cliente (para logs).
    pausa      : float → Segundos entre pedidos (0 = ráfaga sin pausa).
    """
    nombre = f"Cliente-{id_cliente:02d}"
    log.debug("%s iniciado.", nombre)

    for n_pedido in range(1, PEDIDOS_POR_CLIENTE + 1):
        # Seleccionar producto y cantidad aleatoriamente
        producto = random.choice(CATALOGO_PRODUCTOS)
        cantidad = random.randint(1, 3)  # Entre 1 y 3 unidades

        imprimir(
            f"  [{nombre}] Pedido {n_pedido}/{PEDIDOS_POR_CLIENTE}: "
            f"{cantidad}x {producto}"
        )

        # Enviar pedido y mostrar respuesta
        respuesta = enviar_pedido(producto, cantidad)
        mostrar_respuesta(respuesta, producto, cantidad)

        # Pausa entre pedidos (0 en modo ráfaga)
        if pausa > 0 and n_pedido < PEDIDOS_POR_CLIENTE:
            time.sleep(pausa)

    log.debug("%s finalizado.", nombre)


# ─────────────────────────────────────────────────────────────────────────────
# MODO: INTERACTIVO
# ─────────────────────────────────────────────────────────────────────────────
def modo_interactivo() -> None:
    """
    Modo interactivo: el usuario introduce pedidos manualmente desde la terminal.

    El bucle continúa hasta que el usuario ingrese un producto vacío o
    presione Ctrl+C.
    """
    print("=" * 60)
    print("  CLIENTE — Modo Interactivo")
    print(f"  Conectando a {HOST}:{PORT}")
    print("=" * 60)
    print("  Productos disponibles:")
    for prod in CATALOGO_PRODUCTOS:
        print(f"    • {prod}")
    print("  (Deja el nombre en blanco para salir)")
    print("-" * 60)

    while True:
        try:
            # Leer producto
            producto = input("\nProducto: ").strip()
            if not producto:
                print("Saliendo…")
                break

            # Validar que el producto existe en el catálogo local
            # (solo orientativo; el servidor tiene la última palabra)
            if producto not in CATALOGO_PRODUCTOS:
                print(f"  ⚠ '{producto}' no está en el catálogo local. "
                      "Se enviará igualmente al servidor.")

            # Leer cantidad
            try:
                cantidad_str = input("Cantidad: ").strip()
                cantidad = int(cantidad_str)
                if cantidad <= 0:
                    print("  La cantidad debe ser un número positivo.")
                    continue
            except ValueError:
                print("  Ingresa un número entero válido.")
                continue

            # Enviar pedido
            print(f"  → Enviando pedido: {cantidad}x {producto}…")
            respuesta = enviar_pedido(producto, cantidad)
            mostrar_respuesta(respuesta, producto, cantidad)

        except KeyboardInterrupt:
            print("\nInterrupción del usuario. Saliendo…")
            break


# ─────────────────────────────────────────────────────────────────────────────
# MODO: CARGA
# ─────────────────────────────────────────────────────────────────────────────
def modo_carga() -> None:
    """
    Modo de prueba de carga: lanza NUM_CLIENTES_CARGA hilos concurrentes,
    cada uno con PEDIDOS_POR_CLIENTE pedidos y PAUSA_ENTRE_PEDIDOS entre ellos.

    Propósito en el contexto de Modificación 9:
    ─────────────────────────────────────────────
    Con NUM_CLIENTES_CARGA=8 y PEDIDOS_POR_CLIENTE=3 se generan hasta 24
    pedidos en un período corto. La cola del servidor puede superar UMBRAL_ESCALAR
    (=5) fácilmente, haciendo que el monitor cree procesadores adicionales.
    Luego, al reducirse la carga, el monitor eliminará los procesadores sobrantes.
    """
    print("=" * 60)
    print("  CLIENTE — Modo Carga")
    print(f"  {NUM_CLIENTES_CARGA} clientes × {PEDIDOS_POR_CLIENTE} pedidos c/u")
    print(f"  Pausa entre pedidos: {PAUSA_ENTRE_PEDIDOS}s")
    print("=" * 60)

    hilos = []
    inicio = time.time()

    # Lanzar todos los hilos al mismo tiempo para maximizar la concurrencia
    for i in range(1, NUM_CLIENTES_CARGA + 1):
        h = threading.Thread(
            target=hilo_cliente_carga,
            args=(i, PAUSA_ENTRE_PEDIDOS),
            name=f"HiloCliente-{i:02d}",
            daemon=True,
        )
        hilos.append(h)
        h.start()

    # Esperar a que todos los hilos terminen
    for h in hilos:
        h.join()

    duracion = time.time() - inicio
    total_pedidos = NUM_CLIENTES_CARGA * PEDIDOS_POR_CLIENTE
    print("-" * 60)
    print(f"  Modo carga completado en {duracion:.2f}s")
    print(f"  Total pedidos enviados: {total_pedidos}")
    print("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# MODO: RÁFAGA
# ─────────────────────────────────────────────────────────────────────────────
def modo_rafaga() -> None:
    """
    Modo ráfaga: igual que modo_carga pero sin pausa entre pedidos (pausa=0).

    Propósito en el contexto de Modificación 9:
    ─────────────────────────────────────────────
    Al eliminar la pausa, todos los pedidos llegan casi simultáneamente al
    servidor. La cola se llena rápidamente superando UMBRAL_ESCALAR, forzando
    al monitor a crear múltiples procesadores en pocos ciclos de revisión.

    Además, el semáforo sem_capacidad_cola del servidor actuará como
    contrapresión: algunos hilos cliente quedarán bloqueados esperando que
    la cola tenga espacio libre (CAPACIDAD_MAXIMA_COLA=10).
    """
    print("=" * 60)
    print("  CLIENTE — Modo Ráfaga (sin pausa)")
    print(f"  {NUM_CLIENTES_CARGA} clientes × {PEDIDOS_POR_CLIENTE} pedidos c/u")
    print("  ⚡ Máxima concurrencia (sin pausas entre pedidos)")
    print("=" * 60)

    hilos = []
    inicio = time.time()

    for i in range(1, NUM_CLIENTES_CARGA + 1):
        h = threading.Thread(
            target=hilo_cliente_carga,
            args=(i, 0.0),   # pausa=0 → ráfaga
            name=f"HiloRafaga-{i:02d}",
            daemon=True,
        )
        hilos.append(h)
        h.start()

    for h in hilos:
        h.join()

    duracion = time.time() - inicio
    total_pedidos = NUM_CLIENTES_CARGA * PEDIDOS_POR_CLIENTE
    print("-" * 60)
    print(f"  Ráfaga completada en {duracion:.2f}s")
    print(f"  Total pedidos enviados: {total_pedidos}")
    print("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# PUNTO DE ENTRADA
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    """
    Selección de modo según argumento de línea de comandos:

      python cliente.py            → Modo interactivo (por defecto)
      python cliente.py carga      → Modo carga (con pausas entre pedidos)
      python cliente.py rafaga     → Modo ráfaga (sin pausas, máxima presión)
    """
    modos_validos = {"carga", "rafaga"}

    if len(sys.argv) < 2:
        # Sin argumento → modo interactivo
        modo_interactivo()
    else:
        modo = sys.argv[1].lower()
        if modo == "carga":
            modo_carga()
        elif modo == "rafaga":
            modo_rafaga()
        else:
            print(f"Modo desconocido: '{modo}'")
            print("Uso:")
            print("  python cliente.py            → Modo interactivo")
            print("  python cliente.py carga      → Prueba de carga")
            print("  python cliente.py rafaga     → Prueba de ráfaga")
            sys.exit(1)
