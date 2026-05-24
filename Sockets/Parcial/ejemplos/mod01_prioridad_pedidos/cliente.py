"""
================================================================================
MODIFICACIÓN 1: SISTEMA DE PRIORIDAD EN LOS PEDIDOS — cliente.py
================================================================================

PROPÓSITO GENERAL:
    Este cliente simula múltiples compradores que envían pedidos al servidor
    con diferentes niveles de PRIORIDAD. A diferencia del cliente original
    (donde todos los pedidos tenían el mismo peso), aquí cada pedido incluye
    un campo 'prioridad' elegido aleatoriamente entre 1 (alta), 2 (media)
    y 3 (baja).

PROBLEMA QUE RESUELVE:
    En el servidor original, todos los clientes competían en igualdad de
    condiciones (FIFO). Este cliente demuestra cómo un sistema puede indicar
    la urgencia de sus pedidos y confiar en que el servidor los atenderá en
    el orden correcto según la prioridad declarada.

DIFERENCIAS CLAVE RESPECTO AL CLIENTE ORIGINAL:
    1. Cada pedido incluye el campo 'prioridad': random.choice([1, 2, 3]).
    2. El log del cliente muestra la prioridad enviada y la devuelta por el servidor.
    3. Se añade un resumen final con la distribución de prioridades enviadas.
    4. La función 'enviar_pedido()' acepta y procesa el parámetro 'prioridad'.

PROTOCOLO JSON (cliente → servidor):
    {
        "tipo":      "pedido",
        "producto":  "Laptop",
        "cantidad":  2,
        "prioridad": 1       ← NUEVO (elegido aleatoriamente: 1, 2 o 3)
    }

PROTOCOLO JSON (servidor → cliente):
    {
        "estado":    "ok" | "error",
        "mensaje":   "Descripción",
        "prioridad": 1       ← NUEVO: confirmación de prioridad registrada
    }

ESTRUCTURA DEL CLIENTE:
    - Función main(): lanza NUM_CLIENTES_SIMULADOS hilos simultáneos.
    - Cada hilo ejecuta simular_cliente(): genera pedidos aleatorios con prioridad
      y llama a enviar_pedido() para cada uno.
    - enviar_pedido(): abre conexión TCP, envía JSON, recibe respuesta, cierra.
    - Al finalizar todos los hilos, se imprime un resumen estadístico.

AUTOR:      Programación Concurrente — Modificación 1
FECHA:      2026
VERSIÓN:    1.0
================================================================================
"""

# ---------------------------------------------------------------------------
# IMPORTACIONES
# ---------------------------------------------------------------------------
import socket       # API de sockets TCP/IP para la conexión con el servidor
import threading    # Para simular múltiples clientes concurrentes
import json         # Serialización de mensajes en formato JSON
import random       # Para elegir productos, cantidades y prioridades aleatoriamente
import time         # Para pausas entre pedidos y medición de tiempos
import logging      # Sistema de logging con niveles y timestamps
import sys          # Para acceder a stdout

# ---------------------------------------------------------------------------
# CONFIGURACIÓN DEL SISTEMA DE LOGGING
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,   # INFO es suficiente para el cliente (menos verboso que el servidor)
    format="%(asctime)s [%(levelname)-8s] [%(threadName)-20s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("ClientePrioridad")

# ---------------------------------------------------------------------------
# CONSTANTES DE CONFIGURACIÓN DEL CLIENTE
# ---------------------------------------------------------------------------

HOST = "127.0.0.1"
"""str: IP del servidor al que conectarse. Debe coincidir con servidor.py."""

PORT = 65000
"""int: Puerto TCP del servidor. Debe coincidir con servidor.py."""

ENCODING = "utf-8"
"""str: Codificación de caracteres para los mensajes JSON."""

BUFFER_SIZE = 4096
"""int: Tamaño del buffer para recibir la respuesta del servidor."""

NUM_CLIENTES_SIMULADOS = 5
"""int: Número de clientes (hilos) que se lanzarán simultáneamente.
Simula la carga concurrente real sobre el servidor."""

PEDIDOS_POR_CLIENTE = 4
"""int: Número de pedidos que enviará cada cliente simulado.
Total de pedidos = NUM_CLIENTES_SIMULADOS × PEDIDOS_POR_CLIENTE."""

PAUSA_ENTRE_PEDIDOS_MIN = 0.5
"""float: Pausa mínima (segundos) entre pedidos consecutivos del mismo cliente."""

PAUSA_ENTRE_PEDIDOS_MAX = 2.0
"""float: Pausa máxima (segundos) entre pedidos consecutivos del mismo cliente.
La pausa aleatoria simula el tiempo de 'decisión' de un usuario real."""

TIMEOUT_CONEXION = 10.0
"""float: Tiempo máximo (segundos) para establecer conexión con el servidor.
Si el servidor no responde en este tiempo, se considera un fallo."""

# ---------------------------------------------------------------------------
# CATÁLOGO DE PRODUCTOS Y PRIORIDADES
# ---------------------------------------------------------------------------

PRODUCTOS_DISPONIBLES = [
    "Laptop", "Mouse", "Teclado", "Monitor",
    "Auriculares", "USB", "Cargador", "Webcam"
]
"""list[str]: Productos que los clientes pueden pedir.
Debe ser un subconjunto del stock del servidor para tener pedidos exitosos."""

CANTIDADES_POSIBLES = [1, 2, 3, 4, 5]
"""list[int]: Cantidades posibles a pedir. Se elige aleatoriamente."""

PRIORIDADES_POSIBLES = [1, 2, 3]
"""list[int]: Valores de prioridad válidos.
    1 = ALTA  → El servidor procesará este pedido primero.
    2 = MEDIA → Prioridad intermedia.
    3 = BAJA  → El servidor lo procesará al final (puede esperar).

MODIFICACIÓN 1: Esta lista es nueva. En el cliente original no existía el
concepto de prioridad; todos los pedidos eran iguales. Ahora el cliente elige
aleatoriamente con random.choice(PRIORIDADES_POSIBLES).

¿Por qué aleatorio?
    Para simular un entorno realista donde los clientes tienen distintas urgencias:
    - Un cliente VIP podría usar siempre prioridad 1.
    - Un proceso batch nocturno usaría siempre prioridad 3.
    - La elección aleatoria permite probar los tres escenarios mezclados."""

# ---------------------------------------------------------------------------
# DICCIONARIO DE TEXTO PARA PRIORIDADES
# ---------------------------------------------------------------------------

TEXTO_PRIORIDAD = {1: "ALTA", 2: "MEDIA", 3: "BAJA"}
"""dict[int, str]: Mapeo de valor numérico de prioridad a texto descriptivo.
Se usa en logs para hacer la salida más legible que solo el número."""

# ---------------------------------------------------------------------------
# ESTADÍSTICAS COMPARTIDAS (NUEVO EN MODIFICACIÓN 1)
# ---------------------------------------------------------------------------
# Contadores para el resumen final de distribución de prioridades enviadas.
# Se protegen con un Lock porque múltiples hilos los modificarán concurrentemente.

estadisticas_lock = threading.Lock()
"""threading.Lock: Protege los contadores de estadísticas compartidas."""

estadisticas = {
    "prioridad_1_enviados": 0,   # Pedidos con prioridad ALTA enviados
    "prioridad_2_enviados": 0,   # Pedidos con prioridad MEDIA enviados
    "prioridad_3_enviados": 0,   # Pedidos con prioridad BAJA enviados
    "total_exitosos":       0,   # Pedidos confirmados con "estado": "ok"
    "total_errores":        0,   # Pedidos rechazados o con error
    "total_enviados":       0,   # Total de pedidos enviados al servidor
}
"""dict[str, int]: Contadores globales de estadísticas del cliente.
NUEVO en Modificación 1: permite verificar la distribución de prioridades."""


# ---------------------------------------------------------------------------
# FUNCIÓN: ENVIAR UN PEDIDO AL SERVIDOR
# ---------------------------------------------------------------------------

def enviar_pedido(
    id_cliente: int,
    producto: str,
    cantidad: int,
    prioridad: int,
    num_pedido: int
) -> dict:
    """
    Abre una conexión TCP con el servidor, envía un pedido JSON con prioridad,
    recibe la respuesta y la retorna como diccionario.

    MODIFICACIÓN RESPECTO AL ORIGINAL:
        1. Acepta el parámetro 'prioridad' (antes no existía).
        2. Incluye 'prioridad' en el dict JSON que se envía al servidor.
        3. Registra en log la prioridad enviada y la confirmada por el servidor.
        4. Actualiza las estadísticas de prioridad (contador global).

    PATRÓN DE CONEXIÓN:
        Una conexión TCP separada por pedido (no persistente).
        Esto simplifica el código: connect → send → recv → close.
        En sistemas de alta performance se usaría connection pooling o HTTP/2,
        pero para este ejercicio una conexión por pedido es adecuada.

    Args:
        id_cliente (int):   Identificador del cliente (para logs).
        producto (str):     Nombre del producto a pedir.
        cantidad (int):     Unidades a pedir.
        prioridad (int):    Urgencia del pedido (1=alta, 2=media, 3=baja).
        num_pedido (int):   Número secuencial del pedido de este cliente.

    Returns:
        dict: Respuesta del servidor parseada como diccionario Python.
              Retorna {} si hubo error de red o JSON inválido.
    """
    prioridad_texto = TEXTO_PRIORIDAD.get(prioridad, "DESCONOCIDA")

    logger.info(
        f"[CLIENTE-{id_cliente}] → Enviando pedido #{num_pedido} "
        f"| Prioridad: {prioridad} ({prioridad_texto}) "
        f"| {producto} x{cantidad}"
    )

    # Construir el mensaje JSON del pedido
    # MODIFICACIÓN 1: Se añade el campo 'prioridad' al dict.
    # En el cliente original, el mensaje era:
    #   {"tipo": "pedido", "producto": producto, "cantidad": cantidad}
    # Ahora es:
    #   {"tipo": "pedido", "producto": producto, "cantidad": cantidad, "prioridad": 1}
    mensaje = {
        "tipo":      "pedido",
        "producto":  producto,
        "cantidad":  cantidad,
        "prioridad": prioridad,   # ← NUEVO CAMPO
    }

    # Serializar a JSON y codificar a bytes
    datos_a_enviar = json.dumps(mensaje).encode(ENCODING)

    respuesta_dict = {}

    try:
        # ── CREAR SOCKET TCP ───────────────────────────────────────────────────
        # AF_INET:     Protocolo IPv4
        # SOCK_STREAM: TCP (confiable, orientado a conexión)
        # 'with' garantiza que el socket se cierre aunque ocurra una excepción.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

            # Configurar timeout para la conexión Y para recv()
            # Si el servidor no responde en TIMEOUT_CONEXION segundos → excepción
            sock.settimeout(TIMEOUT_CONEXION)

            # ── CONECTAR AL SERVIDOR ───────────────────────────────────────────
            # connect() establece el handshake TCP (SYN → SYN-ACK → ACK).
            # Si el servidor no está escuchando → ConnectionRefusedError.
            sock.connect((HOST, PORT))

            # ── ENVIAR DATOS ───────────────────────────────────────────────────
            # sendall() garantiza que TODOS los bytes se envíen, incluso si
            # internamente requiere múltiples llamadas a send().
            sock.sendall(datos_a_enviar)

            # ── RECIBIR RESPUESTA ──────────────────────────────────────────────
            # recv() es bloqueante hasta TIMEOUT_CONEXION segundos.
            respuesta_cruda = sock.recv(BUFFER_SIZE)

            if respuesta_cruda:
                respuesta_dict = json.loads(respuesta_cruda.decode(ENCODING))
            else:
                logger.warning(
                    f"[CLIENTE-{id_cliente}] Servidor cerró conexión sin responder."
                )

    except ConnectionRefusedError:
        logger.error(
            f"[CLIENTE-{id_cliente}] ✗ Conexión rechazada. "
            f"¿Está corriendo el servidor en {HOST}:{PORT}?"
        )
        respuesta_dict = {"estado": "error", "mensaje": "Conexión rechazada."}

    except socket.timeout:
        logger.error(
            f"[CLIENTE-{id_cliente}] ✗ Timeout: el servidor no respondió "
            f"en {TIMEOUT_CONEXION}s."
        )
        respuesta_dict = {"estado": "error", "mensaje": "Timeout de conexión."}

    except json.JSONDecodeError as e:
        logger.error(
            f"[CLIENTE-{id_cliente}] ✗ Respuesta del servidor no es JSON válido: {e}"
        )
        respuesta_dict = {"estado": "error", "mensaje": "Respuesta inválida del servidor."}

    except OSError as e:
        logger.error(f"[CLIENTE-{id_cliente}] ✗ Error de red: {e}")
        respuesta_dict = {"estado": "error", "mensaje": str(e)}

    # ── REGISTRAR RESPUESTA EN LOG ─────────────────────────────────────────────
    # MODIFICACIÓN 1: Se muestra también la prioridad devuelta por el servidor
    # para verificar que fue registrada correctamente.
    estado     = respuesta_dict.get("estado", "desconocido")
    msg        = respuesta_dict.get("mensaje", "Sin mensaje")
    pri_server = respuesta_dict.get("prioridad", "N/A")  # ← NUEVO: del servidor

    if estado == "ok":
        logger.info(
            f"[CLIENTE-{id_cliente}] ← Respuesta: ✓ OK "
            f"| Prioridad confirmada: {pri_server} "
            f"| {msg}"
        )
    else:
        logger.warning(
            f"[CLIENTE-{id_cliente}] ← Respuesta: ✗ ERROR "
            f"| Prioridad enviada: {prioridad} "
            f"| {msg}"
        )

    # ── ACTUALIZAR ESTADÍSTICAS GLOBALES ──────────────────────────────────────
    # El lock garantiza que los contadores no se corrompan por acceso concurrente.
    with estadisticas_lock:
        estadisticas["total_enviados"] += 1
        estadisticas[f"prioridad_{prioridad}_enviados"] += 1  # ← NUEVO
        if estado == "ok":
            estadisticas["total_exitosos"] += 1
        else:
            estadisticas["total_errores"] += 1

    return respuesta_dict


# ---------------------------------------------------------------------------
# FUNCIÓN: SIMULAR COMPORTAMIENTO DE UN CLIENTE
# ---------------------------------------------------------------------------

def simular_cliente(id_cliente: int) -> None:
    """
    Simula un cliente que envía PEDIDOS_POR_CLIENTE pedidos al servidor,
    cada uno con una prioridad elegida aleatoriamente entre 1, 2 y 3.

    MODIFICACIÓN RESPECTO AL ORIGINAL:
        1. Genera 'prioridad' aleatoria con random.choice(PRIORIDADES_POSIBLES).
        2. Pasa 'prioridad' a la función enviar_pedido().
        3. El log de inicio del cliente muestra que enviará prioridades variables.

    COMPORTAMIENTO:
        Para cada pedido:
        1. Elegir aleatoriamente: producto, cantidad y prioridad.
        2. Llamar a enviar_pedido() para comunicarse con el servidor.
        3. Esperar una pausa aleatoria antes del siguiente pedido
           (simula tiempo de reflexión del usuario).

    ¿POR QUÉ random.choice() Y NO random.randint()?
        - random.choice(PRIORIDADES_POSIBLES) es más explícito: queda claro
          qué valores son válidos al mirar PRIORIDADES_POSIBLES = [1, 2, 3].
        - random.randint(1, 3) también funcionaría, pero ocultaría el conjunto
          de valores válidos dentro de los argumentos de la función.

    Args:
        id_cliente (int): Identificador único del cliente simulado.
    """
    logger.info(
        f"[CLIENTE-{id_cliente}] Iniciado. "
        f"Enviará {PEDIDOS_POR_CLIENTE} pedidos con prioridad aleatoria (1/2/3)."
    )

    for num_pedido in range(1, PEDIDOS_POR_CLIENTE + 1):
        # ── SELECCIÓN ALEATORIA DE PARÁMETROS DEL PEDIDO ──────────────────────

        producto = random.choice(PRODUCTOS_DISPONIBLES)
        """str: Producto elegido al azar del catálogo disponible."""

        cantidad = random.choice(CANTIDADES_POSIBLES)
        """int: Cantidad elegida aleatoriamente."""

        # MODIFICACIÓN CENTRAL: Elegir prioridad aleatoriamente.
        # En el cliente original NO existía esta línea.
        # random.choice() selecciona uniformemente de la lista:
        #   [1, 2, 3] → probabilidad 1/3 para cada valor.
        # En un sistema real, la prioridad podría depender del tipo de cliente,
        # el plan de suscripción, o la urgencia declarada por el usuario.
        prioridad = random.choice(PRIORIDADES_POSIBLES)
        """int: Prioridad del pedido elegida aleatoriamente (1, 2 o 3)."""

        # ── ENVIAR EL PEDIDO ───────────────────────────────────────────────────
        enviar_pedido(
            id_cliente=id_cliente,
            producto=producto,
            cantidad=cantidad,
            prioridad=prioridad,   # ← NUEVO parámetro
            num_pedido=num_pedido,
        )

        # ── PAUSA ENTRE PEDIDOS ────────────────────────────────────────────────
        # No enviar todos los pedidos instantáneamente: simula comportamiento real.
        # La pausa es aleatoria para que los clientes no estén sincronizados
        # (lo que crearía patrones artificiales en la prueba de concurrencia).
        if num_pedido < PEDIDOS_POR_CLIENTE:  # No pausar después del último pedido
            pausa = random.uniform(PAUSA_ENTRE_PEDIDOS_MIN, PAUSA_ENTRE_PEDIDOS_MAX)
            logger.debug(
                f"[CLIENTE-{id_cliente}] Esperando {pausa:.2f}s antes del siguiente pedido."
            )
            time.sleep(pausa)

    logger.info(
        f"[CLIENTE-{id_cliente}] ✓ Todos los pedidos enviados ({PEDIDOS_POR_CLIENTE} total)."
    )


# ---------------------------------------------------------------------------
# FUNCIÓN: IMPRIMIR RESUMEN DE ESTADÍSTICAS (NUEVO EN MODIFICACIÓN 1)
# ---------------------------------------------------------------------------

def imprimir_resumen() -> None:
    """
    Imprime un resumen estadístico de los pedidos enviados por todos los clientes.

    NUEVA EN MODIFICACIÓN 1:
        Esta función no existía en el cliente original. Se añade para verificar
        que la distribución de prioridades fue razonablemente uniforme (cada
        prioridad debería tener aprox. 1/3 de los pedidos totales).

    PROPÓSITO:
        Permite verificar que:
        1. La distribución de prioridades aleatorias es aproximadamente uniforme.
        2. El número de pedidos exitosos y fallidos es el esperado.
        3. Todos los pedidos fueron enviados correctamente.
    """
    total = estadisticas["total_enviados"]
    p1    = estadisticas["prioridad_1_enviados"]
    p2    = estadisticas["prioridad_2_enviados"]
    p3    = estadisticas["prioridad_3_enviados"]
    ok    = estadisticas["total_exitosos"]
    err   = estadisticas["total_errores"]

    # Calcular porcentajes (evitar división por cero)
    def pct(n):
        return f"{(n / total * 100):.1f}%" if total > 0 else "0%"

    logger.info("\n" + "=" * 60)
    logger.info("  RESUMEN FINAL DE PEDIDOS — MODIFICACIÓN 1 (PRIORIDAD)")
    logger.info("=" * 60)
    logger.info(f"  Total enviados:          {total}")
    logger.info(f"  ├─ Prioridad 1 (ALTA):   {p1:3d} pedidos  ({pct(p1)})")
    logger.info(f"  ├─ Prioridad 2 (MEDIA):  {p2:3d} pedidos  ({pct(p2)})")
    logger.info(f"  └─ Prioridad 3 (BAJA):   {p3:3d} pedidos  ({pct(p3)})")
    logger.info(f"  Exitosos (OK):           {ok}")
    logger.info(f"  Errores:                 {err}")
    logger.info("=" * 60)
    logger.info(
        "  NOTA: Los pedidos de prioridad 1 deben aparecer primero"
    )
    logger.info(
        "  en los logs del SERVIDOR, independientemente del orden de llegada."
    )
    logger.info("=" * 60 + "\n")


# ---------------------------------------------------------------------------
# FUNCIÓN PRINCIPAL
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Punto de entrada del cliente. Lanza NUM_CLIENTES_SIMULADOS hilos simultáneos,
    cada uno simulando un cliente que envía pedidos con prioridades aleatorias.

    FLUJO:
        1. Imprimir configuración y aviso del sistema de prioridad.
        2. Crear y lanzar NUM_CLIENTES_SIMULADOS hilos.
        3. Esperar a que todos los hilos terminen (join).
        4. Imprimir el resumen estadístico final.

    ¿POR QUÉ LANZAR TODOS LOS HILOS ANTES DE HACER join()?
        Si hiciéramos start() + join() en el mismo bucle, los clientes serían
        secuenciales (uno espera al anterior). Separar los dos bucles garantiza
        que TODOS los clientes corran simultáneamente, lo que es el propósito
        de la simulación de carga concurrente.
    """
    logger.info("=" * 60)
    logger.info("  CLIENTE CONCURRENTE — MODIFICACIÓN 1: PRIORIDAD")
    logger.info("=" * 60)
    logger.info(f"  Servidor destino:    {HOST}:{PORT}")
    logger.info(f"  Clientes a simular:  {NUM_CLIENTES_SIMULADOS}")
    logger.info(f"  Pedidos por cliente: {PEDIDOS_POR_CLIENTE}")
    logger.info(
        f"  Total pedidos:       "
        f"{NUM_CLIENTES_SIMULADOS * PEDIDOS_POR_CLIENTE}"
    )
    logger.info(f"  Prioridades:         1=ALTA, 2=MEDIA, 3=BAJA (aleatorias)")
    logger.info("=" * 60 + "\n")

    # ── CREAR TODOS LOS HILOS ──────────────────────────────────────────────────
    # Se crean primero todos los hilos antes de iniciar ninguno,
    # aunque en la práctica la diferencia es mínima aquí.
    hilos_clientes = []
    for i in range(1, NUM_CLIENTES_SIMULADOS + 1):
        hilo = threading.Thread(
            target=simular_cliente,
            args=(i,),
            name=f"SimCliente-{i}",
            daemon=False   # NO daemon: queremos esperar a que terminen con join()
        )
        hilos_clientes.append(hilo)

    # ── LANZAR TODOS LOS HILOS SIMULTÁNEAMENTE ─────────────────────────────────
    # Al hacer start() en un bucle separado del join(), todos los clientes
    # se inician prácticamente al mismo tiempo, creando carga concurrente real.
    logger.info(f"Lanzando {NUM_CLIENTES_SIMULADOS} clientes simultáneos...")
    for hilo in hilos_clientes:
        hilo.start()

    # ── ESPERAR A QUE TODOS LOS CLIENTES TERMINEN ─────────────────────────────
    # join() bloquea el hilo principal hasta que el hilo objetivo termina.
    # Sin join(), el programa principal terminaría antes que los hilos hijos,
    # posiblemente cortando pedidos a medias.
    for hilo in hilos_clientes:
        hilo.join()

    # ── IMPRIMIR RESUMEN FINAL ─────────────────────────────────────────────────
    # NUEVO EN MODIFICACIÓN 1: Mostrar distribución de prioridades enviadas.
    imprimir_resumen()

    logger.info("✓ Todos los clientes han terminado. Fin del programa.")


# ---------------------------------------------------------------------------
# PUNTO DE ENTRADA
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
