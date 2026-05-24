"""
================================================================================
SERVIDOR - CENTRAL DE PEDIDOS CON PRODUCTORES Y CONSUMIDORES
MODIFICACIÓN 5: ESTADÍSTICAS AL FINALIZAR
================================================================================

PROPÓSITO GENERAL DEL ARCHIVO:
- Implementar un SERVIDOR TCP que actúa como una "central de pedidos".
- Los CLIENTES (productores) se conectan remotamente vía socket TCP/IP y envían
  pedidos de productos.
- El servidor recibe esos pedidos, los ENCOLA en una cola compartida, y un
  conjunto de HILOS PROCESADORES (consumidores) los despachan.
- Se usan todas las primitivas de sincronización vistas en clase:
    1. SOCKETS TCP/IP  → comunicación cliente-servidor.
    2. HILOS (Thread)  → un hilo por cliente + hilos procesadores.
    3. COLA compartida → almacena pedidos pendientes (list de Python).
    4. SEMÁFORO        → limita la capacidad máxima de la cola.
    5. LOCK            → protege el acceso concurrente a la cola, al stock
                         y al nuevo diccionario de ESTADÍSTICAS.
    6. BARRERA         → los procesadores esperan a que haya al menos N pedidos
                         acumulados antes de empezar a despachar.

QUÉ AGREGA ESTA MODIFICACIÓN (MOD-05) RESPECTO AL ORIGINAL:
    ─────────────────────────────────────────────────────────────────────────
    PROBLEMA QUE RESUELVE:
        El servidor original no guarda ningún registro de lo que ocurrió
        durante su ejecución. Al cerrar no sabemos cuántos pedidos llegaron,
        cuáles fueron rechazados por cola llena, cuáles por falta de stock,
        qué cliente hizo más pedidos ni qué producto fue el más solicitado.
        Tener un resumen al final es fundamental para auditoría, depuración
        y toma de decisiones de negocio.

    SOLUCIÓN IMPLEMENTADA:
        Se agrega un diccionario global `estadisticas` que los hilos actualizan
        de forma thread-safe durante toda la vida del servidor. Al terminar,
        la función `mostrar_estadisticas()` imprime un reporte formateado.

    CAMBIOS CONCRETOS:
        1. Se importa `datetime` para registrar tiempo de inicio/fin.
        2. Se crea `estadisticas = {}` con todos los contadores y sub-dicts.
        3. Se crea `lock_estadisticas = threading.Lock()` para protegerlo.
        4. Se crea `actualizar_estadistica(clave, valor)` thread-safe.
        5. Se llama a `actualizar_estadistica()` en los puntos clave:
               - agregar_pedido_a_cola()  → total_pedidos_recibidos
               - agregar_pedido_a_cola()  → total_pedidos_rechazados_cola_llena
               - agregar_pedido_a_cola()  → pedidos_por_cliente
               - procesar_pedido()        → total_pedidos_despachados
               - procesar_pedido()        → total_pedidos_rechazados_stock
               - procesar_pedido()        → pedidos_por_producto
        6. Se crea `mostrar_estadisticas()` que imprime el reporte final.
        7. `iniciar_servidor()` llama a `mostrar_estadisticas()` al final.
    ─────────────────────────────────────────────────────────────────────────

FLUJO GENERAL:
    1. El servidor arranca, registra tiempo_inicio y muestra el stock inicial.
    2. Lanza HILOS PROCESADORES que quedarán bloqueados en la BARRERA.
    3. Abre un socket TCP y escucha conexiones entrantes.
    4. Por cada cliente que se conecta, crea un HILO OPERADOR que:
       a. Recibe los pedidos del cliente (producto + cantidad).
       b. Valida que haya espacio en la cola (SEMÁFORO).
       c. Agrega el pedido a la cola compartida (protegida por LOCK).
       d. Actualiza estadísticas en cada paso (thread-safe).
    5. Cuando se acumulan suficientes pedidos (BARRERA), los procesadores
       se desbloquean y comienzan a despachar.
    6. Cada procesador actualiza estadísticas al despachar o rechazar.
    7. Al cerrar: se registra tiempo_fin y se imprime el reporte completo.

EJECUCIÓN:
    python servidor.py

CONCEPTOS CLAVE:
    - PRODUCTOR: El hilo que atiende a cada cliente. "Produce" pedidos.
    - CONSUMIDOR: El hilo procesador. "Consume" pedidos de la cola.
    - SECCIÓN CRÍTICA: Código que accede a recursos compartidos (cola, stock,
      estadísticas). Debe protegerse con Lock.
    - DEADLOCK: Se evita adquiriendo locks en orden consistente y nunca
      anidando lock_estadisticas dentro de lock_cola ni viceversa.
================================================================================
"""

# ==============================================================================
# IMPORTACIONES
# ==============================================================================

# import socket
# - Módulo estándar de Python para comunicación de red.
# - Permite crear sockets TCP/IP para enviar y recibir datos entre procesos.
# - Funciones principales que usaremos:
#     * socket.socket(AF_INET, SOCK_STREAM) → crea un socket TCP sobre IPv4.
#     * .bind((host, port))   → asocia el socket a una dirección IP y puerto.
#     * .listen(n)            → pone el socket en modo escucha, con cola de n.
#     * .accept()             → espera y acepta una conexión entrante.
#                               Retorna (socket_cliente, (ip, puerto)).
#     * .recv(n)              → recibe hasta n bytes del socket (bloqueante).
#     * .sendall(bytes)       → envía todos los bytes (reintenta si es necesario).
#     * .close()              → cierra el socket y libera el puerto.
import socket

# import threading
# - Módulo estándar para crear y manejar hilos (threads) en Python.
# - Un HILO es una unidad de ejecución dentro de un proceso. Múltiples hilos
#   comparten el mismo espacio de memoria (variables globales, objetos).
# - Clases/funciones que usaremos:
#     * Thread(target=funcion, args=(arg1, arg2))
#       → Crea un hilo que ejecutará 'funcion' con los argumentos dados.
#       → .start() lo inicia, .join() espera a que termine.
#     * Lock()
#       → Candado de exclusión mutua (mutex). Solo UN hilo puede tenerlo.
#       → .acquire() lo toma (bloquea si otro lo tiene), .release() lo suelta.
#     * Semaphore(n)
#       → Contador protegido. .acquire() decrementa, .release() incrementa.
#     * Barrier(n)
#       → Punto de sincronización. Bloquea hasta que n hilos llamen a .wait().
#     * Event()
#       → Bandera booleana thread-safe. .set() / .clear() / .wait() / .is_set()
import threading

# import time
# - Módulo estándar para funciones relacionadas con el tiempo.
# - Usaremos:
#     * time.sleep(segundos) → pausa el hilo actual por N segundos.
#     * time.strftime(formato) → retorna la hora actual formateada.
import time

# import random
# - Módulo estándar para generar números aleatorios.
# - Usaremos random.randint(a, b) para simular tiempos de procesamiento.
import random

# import json
# - Módulo estándar para serializar/deserializar datos en formato JSON.
# - JSON permite convertir dicts Python ↔ strings para enviar por socket.
# - Funciones:
#     * json.dumps(dict) → convierte dict a string JSON.
#     * json.loads(str)  → convierte string JSON a dict Python.
import json

# ─────────────────────────────────────────────────────────────────────────────
# MOD-05: import datetime
# ─────────────────────────────────────────────────────────────────────────────
# ¿POR QUÉ se importa datetime?
#   El módulo `time` solo proporciona timestamps numéricos o strings formateados
#   para *mostrar* la hora, pero no permite hacer aritmética de fechas fácilmente
#   (por ejemplo, calcular cuánto tiempo tardó el servidor en total).
#   `datetime.datetime` almacena un instante de tiempo como objeto y permite
#   restar dos instantes para obtener un `timedelta` (duración).
#   - datetime.datetime.now() → captura el instante actual como objeto datetime.
#   - fin - inicio            → produce un timedelta con la duración exacta.
#   - str(timedelta)          → lo convierte a string legible ("0:05:23.456789").
# ─────────────────────────────────────────────────────────────────────────────
import datetime


# ==============================================================================
# CONSTANTES GLOBALES
# ==============================================================================

# HOST = "127.0.0.1"
# - Dirección IP de LOOPBACK (localhost). Solo acepta conexiones locales.
# - Usar "0.0.0.0" para aceptar desde cualquier interfaz de red.
HOST = "127.0.0.1"

# PORT = 65000
# - Puerto TCP donde el servidor escucha.
# - Rango 49152–65535 = puertos dinámicos/privados, ideal para pruebas.
PORT = 65000

# ENCODING = "utf-8"
# - Codificación para convertir strings ↔ bytes.
# - UTF-8 soporta todos los caracteres Unicode (acentos, ñ, etc.).
ENCODING = "utf-8"

# BUFFER_SIZE = 4096
# - Tamaño máximo del buffer de recepción (4 KB). Suficiente para JSON pequeños.
BUFFER_SIZE = 4096

# CAPACIDAD_MAXIMA_COLA = 10
# - Máximo número de pedidos simultáneos en la cola.
# - El semáforo usa este valor como límite superior.
CAPACIDAD_MAXIMA_COLA = 10

# NUM_PROCESADORES = 3
# - Cantidad de hilos procesadores (consumidores) para despachar pedidos.
NUM_PROCESADORES = 3

# PEDIDOS_MINIMOS_PARA_BARRERA = 5
# - Umbral mínimo de pedidos antes de que los procesadores se desbloqueen.
# - Simula el procesamiento por lotes (batch processing).
PEDIDOS_MINIMOS_PARA_BARRERA = 5

# MAX_CLIENTES = 5
# - Número máximo de clientes que el servidor acepta antes de cerrar.
MAX_CLIENTES = 5


# ==============================================================================
# STOCK DE PRODUCTOS (INVENTARIO)
# ==============================================================================

# stock_productos: dict[str, int]
# - Diccionario que mapea nombre_producto → cantidad_disponible.
# - RECURSO COMPARTIDO: múltiples hilos lo leen y modifican.
# - DEBE protegerse con lock_stock para evitar condiciones de carrera.
stock_productos = {
    "Laptop": 10,       # 10 laptops disponibles
    "Mouse": 25,        # 25 mouses disponibles
    "Teclado": 20,      # 20 teclados disponibles
    "Monitor": 8,       # 8 monitores disponibles
    "Auriculares": 15,  # 15 auriculares disponibles
    "USB": 30,          # 30 memorias USB disponibles
    "Cargador": 18,     # 18 cargadores disponibles
    "Webcam": 12,       # 12 webcams disponibles
}


# ==============================================================================
# COLA COMPARTIDA DE PEDIDOS
# ==============================================================================

# cola_pedidos: list[dict]
# - Lista que funciona como COLA FIFO (First In, First Out).
# - Cada elemento: {"producto": str, "cantidad": int, "cliente": str}
# - RECURSO COMPARTIDO entre productores (hilos de clientes) y
#   consumidores (hilos procesadores). Protegida con lock_cola.
cola_pedidos = []


# ==============================================================================
# PRIMITIVAS DE SINCRONIZACIÓN
# ==============================================================================

# lock_cola: threading.Lock
# - Protege el acceso a cola_pedidos.
# - Cualquier hilo que lea o modifique cola_pedidos DEBE adquirirlo primero.
lock_cola = threading.Lock()

# lock_stock: threading.Lock
# - Protege el acceso a stock_productos.
# - Lock separado del de la cola para mayor granularidad (paralelismo).
lock_stock = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# MOD-05: lock_estadisticas = threading.Lock()
# ─────────────────────────────────────────────────────────────────────────────
# ¿POR QUÉ un lock separado para estadísticas?
#   El diccionario `estadisticas` es un RECURSO COMPARTIDO: múltiples hilos
#   (hilos de clientes al encolar, hilos procesadores al despachar) lo
#   modifican simultáneamente. Sin exclusión mutua, podría ocurrir una
#   CONDICIÓN DE CARRERA como esta:
#
#     Hilo A lee: total_pedidos_recibidos = 7
#     Hilo B lee: total_pedidos_recibidos = 7
#     Hilo A escribe: total_pedidos_recibidos = 8  ← sobrescribe la lectura de B
#     Hilo B escribe: total_pedidos_recibidos = 8  ← ¡se perdió un incremento!
#
#   Con lock_estadisticas, solo un hilo modifica el diccionario a la vez.
#
# ¿POR QUÉ NO reusar lock_cola o lock_stock?
#   Reusar locks distintos para estadísticas, cola y stock mantiene la
#   GRANULARIDAD FINA: un hilo puede estar actualizando estadísticas mientras
#   otro modifica el stock, sin bloquearse mutuamente. Esto mejora el
#   rendimiento y evita deadlocks innecesarios.
#
# REGLA CRÍTICA DE ORDEN (evitar deadlock):
#   Si algún hilo necesita AMBOS locks (estadísticas + otro), SIEMPRE debe
#   adquirirlos en el mismo orden en todos los hilos. En este código, nunca
#   se adquieren dos locks simultáneamente: las estadísticas se actualizan
#   FUERA de las secciones críticas de cola y stock.
# ─────────────────────────────────────────────────────────────────────────────
lock_estadisticas = threading.Lock()

# semaforo_capacidad: threading.Semaphore
# - Inicializado con CAPACIDAD_MAXIMA_COLA (10). Controla los espacios libres.
# - .acquire() decrementa (bloquea si en 0). .release() incrementa.
semaforo_capacidad = threading.Semaphore(CAPACIDAD_MAXIMA_COLA)

# barrera_procesadores: threading.Barrier
# - Barrera para NUM_PROCESADORES + 1 participantes.
# - Los procesadores esperan aquí hasta que el monitor libera la barrera.
barrera_procesadores = threading.Barrier(NUM_PROCESADORES + 1)
# +1: el hilo que detecta suficientes pedidos también participa en la barrera.

# evento_barrera_liberada: threading.Event
# - Bandera que indica si la barrera ya fue liberada (True = sí).
# - Evita llamar a barrera_procesadores.wait() más de una vez.
evento_barrera_liberada = threading.Event()

# contador_pedidos_totales: int
# - Cuenta los pedidos que han entrado a la cola (para disparar la barrera).
# - Protegido con lock_cola (se modifica siempre dentro de esa sección crítica).
contador_pedidos_totales = 0

# evento_servidor_activo: threading.Event
# - Bandera de vida del servidor. .clear() señala a los procesadores que paren.
evento_servidor_activo = threading.Event()
evento_servidor_activo.set()  # El servidor empieza activo.


# ==============================================================================
# MOD-05: DICCIONARIO DE ESTADÍSTICAS
# ==============================================================================
# ─────────────────────────────────────────────────────────────────────────────
# ¿QUÉ ES estadisticas?
#   Un diccionario global que el servidor mantiene actualizado durante toda su
#   ejecución. Cada vez que ocurre un evento relevante (pedido recibido,
#   despachado, rechazado), el hilo correspondiente actualiza este dict de
#   forma thread-safe usando lock_estadisticas.
#
# ¿POR QUÉ hacerlo global?
#   Todos los hilos (operadores de clientes y procesadores) deben poder acceder
#   a las mismas estadísticas. Hacerlo global es la forma más sencilla de
#   compartirlo entre hilos que no se conocen entre sí.
#   La alternativa sería pasar el dict como parámetro a cada función, lo que
#   añade complejidad sin beneficio real.
#
# ESTRUCTURA DETALLADA:
#   "total_pedidos_recibidos"       → int: pedidos que entraron a la cola.
#   "total_pedidos_despachados"     → int: pedidos procesados con stock suficiente.
#   "total_pedidos_rechazados_stock"     → int: pedidos rechazados por falta de stock.
#   "total_pedidos_rechazados_cola_llena"→ int: pedidos rechazados porque la cola
#                                           estaba llena (semáforo agotado).
#   "pedidos_por_cliente"           → dict {nombre_cliente: cantidad_pedidos}
#                                     Permite saber qué cliente envió más pedidos.
#   "pedidos_por_producto"          → dict {nombre_producto: cantidad_pedidos}
#                                     Permite saber qué producto fue más solicitado.
#   "tiempo_inicio"                 → datetime.datetime: cuando arrancó el servidor.
#   "tiempo_fin"                    → datetime.datetime: cuando terminó el servidor.
#                                     Calculando fin - inicio obtenemos la duración.
# ─────────────────────────────────────────────────────────────────────────────
estadisticas = {
    # ── Contadores globales ──────────────────────────────────────────────────
    "total_pedidos_recibidos": 0,
    # Número total de pedidos que fueron aceptados en la cola.
    # Se incrementa en agregar_pedido_a_cola() SOLO cuando el pedido
    # entra con éxito (semáforo adquirido y cola actualizada).

    "total_pedidos_despachados": 0,
    # Número de pedidos procesados exitosamente (había stock suficiente).
    # Se incrementa en procesar_pedido() al descontar del inventario.

    "total_pedidos_rechazados_stock": 0,
    # Pedidos que llegaron a los procesadores pero no pudieron despacharse
    # porque el stock era insuficiente (producto agotado o cantidad > stock).
    # Se incrementa en procesar_pedido() cuando stock < cantidad.

    "total_pedidos_rechazados_cola_llena": 0,
    # Pedidos que no pudieron ni entrar a la cola porque el semáforo
    # estaba en 0 (la cola estaba a máxima capacidad durante 5 segundos).
    # Se incrementa en agregar_pedido_a_cola() cuando acquire() falla.

    # ── Sub-diccionarios de desglose ─────────────────────────────────────────
    "pedidos_por_cliente": {},
    # Diccionario que mapea nombre_cliente → número de pedidos recibidos.
    # Permite identificar al cliente más activo.
    # Ejemplo: {"Cliente-1": 3, "Cliente-2": 1, "Cliente-3": 5}

    "pedidos_por_producto": {},
    # Diccionario que mapea nombre_producto → número de veces solicitado.
    # Permite identificar el producto más demandado.
    # Ejemplo: {"Laptop": 4, "Mouse": 7, "Teclado": 2}

    # ── Marcas de tiempo ─────────────────────────────────────────────────────
    "tiempo_inicio": None,
    # Se asigna con datetime.datetime.now() al inicio de iniciar_servidor().
    # Comienza en None y se actualiza antes de aceptar el primer cliente.

    "tiempo_fin": None,
    # Se asigna con datetime.datetime.now() al final de iniciar_servidor(),
    # justo antes de llamar a mostrar_estadisticas().
}


# ==============================================================================
# FUNCIONES DEL SERVIDOR
# ==============================================================================


def log(mensaje):
    """
    Imprime un mensaje con marca de tiempo y nombre del hilo actual.

    Parámetros:
        mensaje (str): El texto a imprimir.

    Retorna:
        None (solo imprime en consola).

    ¿Por qué?
        - Ayuda a depurar el programa mostrando QUÉ hilo ejecutó QUÉ acción
          y a QUÉ hora.
        - threading.current_thread().name retorna el nombre del hilo que
          ejecuta esta función (ej: "Procesador-1", "Operador-Cliente-2").
        - time.strftime("%H:%M:%S") formatea la hora actual como HH:MM:SS.
    """
    # Obtener el nombre del hilo que está ejecutando esta función.
    nombre_hilo = threading.current_thread().name

    # Obtener la hora actual formateada como HH:MM:SS.
    hora_actual = time.strftime("%H:%M:%S")

    # Imprimir el mensaje con formato: [HH:MM:SS] [NombreHilo] Mensaje
    print(f"[{hora_actual}] [{nombre_hilo}] {mensaje}")


# ──────────────────────────────────────────────────────────────────────────────
# MOD-05: FUNCIÓN actualizar_estadistica()
# ──────────────────────────────────────────────────────────────────────────────
def actualizar_estadistica(clave, valor=1, sub_clave=None):
    """
    Actualiza de forma thread-safe el diccionario global `estadisticas`.

    PROPÓSITO:
        Centralizar toda la lógica de actualización de estadísticas en un único
        lugar, evitando duplicar el patrón "acquire lock → modificar → release"
        en cada punto del código donde se registra un evento.
        Esto sigue el principio DRY (Don't Repeat Yourself) y facilita el
        mantenimiento: si cambiamos la estructura de estadísticas, solo
        modificamos esta función.

    THREAD-SAFETY:
        Usa `with lock_estadisticas:` para garantizar que ningún otro hilo
        pueda leer o modificar el dict mientras este hilo lo actualiza.
        El bloque `with` es equivalente a:
            lock_estadisticas.acquire()
            try:
                # modificaciones
            finally:
                lock_estadisticas.release()
        La ventaja del `with` es que siempre suelta el lock aunque ocurra
        una excepción dentro del bloque.

    LÓGICA DE ACTUALIZACIÓN:
        - Si sub_clave es None: se interpreta que `clave` es un contador
          entero (como "total_pedidos_recibidos") y se le suma `valor`.
        - Si sub_clave no es None: se interpreta que `clave` apunta a un
          sub-diccionario (como "pedidos_por_cliente") y se suma `valor`
          al contador del sub-dict en la clave `sub_clave`.
          Si `sub_clave` no existe en el sub-dict, se inicializa en 0.

    Parámetros:
        clave (str):
            La clave en el diccionario `estadisticas` a modificar.
            Ejemplos: "total_pedidos_recibidos", "pedidos_por_cliente".

        valor (int, opcional, por defecto=1):
            La cantidad a sumar al contador. Por defecto es 1 porque la
            operación más común es incrementar en 1 (contar un evento).
            Se puede pasar un valor diferente si se necesita sumar más.

        sub_clave (str o None, opcional, por defecto=None):
            Si se proporciona, indica que `clave` apunta a un sub-dict
            y `sub_clave` es la clave dentro de ese sub-dict a incrementar.
            Si es None, se actualiza `estadisticas[clave]` directamente.
            Ejemplo: actualizar_estadistica("pedidos_por_cliente", 1, "Cliente-2")
                     → estadisticas["pedidos_por_cliente"]["Cliente-2"] += 1

    Retorna:
        None (modifica el diccionario global en su lugar).

    EJEMPLOS DE USO:
        # Incrementar un contador simple en 1:
        actualizar_estadistica("total_pedidos_recibidos")

        # Incrementar el contador de un cliente específico:
        actualizar_estadistica("pedidos_por_cliente", sub_clave="Cliente-1")

        # Incrementar el contador de un producto en 2:
        actualizar_estadistica("pedidos_por_producto", valor=2, sub_clave="Laptop")
    """
    # Adquirir el lock de estadísticas antes de cualquier modificación.
    # 'with' garantiza la liberación automática incluso ante excepciones.
    with lock_estadisticas:

        if sub_clave is None:
            # ── Caso 1: Actualizar un contador entero simple ──────────────────
            # estadisticas[clave] es un int (ej: total_pedidos_recibidos).
            # Le sumamos `valor` (por defecto 1).
            # El operador += es equivalente a estadisticas[clave] = estadisticas[clave] + valor
            estadisticas[clave] += valor

        else:
            # ── Caso 2: Actualizar un sub-diccionario ─────────────────────────
            # estadisticas[clave] es un dict (ej: pedidos_por_cliente).
            # Queremos incrementar estadisticas[clave][sub_clave] en `valor`.

            # .get(sub_clave, 0): si sub_clave no existe en el sub-dict,
            # retorna 0 como valor por defecto en lugar de lanzar KeyError.
            # Esto permite inicializar automáticamente el contador la primera
            # vez que se ve un cliente o producto nuevo.
            estadisticas[clave][sub_clave] = (
                estadisticas[clave].get(sub_clave, 0) + valor
            )


def mostrar_stock():
    """
    Muestra el inventario actual de productos en consola.

    No recibe parámetros.

    Retorna:
        None (solo imprime en consola).

    ¿Por qué usamos lock_stock?
        - Para leer el diccionario de forma segura (otro hilo podría estar
          modificándolo al mismo tiempo).
        - 'with lock_stock:' garantiza que el lock se suelta siempre,
          incluso si ocurre un error dentro del bloque.
    """
    with lock_stock:
        print("\n" + "=" * 50)
        print("        STOCK ACTUAL DE PRODUCTOS")
        print("=" * 50)

        # .items() retorna pares (clave, valor) = (nombre_producto, cantidad).
        for producto, cantidad in stock_productos.items():
            # {producto:<15} alinea a la izquierda en 15 caracteres (tabla visual).
            print(f"  {producto:<15} → {cantidad} unidades")

        print("=" * 50 + "\n")


def obtener_lista_productos():
    """
    Retorna la lista de nombres de productos disponibles en el stock.

    No recibe parámetros.

    Retorna:
        list[str]: Lista con los nombres de los productos.
        Ejemplo: ["Laptop", "Mouse", "Teclado", ...]

    Nota: Se usa lock_stock para leer el diccionario de forma segura.
    list(keys()) crea una COPIA, así el lock se suelta rápido.
    """
    with lock_stock:
        return list(stock_productos.keys())


def agregar_pedido_a_cola(pedido):
    """
    Agrega un pedido a la cola compartida de forma segura (PRODUCTOR).

    Parámetros:
        pedido (dict): {
            "producto": str,   # Nombre del producto (ej: "Laptop")
            "cantidad": int,   # Cantidad solicitada (ej: 2)
            "cliente": str     # Identificador del cliente (ej: "Cliente-1")
        }

    Retorna:
        bool: True si el pedido fue agregado exitosamente,
              False si la cola estaba llena (timeout del semáforo).

    MOD-05 — ESTADÍSTICAS QUE SE ACTUALIZAN AQUÍ:
    ─────────────────────────────────────────────────────────────────────────
    Punto 1 (FALLO - cola llena):
        Si semaforo_capacidad.acquire() retorna False (cola llena después de
        5 segundos), se llama:
            actualizar_estadistica("total_pedidos_rechazados_cola_llena")
        Esto registra que hubo un pedido que no pudo ni entrar a la cola.
        Es un tipo de rechazo diferente al rechazo por falta de stock:
        aquí el pedido NI SIQUIERA llega a los procesadores.

    Punto 2 (ÉXITO - pedido encolado):
        Si el pedido entra a la cola con éxito, se llama:
            actualizar_estadistica("total_pedidos_recibidos")
            actualizar_estadistica("pedidos_por_cliente", sub_clave=nombre_cliente)
        La segunda llamada permite saber cuántos pedidos hizo cada cliente.
        Se hace DENTRO del bloque `with lock_cola` para mantener la
        consistencia: si el pedido entró a la cola, sus estadísticas se
        registran. No puede haber un pedido en la cola sin su estadística.
    ─────────────────────────────────────────────────────────────────────────

    Proceso completo:
        1. Intenta adquirir el SEMÁFORO (verifica espacio en la cola).
        2. Si hay espacio: adquiere LOCK de cola, agrega pedido, actualiza stats.
        3. Si no hay espacio: actualiza estadística de rechazo y retorna False.
        4. Verifica si se alcanzó el umbral de la barrera.
    """
    global contador_pedidos_totales
    # 'global' es necesario para poder MODIFICAR (no solo leer) la variable
    # contador_pedidos_totales, que está definida en el ámbito del módulo.

    # ── PASO 1: Intentar adquirir el semáforo ─────────────────────────────────
    # blocking=True: si el semáforo está en 0, el hilo se bloquea y espera.
    # timeout=5: si pasaron 5 segundos y no se liberó espacio, retorna False.
    espacio_disponible = semaforo_capacidad.acquire(blocking=True, timeout=5)

    if not espacio_disponible:
        # La cola está llena y no se liberó espacio en 5 segundos.
        log(f"⚠ Cola llena. No se pudo agregar pedido de {pedido['cliente']}: "
            f"{pedido['cantidad']}x {pedido['producto']}")

        # ── MOD-05: Registrar rechazo por cola llena ──────────────────────────
        # Llamamos a actualizar_estadistica() FUERA de cualquier otro lock,
        # lo que cumple con la regla de no anidar locks (evita deadlock).
        actualizar_estadistica("total_pedidos_rechazados_cola_llena")
        # ──────────────────────────────────────────────────────────────────────

        return False  # Informar al llamador que el pedido no se pudo agregar.

    # ── PASO 2: Adquirir el lock de la cola y agregar el pedido ───────────────
    with lock_cola:
        # append() agrega el elemento al FINAL de la lista (comportamiento FIFO).
        cola_pedidos.append(pedido)

        # Incrementar el contador para el umbral de la barrera.
        contador_pedidos_totales += 1
        total_actual = contador_pedidos_totales  # Copia local para usar fuera.

        log(f"+ Pedido agregado a la cola: {pedido['cantidad']}x {pedido['producto']} "
            f"(de {pedido['cliente']}). "
            f"Cola: {len(cola_pedidos)}/{CAPACIDAD_MAXIMA_COLA} | "
            f"Total histórico: {total_actual}")

    # ── MOD-05: Actualizar estadísticas de recepción exitosa ──────────────────
    # Se hace FUERA de lock_cola para minimizar el tiempo con ese lock adquirido.
    # Como lock_estadisticas y lock_cola son independientes, esto es seguro.
    #
    # ¿Por qué actualizar_estadistica("total_pedidos_recibidos") y no en
    # procesar_pedido()? Porque "recibido" significa "aceptado en la cola",
    # independientemente de si luego puede despacharse. Si el stock falla,
    # el pedido fue recibido pero no despachado. Ambos contadores son distintos.
    actualizar_estadistica("total_pedidos_recibidos")

    # Registrar el pedido bajo el cliente que lo hizo.
    # pedido["cliente"] es el nombre del cliente, ej: "Cliente-2".
    # sub_clave=pedido["cliente"] inserta o incrementa en pedidos_por_cliente.
    actualizar_estadistica("pedidos_por_cliente", sub_clave=pedido["cliente"])
    # ──────────────────────────────────────────────────────────────────────────

    # ── PASO 3: Verificar umbral de la barrera ────────────────────────────────
    # Si ya hay suficientes pedidos Y la barrera no fue liberada antes:
    if (total_actual >= PEDIDOS_MINIMOS_PARA_BARRERA
            and not evento_barrera_liberada.is_set()):
        log(f"★ Se alcanzaron {PEDIDOS_MINIMOS_PARA_BARRERA} pedidos. "
            f"Notificando a la barrera para liberar procesadores...")
        try:
            barrera_procesadores.wait()
        except threading.BrokenBarrierError:
            log("⚠ Error en la barrera, pero se continuará el procesamiento.")
        evento_barrera_liberada.set()  # Marcar que la barrera ya fue liberada.

    return True  # Pedido agregado exitosamente.


def retirar_pedido_de_cola():
    """
    Retira y retorna el primer pedido de la cola compartida (FIFO) (CONSUMIDOR).

    No recibe parámetros.

    Retorna:
        dict o None:
            - dict con la información del pedido si la cola tenía pedidos.
            - None si la cola estaba vacía.

    Proceso:
        1. Adquiere el LOCK de la cola.
        2. Si hay pedidos, retira el PRIMERO (pop(0) = FIFO).
        3. Libera un espacio en el SEMÁFORO (permite a un productor entrar).
        4. Retorna el pedido (o None si estaba vacía).
    """
    with lock_cola:
        if len(cola_pedidos) > 0:
            # pop(0) retira y retorna el PRIMER elemento (FIFO).
            pedido = cola_pedidos.pop(0)

            log(f"- Pedido retirado de la cola: {pedido['cantidad']}x "
                f"{pedido['producto']} (de {pedido['cliente']}). "
                f"Cola restante: {len(cola_pedidos)}")

            # release() incrementa el semáforo en 1, liberando un espacio.
            # Si algún productor estaba bloqueado esperando espacio, se desbloquea.
            semaforo_capacidad.release()

            return pedido
        else:
            return None


def procesar_pedido(pedido):
    """
    Procesa (despacha) un pedido: verifica stock, descuenta y simula tiempo.

    Este es el trabajo principal de cada CONSUMIDOR (hilo procesador).

    Parámetros:
        pedido (dict): {
            "producto": str,   # Nombre del producto
            "cantidad": int,   # Cantidad solicitada
            "cliente": str     # Quién hizo el pedido
        }

    Retorna:
        None (procesa el pedido internamente y actualiza estadísticas).

    MOD-05 — ESTADÍSTICAS QUE SE ACTUALIZAN AQUÍ:
    ─────────────────────────────────────────────────────────────────────────
    Escenario A (stock suficiente → DESPACHADO):
        actualizar_estadistica("total_pedidos_despachados")
        actualizar_estadistica("pedidos_por_producto", sub_clave=producto)

        "total_pedidos_despachados" confirma que el pedido fue atendido.
        "pedidos_por_producto" permite saber qué productos se mueven más.
        Nótese que registramos en pedidos_por_producto solo cuando hay ÉXITO.
        Si quisiéramos contar también los intentos fallidos, haría falta
        otro sub-dict "intentos_por_producto".

    Escenario B (stock insuficiente → RECHAZADO):
        actualizar_estadistica("total_pedidos_rechazados_stock")

        Este contador es diferente a "rechazados_cola_llena":
        aquí el pedido SÍ entró a la cola y SÍ llegó a un procesador,
        pero no se pudo despachar por falta de inventario.

    AMBAS actualizaciones se hacen FUERA del bloque `with lock_stock`,
    siguiendo la regla de nunca anidar lock_estadisticas dentro de lock_stock.
    ─────────────────────────────────────────────────────────────────────────
    """
    # Extraer los datos del pedido.
    producto = pedido["producto"]
    cantidad = pedido["cantidad"]
    cliente = pedido["cliente"]

    # Simular tiempo de procesamiento (1-5 segundos, fuera del lock).
    # La espera se hace FUERA del lock para no bloquear a otros hilos
    # que también necesitan acceder al stock.
    tiempo_procesamiento = random.randint(1, 5)
    log(f"⏳ Procesando pedido de {cliente}: {cantidad}x {producto} "
        f"(tardará {tiempo_procesamiento}s)...")
    time.sleep(tiempo_procesamiento)

    # ── Verificar y descontar stock ────────────────────────────────────────────
    # Variable que capturará el resultado para usarla FUERA del lock.
    resultado_despacho = None  # "despachado" | "rechazado_stock" | "rechazado_no_existe"

    with lock_stock:
        if producto in stock_productos:
            if stock_productos[producto] >= cantidad:
                # Hay stock suficiente → descontar y marcar como despachado.
                stock_productos[producto] -= cantidad
                log(f"✓ DESPACHADO: {cantidad}x {producto} para {cliente}. "
                    f"Stock restante de {producto}: {stock_productos[producto]}")
                resultado_despacho = "despachado"
            else:
                # No hay suficiente stock.
                disponible = stock_productos[producto]
                log(f"✗ RECHAZADO: {cantidad}x {producto} para {cliente}. "
                    f"Stock insuficiente (disponible: {disponible})")
                resultado_despacho = "rechazado_stock"
        else:
            log(f"✗ RECHAZADO: Producto '{producto}' no existe en el inventario. "
                f"Pedido de {cliente}.")
            resultado_despacho = "rechazado_no_existe"

    # ── MOD-05: Actualizar estadísticas FUERA del lock_stock ─────────────────
    # Separar la actualización de estadísticas del bloque `with lock_stock`
    # garantiza que lock_stock se libera lo antes posible (menor contención).
    # Otros hilos pueden acceder al stock mientras este actualiza estadísticas.
    if resultado_despacho == "despachado":
        actualizar_estadistica("total_pedidos_despachados")
        # Registrar el producto despachado para el ranking de productos.
        actualizar_estadistica("pedidos_por_producto", sub_clave=producto)

    elif resultado_despacho in ("rechazado_stock", "rechazado_no_existe"):
        # Ambos casos son rechazos por falta de stock (ya sea porque no hay
        # unidades suficientes o porque el producto ni siquiera existe).
        actualizar_estadistica("total_pedidos_rechazados_stock")
    # ──────────────────────────────────────────────────────────────────────────


def hilo_procesador(id_procesador):
    """
    Función que ejecuta cada hilo procesador (CONSUMIDOR).

    Cada procesador es un hilo que corre en un bucle infinito:
    1. Espera en la BARRERA hasta que haya suficientes pedidos.
    2. Toma pedidos de la cola y los procesa (despacha).
    3. Si la cola está vacía, espera un segundo y vuelve a intentar.
    4. Termina cuando el servidor se apaga y la cola está vacía.

    Parámetros:
        id_procesador (int): Número identificador del procesador (1, 2, 3...).

    Retorna:
        None (ejecuta un bucle hasta que el servidor se apague).
    """
    log(f"Procesador-{id_procesador} iniciado. Esperando en la barrera...")

    # Esperar en la barrera hasta que se acumulen suficientes pedidos.
    try:
        barrera_procesadores.wait()
    except threading.BrokenBarrierError:
        log(f"Procesador-{id_procesador}: Barrera rota, continuando...")

    log(f"Procesador-{id_procesador} desbloqueado. ¡Comenzando a procesar pedidos!")

    # Bucle principal: mientras el servidor esté activo O queden pedidos.
    while evento_servidor_activo.is_set() or len(cola_pedidos) > 0:
        pedido = retirar_pedido_de_cola()

        if pedido is not None:
            procesar_pedido(pedido)
        else:
            if not evento_servidor_activo.is_set():
                break
            # Espera activa reducida: 1 segundo antes de volver a revisar la cola.
            time.sleep(1)

    log(f"Procesador-{id_procesador} finalizado.")


def atender_cliente(conexion_cliente, direccion_cliente, id_cliente):
    """
    Función que ejecuta cada hilo operador (PRODUCTOR) para atender a un cliente.

    Cada cliente que se conecta es atendido por un hilo independiente.
    Este hilo:
    1. Envía la lista de productos disponibles al cliente.
    2. Recibe pedidos del cliente (en formato JSON).
    3. Agrega cada pedido a la cola compartida.
    4. Envía confirmaciones/rechazos al cliente.
    5. Cierra la conexión cuando el cliente termina.

    Parámetros:
        conexion_cliente (socket.socket): El socket de conexión con este cliente.
        direccion_cliente (tuple): (ip, puerto) del cliente.
        id_cliente (int): Número secuencial del cliente (1, 2, 3...).

    Retorna:
        None (el hilo se ejecuta hasta que el cliente se desconecte).

    MOD-05: Las estadísticas en esta función se actualizan DENTRO de
    agregar_pedido_a_cola(), no directamente aquí. Esto mantiene la
    responsabilidad de actualizar estadísticas en las funciones que
    realmente realizan las operaciones sobre la cola.
    """
    nombre_cliente = f"Cliente-{id_cliente}"
    log(f"Conexión aceptada de {nombre_cliente} "
        f"({direccion_cliente[0]}:{direccion_cliente[1]})")

    try:
        # ── PASO 1: Enviar lista de productos al cliente ──────────────────────
        lista_productos = obtener_lista_productos()
        mensaje_bienvenida = {
            "tipo": "bienvenida",
            "mensaje": f"Bienvenido {nombre_cliente} a la Central de Pedidos",
            "productos_disponibles": lista_productos,
            "tu_id": nombre_cliente
        }
        conexion_cliente.sendall(json.dumps(mensaje_bienvenida).encode(ENCODING))

        # ── PASO 2: Recibir pedidos en un bucle ───────────────────────────────
        while True:
            # recv() espera hasta recibir datos (BLOQUEANTE).
            datos_recibidos = conexion_cliente.recv(BUFFER_SIZE)

            # bytes vacíos → el cliente cerró la conexión normalmente.
            if not datos_recibidos:
                log(f"{nombre_cliente} se desconectó.")
                break

            mensaje_texto = datos_recibidos.decode(ENCODING)

            try:
                pedido_datos = json.loads(mensaje_texto)

                # Señal de FIN: el cliente no enviará más pedidos.
                if pedido_datos.get("tipo") == "fin":
                    log(f"{nombre_cliente} ha terminado de enviar pedidos.")
                    respuesta_fin = {
                        "tipo": "fin_confirmado",
                        "mensaje": "Todos tus pedidos fueron recibidos. ¡Gracias!"
                    }
                    conexion_cliente.sendall(
                        json.dumps(respuesta_fin).encode(ENCODING)
                    )
                    break

                # ── PASO 3: Construir y encolar el pedido ─────────────────────
                pedido = {
                    "producto": pedido_datos["producto"],
                    "cantidad": pedido_datos["cantidad"],
                    "cliente": nombre_cliente
                }

                log(f"Pedido recibido de {nombre_cliente}: "
                    f"{pedido['cantidad']}x {pedido['producto']}")

                # agregar_pedido_a_cola() actualiza internamente las estadísticas.
                exito = agregar_pedido_a_cola(pedido)

                # ── PASO 4: Enviar respuesta al cliente ───────────────────────
                if exito:
                    respuesta = {
                        "tipo": "confirmacion",
                        "mensaje": (f"Pedido recibido: {pedido['cantidad']}x "
                                    f"{pedido['producto']}. En cola de procesamiento."),
                        "estado": "en_cola"
                    }
                else:
                    respuesta = {
                        "tipo": "error",
                        "mensaje": (f"Cola llena. No se pudo aceptar el pedido de "
                                    f"{pedido['cantidad']}x {pedido['producto']}."),
                        "estado": "rechazado"
                    }

                conexion_cliente.sendall(
                    json.dumps(respuesta).encode(ENCODING)
                )

            except json.JSONDecodeError:
                log(f"⚠ Mensaje inválido de {nombre_cliente}: {mensaje_texto}")
                error_msg = {
                    "tipo": "error",
                    "mensaje": "Formato de mensaje inválido. Use JSON."
                }
                conexion_cliente.sendall(
                    json.dumps(error_msg).encode(ENCODING)
                )

            except KeyError as e:
                log(f"⚠ Pedido incompleto de {nombre_cliente}. Falta campo: {e}")
                error_msg = {
                    "tipo": "error",
                    "mensaje": f"Pedido incompleto. Falta el campo: {e}"
                }
                conexion_cliente.sendall(
                    json.dumps(error_msg).encode(ENCODING)
                )

    except ConnectionResetError:
        log(f"⚠ {nombre_cliente} cerró la conexión inesperadamente.")

    except Exception as e:
        log(f"⚠ Error atendiendo a {nombre_cliente}: {e}")

    finally:
        # SIEMPRE cerrar el socket del cliente para liberar recursos del SO.
        conexion_cliente.close()
        log(f"Conexión con {nombre_cliente} cerrada.")


# ──────────────────────────────────────────────────────────────────────────────
# MOD-05: FUNCIÓN mostrar_estadisticas()
# ──────────────────────────────────────────────────────────────────────────────
def mostrar_estadisticas():
    """
    Imprime en consola un resumen formateado de todas las estadísticas
    recopiladas durante la ejecución del servidor.

    No recibe parámetros (lee directamente el dict global `estadisticas`).

    Retorna:
        None (solo imprime en consola).

    CUÁNDO SE LLAMA:
        Al final de iniciar_servidor(), justo antes de cerrar el socket.
        En ese punto, todos los hilos procesadores ya terminaron, así que
        no hay riesgo de que alguien modifique `estadisticas` mientras
        estamos leyéndolo. Sin embargo, usamos lock_estadisticas por buena
        práctica (por si alguien modifica el código en el futuro y llama
        a mostrar_estadisticas() antes de que todos los hilos terminen).

    POR QUÉ UN REPORTE FORMATEADO:
        Los datos brutos de `estadisticas` son accesibles pero no legibles.
        Un reporte formateado permite:
            1. Ver de un vistazo el desempeño de la sesión.
            2. Identificar cuellos de botella (muchos rechazos por cola llena
               → aumentar CAPACIDAD_MAXIMA_COLA o NUM_PROCESADORES).
            3. Ver qué productos tienen más demanda.
            4. Ver qué cliente generó más carga.
            5. Calcular la duración total de la sesión.

    CÁLCULO DE DURACIÓN:
        Si tiempo_inicio y tiempo_fin están disponibles:
            duracion = tiempo_fin - tiempo_inicio
            → retorna un datetime.timedelta.
            str(timedelta) lo convierte a "H:MM:SS.ffffff".
        Si alguno es None (ej: servidor interrumpido antes de registrar fin),
        se muestra "N/D" (no disponible) para evitar errores.

    RANKING DE CLIENTE/PRODUCTO MÁS ACTIVO:
        max(dict, key=dict.get) retorna la CLAVE con el mayor VALOR.
        Ejemplo: max({"A": 3, "B": 7, "C": 1}, key={"A":3,"B":7,"C":1}.get)
                 → "B" (porque 7 es el máximo)
        Si el diccionario está vacío (ningún pedido registrado), se usa
        el valor por defecto "N/A".
    """
    # Adquirir el lock por buena práctica (aunque en este punto ya no haya
    # hilos modificando estadísticas).
    with lock_estadisticas:
        # Hacer una copia local para minimizar el tiempo con el lock adquirido
        # y poder formatear el reporte sin mantener el lock innecesariamente.
        stats = dict(estadisticas)

    # ── Calcular duración total ────────────────────────────────────────────────
    if stats["tiempo_inicio"] is not None and stats["tiempo_fin"] is not None:
        # La resta de dos objetos datetime produce un timedelta.
        duracion = stats["tiempo_fin"] - stats["tiempo_inicio"]
        # str(timedelta) produce algo como "0:02:35.123456" (horas:min:seg).
        duracion_str = str(duracion)
        # Formato más legible para el inicio y fin:
        inicio_str = stats["tiempo_inicio"].strftime("%Y-%m-%d %H:%M:%S")
        fin_str = stats["tiempo_fin"].strftime("%Y-%m-%d %H:%M:%S")
    else:
        # Si el servidor no terminó correctamente, no tendremos ambas marcas.
        duracion_str = "N/D"
        inicio_str = "N/D"
        fin_str = "N/D"

    # ── Calcular totales para verificación de consistencia ────────────────────
    # La suma de despachados + rechazados_stock debe ser igual a recibidos
    # (todo pedido que entra a la cola debe tener un resultado).
    total_procesados = (stats["total_pedidos_despachados"] +
                        stats["total_pedidos_rechazados_stock"])

    # ── Encontrar cliente más activo ──────────────────────────────────────────
    pedidos_cliente = stats["pedidos_por_cliente"]
    if pedidos_cliente:
        # max() con key=pedidos_cliente.get busca la clave con mayor valor.
        cliente_top = max(pedidos_cliente, key=pedidos_cliente.get)
        cliente_top_str = f"{cliente_top} ({pedidos_cliente[cliente_top]} pedidos)"
    else:
        cliente_top_str = "N/A"

    # ── Encontrar producto más solicitado ─────────────────────────────────────
    pedidos_producto = stats["pedidos_por_producto"]
    if pedidos_producto:
        producto_top = max(pedidos_producto, key=pedidos_producto.get)
        producto_top_str = f"{producto_top} ({pedidos_producto[producto_top]} veces)"
    else:
        producto_top_str = "N/A"

    # ── Imprimir el reporte ───────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("   📊  ESTADÍSTICAS FINALES DEL SERVIDOR  📊")
    print("=" * 65)

    # ── Sección: Tiempo ────────────────────────────────────────────────────────
    print("\n  ⏱  TIEMPO DE EJECUCIÓN:")
    print(f"     Inicio   : {inicio_str}")
    print(f"     Fin      : {fin_str}")
    print(f"     Duración : {duracion_str}")

    # ── Sección: Contadores globales ──────────────────────────────────────────
    print("\n  📦  PEDIDOS GLOBALES:")
    print(f"     Total recibidos en cola   : {stats['total_pedidos_recibidos']}")
    print(f"     Total despachados (éxito) : {stats['total_pedidos_despachados']}")
    print(f"     Rechazados (sin stock)    : {stats['total_pedidos_rechazados_stock']}")
    print(f"     Rechazados (cola llena)   : {stats['total_pedidos_rechazados_cola_llena']}")
    print(f"     ─────────────────────────────────────────────────")
    print(f"     Total procesados por hilos: {total_procesados}")
    # Nota: total_procesados == total_recibidos cuando todo va bien.
    # Si difieren, hubo pedidos en la cola que no alcanzaron a procesarse.

    # ── Sección: Desglose por cliente ─────────────────────────────────────────
    print("\n  👤  PEDIDOS POR CLIENTE:")
    if pedidos_cliente:
        # Ordenar por cantidad descendente para un ranking visual.
        # sorted() retorna una lista de tuplas (clave, valor) ordenadas.
        # key=lambda x: x[1] ordena por el valor (cantidad).
        # reverse=True → de mayor a menor.
        for cliente, cantidad in sorted(pedidos_cliente.items(),
                                        key=lambda x: x[1],
                                        reverse=True):
            print(f"     {cliente:<20} → {cantidad} pedido(s)")
        print(f"     ─────────────────────────────────────────────────")
        print(f"     Cliente más activo: {cliente_top_str}")
    else:
        print("     (ningún pedido registrado)")

    # ── Sección: Desglose por producto ────────────────────────────────────────
    print("\n  🛒  PEDIDOS DESPACHADOS POR PRODUCTO:")
    if pedidos_producto:
        for producto, cantidad in sorted(pedidos_producto.items(),
                                         key=lambda x: x[1],
                                         reverse=True):
            print(f"     {producto:<20} → {cantidad} vez/veces despachado(s)")
        print(f"     ─────────────────────────────────────────────────")
        print(f"     Producto más demandado: {producto_top_str}")
    else:
        print("     (ningún despacho exitoso registrado)")

    print("\n" + "=" * 65 + "\n")


def iniciar_servidor():
    """
    Función principal que arranca el servidor.

    No recibe parámetros.

    Retorna:
        None (ejecuta el servidor hasta que se cierra con Ctrl+C o se
        alcanza el límite de clientes).

    MOD-05 — CAMBIOS EN ESTA FUNCIÓN:
    ─────────────────────────────────────────────────────────────────────────
    1. Al inicio, se registra estadisticas["tiempo_inicio"] con
       datetime.datetime.now() para capturar cuándo comenzó la sesión.

    2. Al final (en el bloque finally), se registra estadisticas["tiempo_fin"]
       para saber cuándo terminó.

    3. Se llama a mostrar_estadisticas() como último paso antes de retornar,
       para imprimir el reporte completo.

    ¿Por qué registrar tiempo_inicio ANTES de aceptar clientes y tiempo_fin
    DESPUÉS de que todos los procesadores terminan?
        Queremos medir el tiempo TOTAL de la sesión del servidor, desde que
        estuvo listo para recibir conexiones hasta que terminó de procesar
        todo. Si registráramos el inicio al crear el socket y el fin al
        cerrar el socket, perderíamos el tiempo de procesamiento restante.
    ─────────────────────────────────────────────────────────────────────────
    """
    # Mostrar encabezado inicial.
    print("\n" + "=" * 60)
    print("   CENTRAL DE PEDIDOS - SERVIDOR")
    print("   MODIFICACIÓN 5: ESTADÍSTICAS AL FINALIZAR")
    print("=" * 60)
    mostrar_stock()

    # ── MOD-05: Registrar tiempo de inicio ────────────────────────────────────
    # datetime.datetime.now() captura el instante actual como objeto datetime.
    # Lo guardamos en el dict de estadísticas (no necesitamos lock aquí
    # porque ningún hilo ha arrancado todavía).
    estadisticas["tiempo_inicio"] = datetime.datetime.now()
    log(f"Servidor iniciado. Tiempo de inicio: "
        f"{estadisticas['tiempo_inicio'].strftime('%Y-%m-%d %H:%M:%S')}")
    # ──────────────────────────────────────────────────────────────────────────

    # ── PASO 1: Crear e iniciar los hilos procesadores (CONSUMIDORES) ─────────
    hilos_procesadores = []
    for i in range(1, NUM_PROCESADORES + 1):
        hilo = threading.Thread(
            target=hilo_procesador,
            args=(i,),
            name=f"Procesador-{i}",
            daemon=True
            # daemon=True: se cierran automáticamente si el hilo principal termina.
        )
        hilo.start()
        hilos_procesadores.append(hilo)

    log(f"{NUM_PROCESADORES} procesadores iniciados (esperando en barrera).")

    # ── PASO 2: Crear el socket del servidor ──────────────────────────────────
    servidor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # SO_REUSEADDR: permite reutilizar el puerto inmediatamente al reiniciar.
    servidor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    servidor_socket.bind((HOST, PORT))
    servidor_socket.listen(MAX_CLIENTES)

    log(f"Servidor escuchando en {HOST}:{PORT}")
    log(f"Esperando hasta {MAX_CLIENTES} clientes...")

    # ── PASO 3: Aceptar clientes en un bucle ──────────────────────────────────
    hilos_clientes = []
    clientes_conectados = 0

    try:
        while clientes_conectados < MAX_CLIENTES:
            # accept() es BLOQUEANTE: espera hasta que un cliente se conecte.
            conexion, direccion = servidor_socket.accept()
            clientes_conectados += 1

            hilo_cliente = threading.Thread(
                target=atender_cliente,
                args=(conexion, direccion, clientes_conectados),
                name=f"Operador-Cliente-{clientes_conectados}",
                daemon=True
            )
            hilo_cliente.start()
            hilos_clientes.append(hilo_cliente)

            log(f"Cliente {clientes_conectados}/{MAX_CLIENTES} conectado. "
                f"Hilo operador creado.")

    except KeyboardInterrupt:
        log("\n⚠ Servidor interrumpido por el usuario (Ctrl+C).")

    # ── PASO 4: Esperar a que todos los clientes terminen ─────────────────────
    log("Esperando a que todos los clientes terminen...")
    for hilo in hilos_clientes:
        hilo.join()

    log("Todos los clientes se han desconectado.")

    # ── PASO 5: Señalar a los procesadores que paren ──────────────────────────
    evento_servidor_activo.clear()  # Pone la bandera en False.

    if not evento_barrera_liberada.is_set():
        log("⚠ No se alcanzaron suficientes pedidos para la barrera. Abortando...")
        barrera_procesadores.abort()

    log("Esperando a que los procesadores terminen los pedidos restantes...")
    for hilo in hilos_procesadores:
        hilo.join(timeout=30)

    # ── PASO 6: Cerrar el socket del servidor ─────────────────────────────────
    servidor_socket.close()

    # ── MOD-05: Registrar tiempo de fin ───────────────────────────────────────
    # Se registra DESPUÉS de que los procesadores terminaron, así la duración
    # incluye el tiempo completo de procesamiento.
    estadisticas["tiempo_fin"] = datetime.datetime.now()
    log(f"Tiempo de fin registrado: "
        f"{estadisticas['tiempo_fin'].strftime('%Y-%m-%d %H:%M:%S')}")
    # ──────────────────────────────────────────────────────────────────────────

    # ── PASO 7: Mostrar stock final ───────────────────────────────────────────
    print("\n" + "=" * 60)
    print("   STOCK FINAL DESPUÉS DEL PROCESAMIENTO")
    print("=" * 60)
    mostrar_stock()

    # ── MOD-05: Mostrar estadísticas finales ──────────────────────────────────
    # Este es el paso nuevo y central de la Modificación 5.
    # Se llama al final, cuando todos los hilos terminaron y no hay más
    # actualizaciones al diccionario estadisticas.
    mostrar_estadisticas()
    # ──────────────────────────────────────────────────────────────────────────

    log("Servidor cerrado correctamente.")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

# if __name__ == "__main__":
# - Esta condición verifica si este archivo se está ejecutando directamente
#   (python servidor.py) o si está siendo importado desde otro archivo.
# - __name__ == "__main__" → True solo si se ejecuta directamente.
# - Permite que este archivo sea importado sin iniciar el servidor.
if __name__ == "__main__":
    iniciar_servidor()
