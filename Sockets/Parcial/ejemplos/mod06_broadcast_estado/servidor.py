"""
================================================================================
SERVIDOR - CENTRAL DE PEDIDOS CON PRODUCTORES, CONSUMIDORES Y BROADCAST
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
                         y a la LISTA DE CLIENTES ACTIVOS (novedad Mod 6).
    6. BARRERA         → los procesadores esperan a que haya al menos N pedidos
                         acumulados antes de empezar a despachar.

================================================================================
MODIFICACIÓN 6 — BROADCAST DEL ESTADO DE LA COLA A TODOS LOS CLIENTES
================================================================================

CAMBIOS RESPECTO A LA VERSIÓN BASE:

  1. LISTA clientes_activos (nueva):
     - Es una lista global que almacena todos los sockets de clientes
       que están ACTUALMENTE CONECTADOS al servidor.
     - Se usa para enviarles mensajes a TODOS a la vez (broadcast).

  2. LOCK lock_clientes (nuevo):
     - Protege la lista clientes_activos de condiciones de carrera.
     - Varios hilos (uno por cliente) podrían modificar la lista
       simultáneamente → se necesita exclusión mutua.

  3. REGISTRO de cliente al conectar:
     - Dentro de atender_cliente(), en cuanto el cliente se conecta
       y se prepara para recibir pedidos, su socket se agrega a
       clientes_activos usando lock_clientes.

  4. BAJA del registro al desconectar:
     - En el bloque finally de atender_cliente(), el socket del cliente
       se elimina de clientes_activos de forma segura (con lock_clientes).
     - Así, futuros broadcasts no intentan enviar a sockets cerrados.

  5. FUNCIÓN broadcast(mensaje) (nueva):
     - Itera sobre una COPIA de clientes_activos (para no mantener el
       lock mientras envía datos, lo cual podría causar deadlocks).
     - Envía el mensaje JSON a cada cliente activo.
     - Si un cliente ya cerró su socket, captura el error silenciosamente.

  6. BROADCAST al despachar (cambio en procesar_pedido):
     - Cuando un pedido es DESPACHADO exitosamente, procesar_pedido()
       llama a broadcast() con un mensaje de tipo "broadcast".
     - Todos los clientes conectados recibirán la notificación del despacho.

FLUJO GENERAL (igual que base, con adiciones marcadas con [MOD6]):
    1. El servidor arranca y muestra el stock inicial de productos.
    2. Lanza HILOS PROCESADORES que quedarán bloqueados en la BARRERA.
    3. Abre un socket TCP y escucha conexiones entrantes.
    4. Por cada cliente que se conecta, crea un HILO OPERADOR que:
       a. Agrega el socket a clientes_activos.          [MOD6]
       b. Recibe los pedidos del cliente (producto + cantidad).
       c. Valida que haya espacio en la cola (SEMÁFORO).
       d. Agrega el pedido a la cola compartida (protegida por LOCK).
       e. Al desconectarse, elimina el socket de clientes_activos.  [MOD6]
    5. Cuando se acumulan suficientes pedidos (BARRERA), los procesadores
       se desbloquean y comienzan a despachar los pedidos de la cola.
    6. Cada procesador toma un pedido, descuenta del stock (LOCK) y tarda
       un tiempo aleatorio (1-5 segundos) simulando el despacho.
    7. Al despachar, hace BROADCAST a todos los clientes conectados.  [MOD6]

EJECUCIÓN:
    python servidor.py

CONCEPTOS CLAVE:
    - PRODUCTOR: El hilo que atiende a cada cliente. "Produce" pedidos
      y los coloca en la cola.
    - CONSUMIDOR: El hilo procesador. "Consume" pedidos de la cola y
      los despacha.
    - SECCIÓN CRÍTICA: Cualquier bloque de código donde se accede a un
      recurso compartido (cola, stock, clientes_activos). Debe protegerse.
    - BROADCAST: Envío de un mensaje a TODOS los receptores conectados
      en ese momento. Se usa para informar el estado del sistema en tiempo real.
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
#       → Crea un hilo que ejecutará la función 'funcion' con los argumentos dados.
#       → .start() lo inicia, .join() espera a que termine.
#     * Lock()
#       → Candado de exclusión mutua (mutex). Solo UN hilo puede tenerlo a la vez.
#       → .acquire() lo toma (bloquea si otro lo tiene), .release() lo suelta.
#     * Semaphore(n)
#       → Contador protegido inicializado en n. Permite hasta n accesos simultáneos.
#       → .acquire() decrementa el contador (bloquea si es 0).
#       → .release() incrementa el contador (desbloquea a un hilo en espera).
#     * Barrier(n)
#       → Punto de sincronización. Bloquea hilos hasta que n hilos llamen a .wait().
#       → Cuando el n-ésimo hilo llama a .wait(), TODOS se desbloquean.
#     * Event()
#       → Bandera booleana thread-safe. .set() la activa, .clear() la desactiva,
#       → .wait() bloquea hasta que esté activa, .is_set() consulta el estado.
#     * current_thread().name
#       → Retorna el nombre del hilo que está ejecutando ese código.
import threading

# import time
# - Módulo estándar para funciones relacionadas con el tiempo.
# - Usaremos:
#     * time.sleep(segundos) → pausa el hilo actual por 'segundos' segundos.
#       Importante: solo pausa el hilo que lo llama, NO todo el programa.
#     * time.strftime(formato) → retorna la hora actual formateada como string.
import time

# import random
# - Módulo estándar para generar números aleatorios.
# - Usaremos:
#     * random.randint(a, b) → retorna un entero aleatorio entre a y b (inclusive).
#       Ejemplo: random.randint(1, 5) puede retornar 1, 2, 3, 4 o 5.
import random

# import json
# - Módulo estándar para serializar/deserializar datos en formato JSON.
# - JSON (JavaScript Object Notation) es un formato de texto para intercambiar datos.
# - Usaremos:
#     * json.dumps(diccionario) → convierte un dict de Python a string JSON.
#       Ejemplo: json.dumps({"producto": "Laptop"}) → '{"producto": "Laptop"}'
#     * json.loads(string_json) → convierte un string JSON a dict de Python.
#       Ejemplo: json.loads('{"producto": "Laptop"}') → {"producto": "Laptop"}
# - ¿Por qué JSON? Porque los sockets solo envían bytes, y JSON nos permite
#   convertir diccionarios Python a texto (y luego a bytes) de forma estándar.
import json

# ==============================================================================
# CONSTANTES GLOBALES
# ==============================================================================

# HOST = "127.0.0.1"
# - Dirección IP donde el servidor escuchará conexiones.
# - "127.0.0.1" es la dirección de LOOPBACK (localhost).
#   Significa que SOLO acepta conexiones desde la misma máquina.
# - Si quisiéramos aceptar conexiones desde otras máquinas en la red,
#   usaríamos "0.0.0.0" (todas las interfaces de red).
HOST = "127.0.0.1"

# PORT = 65000
# - Puerto TCP donde el servidor escuchará.
# - Los puertos van de 0 a 65535.
#   * 0-1023: puertos "bien conocidos" (requieren permisos de administrador).
#     Ejemplos: 80 (HTTP), 443 (HTTPS), 22 (SSH).
#   * 1024-49151: puertos registrados (usados por aplicaciones conocidas).
#   * 49152-65535: puertos dinámicos/privados (ideales para pruebas).
# - 65000 es un puerto alto, ideal para pruebas porque no conflicta con
#   servicios del sistema.
PORT = 65000

# ENCODING = "utf-8"
# - Codificación de caracteres usada para convertir strings ↔ bytes.
# - Los sockets trabajan con BYTES, no con strings.
#   * Para ENVIAR: "Hola".encode("utf-8") → b'Hola' (bytes)
#   * Para RECIBIR: b'Hola'.decode("utf-8") → "Hola" (string)
# - UTF-8 soporta todos los caracteres Unicode (acentos, ñ, emojis, etc.).
ENCODING = "utf-8"

# BUFFER_SIZE = 4096
# - Tamaño máximo del buffer de recepción en bytes.
# - Cuando llamamos a socket.recv(BUFFER_SIZE), el socket lee HASTA 4096 bytes.
# - Si el mensaje es más pequeño, solo lee lo que haya disponible.
# - 4096 bytes = 4 KB, suficiente para nuestros mensajes JSON pequeños.
BUFFER_SIZE = 4096

# CAPACIDAD_MAXIMA_COLA = 10
# - Número máximo de pedidos que pueden estar simultáneamente en la cola.
# - Este valor lo usa el SEMÁFORO para limitar la capacidad.
# - Si la cola está llena (10 pedidos), el próximo productor que intente
#   agregar un pedido se BLOQUEARÁ hasta que un consumidor retire uno.
# - Simula la capacidad máxima del sistema de la central de pedidos.
CAPACIDAD_MAXIMA_COLA = 10

# NUM_PROCESADORES = 3
# - Cantidad de hilos procesadores (consumidores) que despachan pedidos.
# - Más procesadores = más pedidos se atienden en paralelo.
# - Cada procesador es un hilo independiente que toma pedidos de la cola.
NUM_PROCESADORES = 3

# PEDIDOS_MINIMOS_PARA_BARRERA = 5
# - Cantidad mínima de pedidos que deben acumularse en la cola ANTES de que
#   los procesadores comiencen a despachar.
# - La BARRERA se usa para esta sincronización: los procesadores esperan
#   hasta que se alcance este umbral.
# - Esto simula que la central de pedidos espera a tener un lote mínimo
#   antes de empezar a procesar (eficiencia por lotes).
PEDIDOS_MINIMOS_PARA_BARRERA = 5

# MAX_CLIENTES = 5
# - Número máximo de clientes que el servidor aceptará antes de cerrar
#   la conexión entrante.
# - Cada cliente se conecta, envía sus pedidos, y se desconecta.
MAX_CLIENTES = 5

# ==============================================================================
# STOCK DE PRODUCTOS (INVENTARIO)
# ==============================================================================

# stock_productos: dict[str, int]
# - Diccionario que mapea nombre_producto → cantidad_disponible.
# - Este es un RECURSO COMPARTIDO: múltiples hilos lo leen y modifican.
# - DEBE protegerse con un Lock para evitar condiciones de carrera.
#   Ejemplo de condición de carrera sin Lock:
#     Hilo A lee stock["Laptop"] = 5
#     Hilo B lee stock["Laptop"] = 5
#     Hilo A descuenta 2 → stock["Laptop"] = 3
#     Hilo B descuenta 3 → stock["Laptop"] = 2  ← ¡INCORRECTO! Debería ser 0.
#   Con Lock, solo un hilo accede a la vez, evitando este problema.
stock_productos = {
    "Laptop": 10,       # 10 laptops disponibles en el inventario
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
#   El primer pedido que entra es el primero que se procesa.
# - Cada elemento es un diccionario con la información del pedido:
#   {"producto": "Laptop", "cantidad": 2, "cliente": "Cliente-1"}
# - Es un RECURSO COMPARTIDO entre productores (hilos de clientes) y
#   consumidores (hilos procesadores).
# - DEBE protegerse con Lock para acceso seguro.
# - Se usa append() para agregar al final y pop(0) para sacar del inicio (FIFO).
cola_pedidos = []

# ==============================================================================
# PRIMITIVAS DE SINCRONIZACIÓN (BASE)
# ==============================================================================

# lock_cola: threading.Lock
# - Candado (Lock / Mutex) para proteger el acceso a la cola de pedidos.
# - REGLA: Cualquier hilo que quiera LEER o MODIFICAR 'cola_pedidos' DEBE
#   primero adquirir este lock con lock_cola.acquire() y luego soltarlo
#   con lock_cola.release().
# - También se puede usar con 'with lock_cola:' que adquiere y suelta
#   automáticamente (incluso si hay excepciones).
# - Solo UN hilo puede tener el lock a la vez. Los demás esperan.
lock_cola = threading.Lock()

# lock_stock: threading.Lock
# - Candado separado para proteger el acceso al diccionario stock_productos.
# - Usamos un lock SEPARADO del de la cola para mayor granularidad:
#   un hilo puede estar modificando el stock mientras otro modifica la cola.
# - Si usáramos un solo lock para ambos, se reduciría la concurrencia.
lock_stock = threading.Lock()

# semaforo_capacidad: threading.Semaphore
# - Semáforo inicializado con CAPACIDAD_MAXIMA_COLA (10).
# - Funciona como un "contador de espacios disponibles" en la cola.
# - Cada vez que un productor agrega un pedido a la cola, llama a
#   semaforo_capacidad.acquire() que DECREMENTA el contador en 1.
#   * Si el contador llega a 0, el siguiente acquire() se BLOQUEA
#     hasta que un consumidor llame a release().
# - Cada vez que un consumidor retira un pedido, llama a
#   semaforo_capacidad.release() que INCREMENTA el contador en 1.
#   * Esto "libera" un espacio y desbloquea a un productor en espera.
# - EFECTO: La cola nunca tendrá más de CAPACIDAD_MAXIMA_COLA pedidos.
semaforo_capacidad = threading.Semaphore(CAPACIDAD_MAXIMA_COLA)

# barrera_procesadores: threading.Barrier
# - Barrera configurada para PEDIDOS_MINIMOS_PARA_BARRERA (5) participantes.
# - Los hilos procesadores llamarán a barrera_procesadores.wait() y se
#   bloquearán hasta que 5 hilos (o señales) lleguen al punto de barrera.
# - Usamos un enfoque donde el hilo que monitorea la cola notifica a la
#   barrera cuando se alcanzan suficientes pedidos.
# - EFECTO: Los procesadores no empiezan a trabajar hasta que haya al menos
#   5 pedidos en la cola, simulando procesamiento por lotes.
barrera_procesadores = threading.Barrier(NUM_PROCESADORES + 1)
# +1 porque el hilo "monitor" también participa en la barrera.
# Cuando hay suficientes pedidos, el monitor llama a .wait() y desbloquea
# a todos los procesadores que ya estaban esperando.

# evento_barrera_liberada: threading.Event
# - Un Event es como una bandera booleana thread-safe.
# - .set()    → pone la bandera en True (señal de "adelante").
# - .clear()  → pone la bandera en False.
# - .wait()   → bloquea el hilo hasta que la bandera sea True.
# - .is_set() → retorna True si la bandera está encendida.
# - Lo usamos para indicar que la barrera ya fue liberada y que los
#   procesadores pueden empezar a tomar pedidos de la cola.
evento_barrera_liberada = threading.Event()

# contador_pedidos_totales: int
# - Lleva la cuenta del total de pedidos que han entrado a la cola.
# - Se usa para saber cuándo se alcanza PEDIDOS_MINIMOS_PARA_BARRERA.
# - Es un recurso compartido, se protege con lock_cola.
contador_pedidos_totales = 0

# evento_servidor_activo: threading.Event
# - Bandera que indica si el servidor sigue activo (aceptando clientes).
# - Cuando se apaga (clear()), los procesadores saben que deben terminar.
evento_servidor_activo = threading.Event()
evento_servidor_activo.set()  # Inicialmente el servidor está activo.

# ==============================================================================
# [MOD6] NUEVAS ESTRUCTURAS PARA BROADCAST
# ==============================================================================

# clientes_activos: list[socket.socket]
# - Lista que contiene los objetos SOCKET de todos los clientes que están
#   ACTUALMENTE conectados al servidor en este momento.
# - Cada elemento es un socket.socket con el que podemos enviar datos al
#   cliente correspondiente.
# - Ciclo de vida de un elemento en esta lista:
#     1. Se AGREGA cuando el hilo atender_cliente() arranca y el cliente
#        ya está listo para recibir mensajes.
#     2. Se ELIMINA en el bloque finally de atender_cliente(), cuando el
#        cliente se desconecta (con o sin error).
# - Esta lista permite implementar BROADCAST: enviar un mismo mensaje a
#   TODOS los clientes conectados al mismo tiempo.
# - ADVERTENCIA: Es un RECURSO COMPARTIDO. Múltiples hilos (uno por cliente)
#   pueden intentar modificarla al mismo tiempo → NECESITA un Lock.
# - La inicializamos vacía: cuando arranque el servidor, aún no hay clientes.
clientes_activos = []

# lock_clientes: threading.Lock
# - Candado (mutex) para proteger EXCLUSIVAMENTE la lista clientes_activos.
# - ¿Por qué un lock SEPARADO?
#     * Granularidad fina: un hilo puede estar enviando datos al cliente
#       (sin tocar la lista) mientras otro modifica la lista.
#     * Si usáramos lock_cola para proteger también la lista, aumentaríamos
#       la contención (más hilos esperando el mismo lock).
# - REGLA DE USO:
#     * Para AGREGAR un cliente: adquirir lock_clientes → append → release.
#     * Para ELIMINAR un cliente: adquirir lock_clientes → remove → release.
#     * Para BROADCAST: adquirir lock_clientes → copiar lista → release.
#       (luego iterar sobre la COPIA sin el lock, para no bloquearlo mientras
#       se envían datos por la red, operación que puede ser lenta).
# - La regla "copiar lista antes de iterar" es crucial: si mantuviéramos
#   el lock mientras enviamos datos, ningún otro hilo podría agregar/eliminar
#   clientes mientras dura el broadcast (contención prolongada).
lock_clientes = threading.Lock()


# ==============================================================================
# FUNCIONES DEL SERVIDOR
# ==============================================================================


def log(mensaje):
    """
    Imprime un mensaje con marca de tiempo y nombre del hilo actual.

    Parámetros:
        mensaje (str): El texto a imprimir.

    Retorna:
        None (no retorna nada, solo imprime en consola).

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


def mostrar_stock():
    """
    Muestra el inventario actual de productos en consola.

    No recibe parámetros.

    Retorna:
        None (solo imprime en consola).

    ¿Por qué usamos lock_stock?
        - Porque otro hilo podría estar modificando stock_productos al mismo
          tiempo (por ejemplo, un procesador descontando unidades).
        - Sin lock, podríamos leer un estado intermedio/inconsistente.
        - 'with lock_stock:' es equivalente a:
              lock_stock.acquire()
              try:
                  ... código ...
              finally:
                  lock_stock.release()
          La ventaja del 'with' es que SIEMPRE suelta el lock, incluso si
          ocurre un error dentro del bloque.
    """
    # Adquirir el lock del stock antes de leerlo.
    # 'with' garantiza que se libere automáticamente al salir del bloque.
    with lock_stock:
        print("\n" + "=" * 50)
        print("        STOCK ACTUAL DE PRODUCTOS")
        print("=" * 50)

        # Iterar sobre cada producto y su cantidad en el diccionario.
        # .items() retorna pares (clave, valor) = (nombre_producto, cantidad).
        for producto, cantidad in stock_productos.items():
            # f-string con formato: {producto:<15} alinea a la izquierda en 15 caracteres.
            # Esto hace que la tabla quede alineada visualmente.
            print(f"  {producto:<15} → {cantidad} unidades")

        print("=" * 50 + "\n")


def obtener_lista_productos():
    """
    Retorna la lista de nombres de productos disponibles en el stock.

    No recibe parámetros.

    Retorna:
        list[str]: Lista con los nombres de los productos.
        Ejemplo: ["Laptop", "Mouse", "Teclado", ...]

    ¿Por qué usamos lock_stock?
        - Para leer el diccionario de forma segura (otro hilo podría estar
          modificándolo en este momento).
        - list(stock_productos.keys()) crea una COPIA de las claves, así
          el lock se suelta rápido y el receptor trabaja con la copia.
    """
    with lock_stock:
        # .keys() retorna las claves del diccionario (nombres de productos).
        # list() convierte esas claves en una lista de strings.
        return list(stock_productos.keys())


# ==============================================================================
# [MOD6] FUNCIÓN BROADCAST — NÚCLEO DE LA MODIFICACIÓN 6
# ==============================================================================

def broadcast(mensaje):
    """
    [MOD6] Envía un mensaje a TODOS los clientes actualmente conectados.

    Esta función es el núcleo de la Modificación 6. Implementa el patrón
    BROADCAST (difusión): un mismo mensaje llega a todos los receptores
    conectados, sin importar cuántos sean.

    Parámetros:
        mensaje (str): El string JSON ya serializado que se enviará a cada
                       cliente. Se espera que sea el resultado de json.dumps()
                       aplicado a un diccionario de tipo "broadcast".
                       Ejemplo:
                         '{"tipo": "broadcast", "mensaje": "Pedido de Cliente-1: 2x Laptop - DESPACHADO"}'

    Retorna:
        None (no retorna nada; los envíos son efectos secundarios).

    Proceso detallado:
        1. Adquirir lock_clientes (exclusión mutua sobre clientes_activos).
        2. Hacer una COPIA SUPERFICIAL (list(...)) de clientes_activos.
           - Esto es fundamental: se copia la referencia a los sockets,
             no los sockets en sí (son objetos mutables).
           - ¿Por qué copiar? Para soltar el lock INMEDIATAMENTE y no
             mantenerlo mientras se realizan envíos de red (que pueden
             tardar o fallar). Si mantuviéramos el lock durante el envío,
             ningún cliente podría conectarse o desconectarse mientras
             dura el broadcast, causando contención prolongada.
        3. Soltar lock_clientes (queda libre para otras operaciones).
        4. Iterar sobre la COPIA (sin lock) y enviar el mensaje a cada socket.
        5. Si un cliente ya cerró su socket (OSError, BrokenPipeError, etc.),
           capturar la excepción silenciosamente y continuar con el siguiente.
           - El socket "muerto" será eliminado de clientes_activos
             eventualmente por su hilo atender_cliente() en el bloque finally.

    Patrón de diseño:
        - "Copy-on-iterate": copiamos la colección antes de iterar, para
          evitar problemas si la colección cambia durante la iteración.
        - Lock de granularidad fina: el lock se mantiene el mínimo tiempo
          posible (solo para copiar la lista, no para el envío en sí).

    Hilo que llama a esta función:
        - Principalmente los hilos PROCESADORES (hilo_procesador), que la
          invocan cada vez que despachan un pedido exitosamente.
        - Podría llamarla cualquier hilo del servidor que necesite notificar
          a todos los clientes de algún evento.
    """
    # --- PASO 1 y 2: Adquirir el lock y copiar la lista ---
    # 'with lock_clientes:' adquiere el lock al entrar y lo suelta al salir.
    with lock_clientes:
        # list(clientes_activos) crea una NUEVA lista con las mismas
        # referencias a sockets. Es una copia superficial (shallow copy).
        # Esto garantiza que si alguien agrega/elimina clientes mientras
        # iteramos, iteramos sobre el estado que había en este momento.
        instantanea_clientes = list(clientes_activos)
    # El lock ya fue soltado aquí. La variable instantanea_clientes es
    # local a esta función y no es compartida → no necesita protección.

    # Convertir el string de mensaje a bytes una sola vez (eficiencia).
    # Es mejor hacerlo aquí que dentro del bucle (no repetir la conversión).
    mensaje_bytes = mensaje.encode(ENCODING)

    # Contar éxitos y fallos para el log (diagnóstico).
    enviados = 0
    fallidos = 0

    # --- PASO 4: Iterar sobre la copia y enviar a cada cliente ---
    for sock_cliente in instantanea_clientes:
        try:
            # sendall() envía TODOS los bytes al socket del cliente.
            # Si el cliente está conectado y activo, el envío tendrá éxito.
            sock_cliente.sendall(mensaje_bytes)
            enviados += 1

        except OSError:
            # OSError incluye casos como:
            #   - BrokenPipeError: el cliente cerró la conexión abruptamente.
            #   - ConnectionResetError: el cliente se cayó.
            #   - [Errno 9] Bad file descriptor: el socket ya fue cerrado.
            # En todos estos casos, ignoramos el error silenciosamente.
            # El socket "muerto" será limpiado por su hilo atender_cliente()
            # cuando detecte la desconexión y ejecute su bloque finally.
            fallidos += 1

    # Log informativo: cuántos clientes recibieron el broadcast.
    if instantanea_clientes:
        log(f"[BROADCAST] Enviado a {enviados}/{len(instantanea_clientes)} clientes "
            f"({fallidos} fallos).")
    # Si no hay clientes activos, el broadcast se "envió" a 0 clientes.
    # No imprimimos nada en ese caso para no saturar el log.


def agregar_pedido_a_cola(pedido):
    """
    Agrega un pedido a la cola compartida de forma segura.

    Este es el método que usan los PRODUCTORES (hilos operadores de clientes).

    Parámetros:
        pedido (dict): Diccionario con la información del pedido.
            Estructura esperada:
            {
                "producto": str,   # Nombre del producto (ej: "Laptop")
                "cantidad": int,   # Cantidad solicitada (ej: 2)
                "cliente": str     # Identificador del cliente (ej: "Cliente-1")
            }

    Retorna:
        bool: True si el pedido fue agregado exitosamente, False si la cola
              estaba llena y no se pudo agregar (timeout del semáforo).

    Proceso:
        1. Intenta adquirir el SEMÁFORO (verifica que haya espacio en la cola).
        2. Si hay espacio, adquiere el LOCK de la cola.
        3. Agrega el pedido al final de la cola (append).
        4. Incrementa el contador de pedidos totales.
        5. Si se alcanzó el umbral de la barrera, la notifica.
        6. Suelta el lock y retorna True.
        7. Si no hay espacio (semáforo en 0), retorna False después de 5 segundos.
    """
    # Variable global para poder modificar el contador desde dentro de la función.
    # Sin 'global', Python trataría contador_pedidos_totales como variable LOCAL
    # y daría error al intentar asignarle un valor.
    global contador_pedidos_totales

    # --- PASO 1: Adquirir el semáforo ---
    # semaforo_capacidad.acquire(blocking=True, timeout=5)
    #   - blocking=True: si el semáforo está en 0, el hilo se BLOQUEA (espera).
    #   - timeout=5: si después de 5 segundos no se liberó espacio, retorna False.
    #   - Si hay espacio (semáforo > 0), decrementa el contador y retorna True.
    # ¿Por qué timeout? Para no bloquear al cliente indefinidamente si la cola
    # está llena. Después de 5 segundos, le informamos que no se pudo agregar.
    espacio_disponible = semaforo_capacidad.acquire(blocking=True, timeout=5)

    # Si no hay espacio disponible (semáforo agotado después del timeout):
    if not espacio_disponible:
        log(f"⚠ Cola llena. No se pudo agregar pedido de {pedido['cliente']}: "
            f"{pedido['cantidad']}x {pedido['producto']}")
        return False  # Informar al llamador que el pedido no se agregó.

    # --- PASO 2: Adquirir el lock de la cola ---
    # Necesitamos exclusión mutua para modificar cola_pedidos de forma segura.
    with lock_cola:
        # --- PASO 3: Agregar el pedido a la cola ---
        # append() agrega el elemento al FINAL de la lista (comportamiento FIFO).
        cola_pedidos.append(pedido)

        # --- PASO 4: Incrementar el contador ---
        contador_pedidos_totales += 1

        # Guardar el valor actual para usarlo fuera del lock.
        total_actual = contador_pedidos_totales

        # Mostrar información del pedido agregado.
        log(f"+ Pedido agregado a la cola: {pedido['cantidad']}x {pedido['producto']} "
            f"(de {pedido['cliente']}). "
            f"Cola: {len(cola_pedidos)}/{CAPACIDAD_MAXIMA_COLA} | "
            f"Total histórico: {total_actual}")

    # --- PASO 5: Verificar si se alcanzó el umbral de la barrera ---
    # Si ya se acumularon suficientes pedidos Y la barrera no ha sido liberada aún:
    if (total_actual >= PEDIDOS_MINIMOS_PARA_BARRERA
            and not evento_barrera_liberada.is_set()):
        log(f"★ Se alcanzaron {PEDIDOS_MINIMOS_PARA_BARRERA} pedidos. "
            f"Notificando a la barrera para liberar procesadores...")
        # Llamar a wait() en la barrera desde el hilo monitor.
        # Cuando todos los NUM_PROCESADORES + 1 (este hilo) llamen a wait(),
        # la barrera se abre y todos continúan.
        try:
            barrera_procesadores.wait()
        except threading.BrokenBarrierError:
            # BrokenBarrierError ocurre si la barrera se "rompe" (ej: timeout).
            log("⚠ Error en la barrera, pero se continuará el procesamiento.")
        # Marcar que la barrera ya fue liberada.
        evento_barrera_liberada.set()

    return True  # Pedido agregado exitosamente.


def retirar_pedido_de_cola():
    """
    Retira y retorna el primer pedido de la cola compartida (FIFO).

    Este es el método que usan los CONSUMIDORES (hilos procesadores).

    No recibe parámetros.

    Retorna:
        dict o None:
            - dict con la información del pedido si la cola tenía pedidos.
            - None si la cola estaba vacía.

    Proceso:
        1. Adquiere el LOCK de la cola (exclusión mutua).
        2. Si la cola tiene elementos, retira el PRIMERO (pop(0) = FIFO).
        3. Libera un espacio en el SEMÁFORO (release()).
        4. Suelta el lock y retorna el pedido.
        5. Si la cola está vacía, retorna None.
    """
    # Adquirir el lock para acceder a la cola de forma segura.
    with lock_cola:
        # Verificar si hay pedidos en la cola.
        # len(cola_pedidos) retorna la cantidad de elementos en la lista.
        if len(cola_pedidos) > 0:
            # pop(0) retira y retorna el PRIMER elemento de la lista.
            # Esto implementa el comportamiento FIFO (First In, First Out).
            # El primer pedido que entró es el primero que se atiende.
            pedido = cola_pedidos.pop(0)

            log(f"- Pedido retirado de la cola: {pedido['cantidad']}x "
                f"{pedido['producto']} (de {pedido['cliente']}). "
                f"Cola restante: {len(cola_pedidos)}")

            # Liberar un espacio en el semáforo.
            # Esto incrementa el contador del semáforo en 1, indicando que
            # hay un espacio libre en la cola para un nuevo pedido.
            # Si algún productor estaba bloqueado esperando espacio, se desbloquea.
            semaforo_capacidad.release()

            return pedido  # Retornar el pedido para que el procesador lo despache.

        else:
            # La cola está vacía, no hay pedidos para procesar.
            return None


def procesar_pedido(pedido):
    """
    Procesa (despacha) un pedido: verifica stock, descuenta, simula tiempo
    y realiza BROADCAST a todos los clientes conectados.

    Este es el trabajo principal de cada CONSUMIDOR (hilo procesador).

    Parámetros:
        pedido (dict): Diccionario con la información del pedido.
            Estructura:
            {
                "producto": str,   # Nombre del producto
                "cantidad": int,   # Cantidad solicitada
                "cliente": str     # Identificador del cliente
            }

    Retorna:
        None (no retorna nada, solo procesa el pedido y muestra resultados).

    Proceso:
        1. Simula el tiempo de procesamiento (1-5 segundos).
        2. Adquiere el LOCK del stock para leer/modificar el inventario.
        3. Verifica si hay suficiente stock del producto solicitado.
        4. Si hay stock: descuenta la cantidad y marca como "DESPACHADO".
           [MOD6] Llama a broadcast() para notificar a todos los clientes.
        5. Si NO hay stock: marca como "RECHAZADO" (sin stock).
        6. Suelta el lock.

    [MOD6] CAMBIO EN ESTA FUNCIÓN:
        - Después de despachar exitosamente un pedido (stock descontado),
          se construye un mensaje de broadcast y se llama a la función
          broadcast() para enviarlo a todos los clientes activos.
        - El mensaje tiene formato:
          {"tipo": "broadcast", "mensaje": "Pedido de X: Nx Producto - DESPACHADO"}
        - El broadcast se realiza FUERA del lock_stock para no mantenerlo
          más tiempo del necesario. Se construye el mensaje dentro del lock
          (para tener los datos correctos) y se llama a broadcast() fuera.
        - broadcast() usa su propio mecanismo de lock (lock_clientes), no
          interfiere con lock_stock.
    """
    # Extraer los datos del pedido para mayor legibilidad.
    producto = pedido["producto"]    # Nombre del producto solicitado.
    cantidad = pedido["cantidad"]    # Cantidad solicitada.
    cliente = pedido["cliente"]      # Quién hizo el pedido.

    # --- Simular tiempo de procesamiento ---
    # random.randint(1, 5) genera un entero aleatorio entre 1 y 5 (inclusive).
    # time.sleep() pausa SOLO este hilo por esa cantidad de segundos.
    # Esto simula que el procesador tarda en preparar/empacar/despachar el pedido.
    tiempo_procesamiento = random.randint(1, 5)
    log(f"⏳ Procesando pedido de {cliente}: {cantidad}x {producto} "
        f"(tardará {tiempo_procesamiento}s)...")
    time.sleep(tiempo_procesamiento)

    # --- Verificar y descontar stock ---
    # Usamos lock_stock para acceder al diccionario de stock de forma segura.
    # Variable para construir el mensaje de broadcast DENTRO del lock
    # (donde tenemos datos consistentes) y enviarlo FUERA del lock.
    mensaje_broadcast = None  # Se asignará si el pedido se despacha con éxito.

    with lock_stock:
        # Verificar si el producto existe en el stock.
        if producto in stock_productos:
            # Verificar si hay suficiente cantidad.
            if stock_productos[producto] >= cantidad:
                # Descontar la cantidad del stock.
                stock_productos[producto] -= cantidad
                log(f"✓ DESPACHADO: {cantidad}x {producto} para {cliente}. "
                    f"Stock restante de {producto}: {stock_productos[producto]}")

                # [MOD6] Construir el mensaje de broadcast DENTRO del lock.
                # Así tenemos el estado del stock en el momento exacto del despacho.
                # El mensaje sigue el formato acordado con el cliente:
                # {"tipo": "broadcast", "mensaje": "Pedido de X: Nx Producto - DESPACHADO"}
                texto_broadcast = (
                    f"Pedido de {cliente}: {cantidad}x {producto} - DESPACHADO"
                )
                mensaje_broadcast = json.dumps({
                    "tipo": "broadcast",
                    "mensaje": texto_broadcast
                })
                # Nota: json.dumps() convierte el dict a string JSON.
                # La función broadcast() lo codificará a bytes al enviarlo.

            else:
                # No hay suficiente stock.
                disponible = stock_productos[producto]
                log(f"✗ RECHAZADO: {cantidad}x {producto} para {cliente}. "
                    f"Stock insuficiente (disponible: {disponible})")
                # No se hace broadcast en caso de rechazo (pedido no procesado).
        else:
            # El producto no existe en el inventario.
            log(f"✗ RECHAZADO: Producto '{producto}' no existe en el inventario. "
                f"Pedido de {cliente}.")
            # No se hace broadcast en caso de producto inválido.

    # [MOD6] BROADCAST — Ejecutado FUERA del lock_stock.
    # Razones para hacerlo fuera del lock:
    #   1. El lock_stock ya fue soltado (bloque 'with' terminó).
    #   2. broadcast() puede tardar si hay muchos clientes o hay problemas de red.
    #   3. Mantener lock_stock durante el broadcast retrasaría a otros procesadores
    #      que necesitan acceder al stock.
    # Solo se hace broadcast si el pedido fue despachado exitosamente.
    if mensaje_broadcast is not None:
        log(f"📢 Iniciando broadcast del despacho a todos los clientes...")
        broadcast(mensaje_broadcast)


def hilo_procesador(id_procesador):
    """
    Función que ejecuta cada hilo procesador (CONSUMIDOR).

    Cada procesador es un hilo que corre en un bucle infinito:
    1. Espera en la BARRERA hasta que haya suficientes pedidos.
    2. Toma pedidos de la cola y los procesa (despacha).
    3. Al despachar, hace BROADCAST a todos los clientes.  [MOD6]
    4. Si la cola está vacía, espera un segundo y vuelve a intentar.
    5. Termina cuando el servidor se apaga y la cola está vacía.

    Parámetros:
        id_procesador (int): Número identificador del procesador (1, 2, 3...).
            Se usa solo para mostrar en los logs cuál procesador está actuando.

    Retorna:
        None (el hilo ejecuta un bucle hasta que el servidor se apague).
    """
    log(f"Procesador-{id_procesador} iniciado. Esperando en la barrera...")

    # --- PASO 1: Esperar en la BARRERA ---
    # El procesador se bloquea aquí hasta que TODOS los participantes de la
    # barrera (NUM_PROCESADORES + 1 = 4) llamen a .wait().
    # El "+1" es el hilo que detecta que hay suficientes pedidos en la cola.
    # Cuando el último participante llama a .wait(), TODOS se desbloquean.
    try:
        barrera_procesadores.wait()
    except threading.BrokenBarrierError:
        # Si la barrera se rompe (timeout u otro error), continuamos igualmente.
        log(f"Procesador-{id_procesador}: Barrera rota, continuando...")

    log(f"Procesador-{id_procesador} desbloqueado. ¡Comenzando a procesar pedidos!")

    # --- PASO 2: Bucle principal de procesamiento ---
    # El procesador sigue trabajando mientras:
    #   a) El servidor esté activo (evento_servidor_activo está set), O
    #   b) Queden pedidos en la cola (para no perder pedidos al cerrar).
    while evento_servidor_activo.is_set() or len(cola_pedidos) > 0:
        # Intentar retirar un pedido de la cola.
        pedido = retirar_pedido_de_cola()

        if pedido is not None:
            # Si obtuvimos un pedido, procesarlo (despacharlo).
            # procesar_pedido() incluye el broadcast [MOD6].
            procesar_pedido(pedido)
        else:
            # Si la cola está vacía y el servidor ya no está activo, terminar.
            if not evento_servidor_activo.is_set():
                break
            # Si la cola está vacía pero el servidor sigue activo, esperar
            # un segundo antes de volver a intentar.
            # Esto evita un "busy wait" (bucle que consume CPU sin hacer nada).
            time.sleep(1)

    log(f"Procesador-{id_procesador} finalizado.")


def atender_cliente(conexion_cliente, direccion_cliente, id_cliente):
    """
    Función que ejecuta cada hilo operador (PRODUCTOR) para atender a un cliente.

    Cada cliente que se conecta al servidor es atendido por un hilo independiente.
    Este hilo:
    1. [MOD6] Registra el socket del cliente en clientes_activos.
    2. Envía la lista de productos disponibles al cliente.
    3. Recibe pedidos del cliente (en formato JSON).
    4. Agrega cada pedido a la cola compartida.
    5. Envía confirmaciones/rechazos al cliente.
    6. Cierra la conexión cuando el cliente termina.
    7. [MOD6] Elimina el socket del cliente de clientes_activos.

    Parámetros:
        conexion_cliente (socket.socket): El socket de la conexión con el cliente.
            Este objeto permite enviar y recibir datos con ESE cliente específico.
            Cada cliente tiene su propio socket de conexión.
            [MOD6] Este mismo socket se almacena en clientes_activos para el broadcast.
        direccion_cliente (tuple): Tupla (ip, puerto) del cliente.
            Ejemplo: ("127.0.0.1", 54321)
            El puerto es asignado aleatoriamente por el SO del cliente.
        id_cliente (int): Número secuencial del cliente (1, 2, 3...).
            Se usa para identificar al cliente en los logs.

    Retorna:
        None (el hilo se ejecuta hasta que el cliente se desconecte).

    [MOD6] CAMBIOS EN ESTA FUNCIÓN:
        - Al inicio (después de enviar la bienvenida): se registra el socket
          del cliente en la lista clientes_activos protegida por lock_clientes.
        - En el bloque finally: se elimina el socket de clientes_activos
          de forma segura con lock_clientes.
        - El cliente puede recibir mensajes de tipo "broadcast" de los
          procesadores mientras está conectado (los recibe de forma pasiva,
          ya que el servidor se los envía directamente por su socket).
    """
    # Crear un nombre legible para este cliente.
    nombre_cliente = f"Cliente-{id_cliente}"
    log(f"Conexión aceptada de {nombre_cliente} ({direccion_cliente[0]}:{direccion_cliente[1]})")

    try:
        # --- PASO 1: Enviar la lista de productos disponibles al cliente ---
        # Obtenemos la lista de productos del stock.
        lista_productos = obtener_lista_productos()

        # Creamos un mensaje de BIENVENIDA en formato JSON.
        # El cliente recibirá este diccionario y sabrá qué productos puede pedir.
        mensaje_bienvenida = {
            "tipo": "bienvenida",                     # Tipo de mensaje para que el cliente lo identifique.
            "mensaje": f"Bienvenido {nombre_cliente} a la Central de Pedidos",
            "productos_disponibles": lista_productos,  # Lista de nombres de productos.
            "tu_id": nombre_cliente                    # ID asignado al cliente.
        }

        # Convertir el diccionario a string JSON y luego a bytes para enviar.
        # json.dumps() → convierte dict a string JSON.
        # .encode(ENCODING) → convierte string a bytes (necesario para el socket).
        # sendall() → envía TODOS los bytes (a diferencia de send(), que puede enviar
        #             solo una parte y habría que reintentar).
        conexion_cliente.sendall(json.dumps(mensaje_bienvenida).encode(ENCODING))

        # [MOD6] --- REGISTRO EN clientes_activos ---
        # DESPUÉS de enviar la bienvenida (y no antes), registramos al cliente
        # en la lista de activos. De esta forma, el cliente solo recibe broadcasts
        # a partir del momento en que ya está listo para recibirlos (ha procesado
        # la bienvenida). Si lo registráramos antes, podría recibir un broadcast
        # mezclado con la respuesta de bienvenida.
        #
        # Adquirimos lock_clientes para modificar la lista de forma segura.
        # 'with' garantiza que el lock se suelte aunque ocurra un error.
        with lock_clientes:
            clientes_activos.append(conexion_cliente)
            # Registrar cuántos clientes hay activos ahora (para diagnóstico).
            num_activos = len(clientes_activos)
        # El lock ya fue soltado aquí.
        log(f"[MOD6] {nombre_cliente} registrado en clientes_activos. "
            f"Total activos: {num_activos}")

        # --- PASO 2: Recibir pedidos del cliente en un bucle ---
        while True:
            # recv(BUFFER_SIZE) espera y recibe hasta BUFFER_SIZE bytes del cliente.
            # Es una operación BLOQUEANTE: el hilo se pausa aquí hasta que
            # lleguen datos o el cliente cierre la conexión.
            datos_recibidos = conexion_cliente.recv(BUFFER_SIZE)

            # Si recv() retorna bytes vacíos (b''), significa que el cliente
            # cerró la conexión. Esto es normal y esperado.
            if not datos_recibidos:
                log(f"{nombre_cliente} se desconectó.")
                break  # Salir del bucle y terminar el hilo.

            # Decodificar los bytes recibidos a string.
            mensaje_texto = datos_recibidos.decode(ENCODING)

            try:
                # Intentar parsear el mensaje como JSON.
                # Si el cliente envía un formato inválido, json.loads() lanzará
                # una excepción json.JSONDecodeError.
                pedido_datos = json.loads(mensaje_texto)

                # Verificar si el cliente envió la señal de FIN (ya no enviará más pedidos).
                if pedido_datos.get("tipo") == "fin":
                    log(f"{nombre_cliente} ha terminado de enviar pedidos.")

                    # Enviar confirmación de cierre al cliente.
                    respuesta_fin = {
                        "tipo": "fin_confirmado",
                        "mensaje": "Todos tus pedidos fueron recibidos. ¡Gracias!"
                    }
                    conexion_cliente.sendall(
                        json.dumps(respuesta_fin).encode(ENCODING)
                    )
                    break  # Salir del bucle.

                # --- PASO 3: Procesar el pedido recibido ---
                # Construir el diccionario del pedido con la información necesaria.
                pedido = {
                    "producto": pedido_datos["producto"],    # Nombre del producto.
                    "cantidad": pedido_datos["cantidad"],    # Cantidad solicitada.
                    "cliente": nombre_cliente                # Quién lo pidió.
                }

                log(f"Pedido recibido de {nombre_cliente}: "
                    f"{pedido['cantidad']}x {pedido['producto']}")

                # --- PASO 4: Agregar el pedido a la cola ---
                # agregar_pedido_a_cola() maneja internamente el semáforo y el lock.
                exito = agregar_pedido_a_cola(pedido)

                # --- PASO 5: Enviar respuesta al cliente ---
                if exito:
                    respuesta = {
                        "tipo": "confirmacion",
                        "mensaje": f"Pedido recibido: {pedido['cantidad']}x "
                                   f"{pedido['producto']}. En cola de procesamiento.",
                        "estado": "en_cola"
                    }
                else:
                    respuesta = {
                        "tipo": "error",
                        "mensaje": f"Cola llena. No se pudo aceptar el pedido de "
                                   f"{pedido['cantidad']}x {pedido['producto']}.",
                        "estado": "rechazado"
                    }

                # Enviar la respuesta al cliente.
                conexion_cliente.sendall(
                    json.dumps(respuesta).encode(ENCODING)
                )

            except json.JSONDecodeError:
                # El mensaje no era JSON válido.
                log(f"⚠ Mensaje inválido de {nombre_cliente}: {mensaje_texto}")
                error_msg = {
                    "tipo": "error",
                    "mensaje": "Formato de mensaje inválido. Use JSON."
                }
                conexion_cliente.sendall(
                    json.dumps(error_msg).encode(ENCODING)
                )

            except KeyError as e:
                # Faltaba un campo esperado en el JSON (ej: "producto" o "cantidad").
                log(f"⚠ Pedido incompleto de {nombre_cliente}. Falta campo: {e}")
                error_msg = {
                    "tipo": "error",
                    "mensaje": f"Pedido incompleto. Falta el campo: {e}"
                }
                conexion_cliente.sendall(
                    json.dumps(error_msg).encode(ENCODING)
                )

    except ConnectionResetError:
        # El cliente cerró la conexión abruptamente (sin enviar señal de fin).
        # Esto pasa si el cliente se cierra inesperadamente o pierde conexión.
        log(f"⚠ {nombre_cliente} cerró la conexión inesperadamente.")

    except Exception as e:
        # Cualquier otro error inesperado.
        log(f"⚠ Error atendiendo a {nombre_cliente}: {e}")

    finally:
        # El bloque finally se SIEMPRE ejecuta, sin importar qué pasó arriba.
        # (sea que el cliente terminó normalmente o hubo un error).

        # [MOD6] --- BAJA DEL REGISTRO EN clientes_activos ---
        # Eliminar el socket de este cliente de la lista de activos.
        # Es CRÍTICO hacerlo aquí para que:
        #   1. Futuros broadcasts no intenten enviar a este socket (ya cerrado).
        #   2. La lista refleje el estado real del sistema en todo momento.
        # Adquirimos lock_clientes para la operación de eliminación.
        with lock_clientes:
            # Verificamos si el socket está en la lista antes de eliminar.
            # Podría no estar si la conexión falló antes del registro (ej:
            # error al enviar la bienvenida, antes del append inicial).
            if conexion_cliente in clientes_activos:
                clientes_activos.remove(conexion_cliente)
                num_activos = len(clientes_activos)
                log(f"[MOD6] {nombre_cliente} eliminado de clientes_activos. "
                    f"Total activos restantes: {num_activos}")
        # El lock ya fue soltado aquí.

        # SIEMPRE cerrar el socket del cliente, sin importar qué pasó.
        # Esto libera los recursos del sistema operativo asociados al socket.
        conexion_cliente.close()
        log(f"Conexión con {nombre_cliente} cerrada.")


def iniciar_servidor():
    """
    Función principal que arranca el servidor.

    No recibe parámetros.

    Retorna:
        None (ejecuta el servidor hasta que se cierra con Ctrl+C o se
        alcanza el límite de clientes).

    Proceso:
        1. Muestra el stock inicial de productos.
        2. Crea e inicia los hilos PROCESADORES (consumidores).
        3. Crea el socket del servidor y lo pone a escuchar.
        4. Acepta clientes en un bucle, creando un hilo por cada uno.
        5. Cuando se alcanza MAX_CLIENTES, deja de aceptar y espera a que
           los procesadores terminen.
        6. Muestra el stock final.
    """
    # Mostrar el stock inicial para referencia.
    print("\n" + "=" * 60)
    print("   CENTRAL DE PEDIDOS - SERVIDOR")
    print("   Simulación con Productores, Consumidores y BROADCAST [MOD6]")
    print("=" * 60)
    mostrar_stock()

    # --- PASO 1: Crear e iniciar los hilos procesadores (CONSUMIDORES) ---
    # Almacenamos los hilos en una lista para poder hacer .join() después.
    hilos_procesadores = []
    for i in range(1, NUM_PROCESADORES + 1):
        # Crear un hilo que ejecutará la función hilo_procesador.
        # target: la función que el hilo ejecutará.
        # args: tupla con los argumentos para la función. (i,) es una tupla
        #       de un solo elemento (la coma es necesaria para que Python
        #       la reconozca como tupla y no como paréntesis).
        # name: nombre del hilo (aparece en los logs).
        # daemon=True: si el programa principal termina, los hilos daemon
        #              se cierran automáticamente. Sin daemon=True, el programa
        #              esperaría indefinidamente a que estos hilos terminen.
        hilo = threading.Thread(
            target=hilo_procesador,
            args=(i,),
            name=f"Procesador-{i}",
            daemon=True
        )
        hilo.start()  # Iniciar el hilo (comienza a ejecutar hilo_procesador).
        hilos_procesadores.append(hilo)

    log(f"{NUM_PROCESADORES} procesadores iniciados (esperando en barrera).")

    # --- PASO 2: Crear el socket del servidor ---
    # socket.socket(AF_INET, SOCK_STREAM):
    #   - AF_INET: familia de direcciones IPv4.
    #   - SOCK_STREAM: tipo de socket TCP (flujo de datos confiable y ordenado).
    #   - Juntos crean un socket TCP/IP v4.
    servidor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # setsockopt(SOL_SOCKET, SO_REUSEADDR, 1):
    #   - SOL_SOCKET: nivel de opción = socket general.
    #   - SO_REUSEADDR: permite reutilizar la dirección/puerto inmediatamente.
    #   - 1: activar la opción (True).
    #   - ¿Por qué? Cuando cierras un servidor, el SO mantiene el puerto en
    #     estado TIME_WAIT por ~60 segundos. Sin esta opción, al reiniciar
    #     el servidor obtendrías "Address already in use". Con esta opción,
    #     puedes reiniciar inmediatamente.
    servidor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # bind((HOST, PORT)):
    #   - Asocia el socket a la dirección IP y puerto especificados.
    #   - Después de esto, el socket "escucha" en 127.0.0.1:65000.
    #   - Si el puerto ya está en uso, lanza OSError.
    servidor_socket.bind((HOST, PORT))

    # listen(MAX_CLIENTES):
    #   - Pone el socket en modo "servidor" (escucha conexiones entrantes).
    #   - MAX_CLIENTES es el tamaño de la cola de conexiones pendientes.
    #     Si llegan más conexiones de las que podemos aceptar, se rechazan.
    #   - Después de listen(), el socket NO acepta conexiones aún. Eso lo
    #     hace accept().
    servidor_socket.listen(MAX_CLIENTES)

    log(f"Servidor escuchando en {HOST}:{PORT}")
    log(f"Esperando hasta {MAX_CLIENTES} clientes...")
    log(f"[MOD6] Broadcast habilitado: todos los clientes recibirán notificaciones de despacho.")

    # --- PASO 3: Aceptar clientes en un bucle ---
    # Lista para almacenar los hilos de los clientes y poder hacer .join().
    hilos_clientes = []

    # Contador de clientes conectados.
    clientes_conectados = 0

    try:
        # Bucle que acepta clientes hasta alcanzar el límite.
        while clientes_conectados < MAX_CLIENTES:
            # accept(): espera a que un cliente se conecte (BLOQUEANTE).
            # Cuando un cliente se conecta, retorna:
            #   - conexion: un NUEVO socket exclusivo para este cliente.
            #     El socket original (servidor_socket) sigue escuchando.
            #   - direccion: tupla (ip_cliente, puerto_cliente).
            conexion, direccion = servidor_socket.accept()

            clientes_conectados += 1

            # Crear un hilo para atender a este cliente.
            # Cada cliente tiene su propio hilo, así el servidor puede
            # atender múltiples clientes SIMULTÁNEAMENTE (concurrencia).
            # [MOD6] atender_cliente() ahora registra/elimina el socket
            #        en clientes_activos para el broadcast.
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
        # Ctrl+C fue presionado. Cerrar el servidor de forma ordenada.
        log("\n⚠ Servidor interrumpido por el usuario (Ctrl+C).")

    # --- PASO 4: Esperar a que todos los clientes terminen ---
    log("Esperando a que todos los clientes terminen...")
    for hilo in hilos_clientes:
        # .join() bloquea el hilo principal hasta que este hilo termine.
        # Así nos aseguramos de que todos los pedidos fueron recibidos
        # antes de cerrar el servidor.
        hilo.join()

    log("Todos los clientes se han desconectado.")

    # --- PASO 5: Esperar a que los procesadores terminen ---
    # Señalar a los procesadores que el servidor ya no está activo.
    evento_servidor_activo.clear()  # Pone la bandera en False.

    # Si la barrera no fue liberada (no llegaron suficientes pedidos),
    # la rompemos para que los procesadores no se queden bloqueados.
    if not evento_barrera_liberada.is_set():
        log("⚠ No se alcanzaron suficientes pedidos para la barrera. Abortando barrera...")
        barrera_procesadores.abort()
        # abort() "rompe" la barrera, haciendo que todos los .wait() lancen
        # BrokenBarrierError, y los hilos pueden continuar o terminar.

    # Dar tiempo a los procesadores para terminar los pedidos restantes.
    log("Esperando a que los procesadores terminen los pedidos restantes...")
    for hilo in hilos_procesadores:
        hilo.join(timeout=30)
        # timeout=30: espera máximo 30 segundos por cada procesador.
        # Si un procesador no termina en 30 segundos, continuamos (es daemon).

    # --- PASO 6: Cerrar el socket del servidor ---
    servidor_socket.close()
    # close() libera el puerto y todos los recursos del socket.
    # Después de esto, ningún nuevo cliente puede conectarse.

    # --- PASO 7: Mostrar el stock final ---
    print("\n" + "=" * 60)
    print("   STOCK FINAL DESPUÉS DEL PROCESAMIENTO")
    print("=" * 60)
    mostrar_stock()
    log("Servidor cerrado correctamente.")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================

# if __name__ == "__main__":
# - Esta condición verifica si este archivo se está EJECUTANDO directamente
#   (python servidor.py) o si está siendo IMPORTADO desde otro archivo.
# - __name__ es una variable especial que Python establece:
#   * Si el archivo se ejecuta directamente: __name__ == "__main__" → True
#   * Si el archivo se importa: __name__ == "servidor" → False
# - ¿Por qué usarlo? Para que el servidor SOLO se inicie cuando ejecutamos
#   este archivo directamente, no cuando lo importamos desde otro módulo.
if __name__ == "__main__":
    iniciar_servidor()
