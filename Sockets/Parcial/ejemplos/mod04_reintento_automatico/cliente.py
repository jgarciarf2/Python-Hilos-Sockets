"""
==============================================================================
cliente.py  —  MODIFICACIÓN 4: Reintento automático cuando la cola está llena
==============================================================================

DESCRIPCIÓN GENERAL
-------------------
Este cliente TCP envía pedidos de productos al servidor concurrente.
Cuando el servidor responde que la cola está llena ({"tipo": "error",
"estado": "rechazado"}), el cliente NO abandona inmediatamente el pedido:
espera TIEMPO_ESPERA_REINTENTO segundos y vuelve a intentarlo, hasta un
máximo de MAX_REINTENTOS veces.

QUÉ ES NUEVO EN ESTA MODIFICACIÓN
-----------------------------------
  1. Constante MAX_REINTENTOS = 3
       Número máximo de intentos adicionales que el cliente hará para un
       pedido cuando recibe una respuesta de «cola llena».
       «Intento adicional» significa: el intento original NO cuenta como
       reintento. Si MAX_REINTENTOS=3, el cliente intentará en total
       hasta 4 veces (1 original + 3 reintentos).

  2. Constante TIEMPO_ESPERA_REINTENTO = 2  (segundos)
       Pausa entre reintentos. Permite que el servidor vacíe un poco la
       cola antes del siguiente intento. Sin esta pausa, reintentar
       inmediatamente probablemente volvería a recibir «cola llena».

  3. Función reintentar_pedido(socket_cliente, producto, cantidad, max_reintentos)
       Encapsula toda la lógica de reintentos. Recibe el socket ya conectado
       y lo reutiliza para los reintentos (cada reintento es una nueva conexión
       en realidad —ver diseño—). Retorna True si el pedido fue aceptado
       eventualmente, o False si se agotaron los reintentos.

  4. Contador de reintentos por pedido
       La función reintentar_pedido lleva un bucle con variable `intento`
       que va de 1 a max_reintentos. En cada iteración muestra en consola:
           'Reintento 1/3 en 2 segundos...'
           'Reintento 2/3 en 2 segundos...'
       etc.

  5. Registro de pedido fallido
       Si todos los reintentos se agotan, el cliente imprime un mensaje
       de error claro y el pedido queda marcado como fallido en la lista
       `resultados`. Luego continúa con el siguiente pedido sin bloquearse.

POR QUÉ ESTA LÓGICA ESTÁ EN EL CLIENTE Y NO EN EL SERVIDOR
------------------------------------------------------------
El principio de «responsabilidad única» dicta que:
  - El servidor decide CUÁNDO rechazar (cola llena) → lógica de capacidad.
  - El cliente decide SI reintentar y CUÁNTAS veces → política de resiliencia.

Si el servidor gestionara los reintentos internamente, crearía acoplamiento:
  a) El servidor no sabe cuánto tiempo puede esperar el cliente.
  b) El servidor bloquearía su propio hilo manejador ocupando un slot del
     semáforo durante la espera.
  c) Distintos clientes podrían tener distintas políticas de reintento.

Al poner la lógica en el cliente, cada cliente puede configurar sus propios
MAX_REINTENTOS y TIEMPO_ESPERA_REINTENTO de forma independiente.

PROTOCOLO DE COMUNICACIÓN
--------------------------
  Pedido enviado  →  {"tipo": "pedido", "producto": "...", "cantidad": N}

  Respuestas posibles del servidor:
    {"tipo": "respuesta", "estado": "encolado",   "mensaje": "..."}
        → pedido en cola; esperar segunda respuesta del procesador
    {"tipo": "respuesta", "estado": "ok",         "mensaje": "...", "stock_restante": N}
        → pedido procesado con éxito
    {"tipo": "respuesta", "estado": "rechazado",  "mensaje": "..."}
        → error de negocio (stock insuficiente, producto no existe, etc.)
    {"tipo": "error",     "estado": "rechazado",  "mensaje": "Cola llena. Reintente más tarde."}
        → cola llena → ACTIVAR LÓGICA DE REINTENTOS  ← MODIFICACIÓN 4

DIAGRAMA DE FLUJO DEL CLIENTE (por pedido)
-------------------------------------------
  enviar_pedido()
    ├─ conectar al servidor
    ├─ enviar JSON
    ├─ recibir respuesta
    │   ├─ "encolado"   → recibir segunda respuesta (del procesador)
    │   ├─ "ok"         → registrar éxito
    │   ├─ "rechazado"  → registrar fallo (error de negocio)
    │   └─ "error"/"rechazado" (cola llena)
    │        └─ reintentar_pedido()          ← NUEVO EN MOD-04
    │              ├─ intento 1: espera 2s → enviar → ¿ok? → retornar True
    │              ├─ intento 2: espera 2s → enviar → ¿ok? → retornar True
    │              ├─ intento 3: espera 2s → enviar → ¿ok? → retornar True
    │              └─ agotados → retornar False → registrar fallo
    └─ cerrar socket

==============================================================================
"""

import socket   # Comunicación TCP
import json     # Serialización del protocolo
import time     # time.sleep() para espera entre reintentos
import random   # Variación aleatoria en pedidos de prueba

# ===========================================================================
# CONSTANTES DE CONFIGURACIÓN
# ===========================================================================

HOST = "127.0.0.1"   # Dirección del servidor (debe coincidir con servidor.py)
PORT = 65000          # Puerto TCP del servidor
ENCODING = "utf-8"   # Codificación de texto para JSON
BUFFER_SIZE = 4096   # Bytes máximos leídos en un solo recv()

# ─────────────────────────────────────────────────────────────────────────────
# MODIFICACIÓN 4 — Nuevas constantes de reintento
# ─────────────────────────────────────────────────────────────────────────────

MAX_REINTENTOS = 3
# Número máximo de reintentos cuando la cola del servidor está llena.
# Valor elegido como compromiso entre persistencia y no saturar el servidor.
# Un valor muy alto (ej. 10) haría que el cliente espere demasiado.
# Un valor de 0 desactivaría los reintentos (comportamiento del cliente base).

TIEMPO_ESPERA_REINTENTO = 2
# Segundos que el cliente espera entre un reintento y el siguiente.
# Razón: dar tiempo al servidor para procesar pedidos y liberar espacio en
# la cola. Sin este delay, los reintentos inmediatos casi siempre fallarían
# porque la cola no tendría tiempo de drenarse.
# El valor debe ser mayor que el tiempo medio de procesamiento del servidor
# (0.5–1.5 s en este ejemplo) para tener una probabilidad razonable de éxito.

# ─────────────────────────────────────────────────────────────────────────────


# ===========================================================================
# FUNCIÓN: enviar_pedido_simple
# ===========================================================================

def enviar_pedido_simple(producto, cantidad):
    """
    Envía un único pedido al servidor y retorna la(s) respuesta(s).

    Esta función maneja el ciclo completo de una solicitud:
      1. Abrir conexión TCP.
      2. Serializar y enviar el pedido como JSON.
      3. Recibir la primera respuesta:
           - Si es ACK de encolado («encolado»), esperar una segunda respuesta
             del procesador.
           - Si es rechazo de cola llena («error»/«rechazado»), retornar
             esa respuesta para que el llamador decida si reintentar.
      4. Cerrar la conexión.

    PARÁMETROS
    ----------
    producto  : str   Nombre del producto a pedir.
    cantidad  : int   Unidades solicitadas.

    RETORNA
    -------
    dict | None
        Última respuesta JSON recibida del servidor, o None en caso de error
        de red. El llamador examina "tipo" y "estado" para decidir qué hacer.

    POR QUÉ RETORNAR LA RESPUESTA EN VEZ DE IMPRIMIR DIRECTAMENTE
    --------------------------------------------------------------
    Al retornar la respuesta, reintentar_pedido() puede examinarla y decidir
    si el rechazo fue «cola llena» (→ reintentar) o un error de negocio
    permanente (→ no reintentar). Si imprimiéramos dentro de esta función y
    no retornáramos nada, el llamador no podría distinguir los casos.
    """
    pedido = {
        "tipo": "pedido",
        "producto": producto,
        "cantidad": cantidad,
    }

    try:
        # Crear nuevo socket TCP para cada llamada.
        # En un sistema de producción usaríamos un pool de conexiones,
        # pero para claridad pedagógica creamos una conexión por pedido.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            s.settimeout(10)  # Timeout de 10 s para recv(); evita cuelgues

            # Serializar el pedido a JSON y enviarlo completo
            pedido_json = json.dumps(pedido, ensure_ascii=False)
            s.sendall(pedido_json.encode(ENCODING))

            # ── PRIMERA RESPUESTA ────────────────────────────────────────────
            datos_raw = s.recv(BUFFER_SIZE)
            if not datos_raw:
                print(f"[Cliente] Conexión cerrada inesperadamente por el servidor.")
                return None

            respuesta = json.loads(datos_raw.decode(ENCODING))

            # Si el servidor confirmó que el pedido fue encolado,
            # hay una segunda respuesta pendiente: la del procesador.
            if respuesta.get("estado") == "encolado":
                print(
                    f"[Cliente] Pedido encolado en servidor. "
                    f"Esperando resultado de procesamiento..."
                )
                # ── SEGUNDA RESPUESTA (del procesador) ───────────────────────
                datos_raw2 = s.recv(BUFFER_SIZE)
                if datos_raw2:
                    respuesta = json.loads(datos_raw2.decode(ENCODING))
                else:
                    print("[Cliente] No se recibió respuesta del procesador.")
                    return None

            return respuesta

    except ConnectionRefusedError:
        print(f"[Cliente] No se pudo conectar a {HOST}:{PORT}. ¿Servidor activo?")
        return None
    except socket.timeout:
        print(f"[Cliente] Timeout esperando respuesta del servidor.")
        return None
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"[Cliente] Error al parsear respuesta JSON: {e}")
        return None
    except OSError as e:
        print(f"[Cliente] Error de socket: {e}")
        return None


# ===========================================================================
# FUNCIÓN: reintentar_pedido   ← NUEVA EN MODIFICACIÓN 4
# ===========================================================================

def reintentar_pedido(producto, cantidad, max_reintentos):
    """
    Intenta enviar un pedido múltiples veces cuando la cola del servidor está llena.

    MODIFICACIÓN 4 — Esta es la función central de la nueva funcionalidad.

    PARÁMETROS
    ----------
    producto       : str
        Nombre del producto a pedir.
    cantidad       : int
        Unidades solicitadas.
    max_reintentos : int
        Número máximo de reintentos adicionales al intento original.
        Se pasa como parámetro (en vez de usar la constante directamente)
        para que la función sea reutilizable y testeable con distintos valores.

    RETORNA
    -------
    dict | None
        - Si algún reintento tuvo éxito: la respuesta final del servidor.
        - Si todos los reintentos fallaron: None.

    LÓGICA INTERNA
    --------------
    El bucle itera desde intento=1 hasta max_reintentos (inclusive).
    En cada iteración:
      a) Muestra «Reintento N/MAX en TIEMPO segundos...»
      b) Duerme TIEMPO_ESPERA_REINTENTO segundos (time.sleep)
      c) Llama a enviar_pedido_simple() para hacer el intento real
      d) Examina la respuesta:
           - Si es None o "error"/"rechazado" → continúa el bucle
           - Cualquier otro caso (ok, encolado) → retorna la respuesta

    POR QUÉ time.sleep() Y NO OTRO MECANISMO
    ----------------------------------------
    time.sleep() bloquea el hilo actual durante TIEMPO_ESPERA_REINTENTO
    segundos. Esto es suficiente para un cliente de un solo hilo.
    Si el cliente fuera multihilo, usaríamos threading.Event.wait() con
    timeout para poder cancelar la espera si se recibe una señal de parada.

    POR QUÉ NO USAR BACKOFF EXPONENCIAL
    ------------------------------------
    El backoff exponencial (2s, 4s, 8s...) es mejor en sistemas de alta
    carga, pero para esta práctica didáctica usamos un intervalo fijo
    (TIEMPO_ESPERA_REINTENTO) para que el comportamiento sea predecible
    y fácil de observar en consola.

    EJEMPLO DE SALIDA EN CONSOLA
    ----------------------------
        [Cliente] Cola llena detectada para 'Laptop' x3.
        [Cliente] Reintento 1/3 en 2 segundos...
        [Cliente] Reintento 2/3 en 2 segundos...
        [Cliente] Pedido de 'Laptop' x3 aceptado en reintento 2.
    """
    print(
        f"[Cliente] Cola llena detectada para '{producto}' x{cantidad}."
    )

    for intento in range(1, max_reintentos + 1):
        # ── MOSTRAR MENSAJE DE REINTENTO (formato requerido por el enunciado) ──
        print(
            f"[Cliente] Reintento {intento}/{max_reintentos} "
            f"en {TIEMPO_ESPERA_REINTENTO} segundos..."
        )

        # ── ESPERAR ANTES DE REINTENTAR ────────────────────────────────────────
        # Pausa para dar tiempo al servidor de procesar pedidos y liberar espacio
        # en su cola. Sin esta pausa, reintentar inmediatamente casi siempre
        # resultaría en otro rechazo por «cola llena».
        time.sleep(TIEMPO_ESPERA_REINTENTO)

        # ── HACER EL INTENTO ───────────────────────────────────────────────────
        respuesta = enviar_pedido_simple(producto, cantidad)

        if respuesta is None:
            # Error de red; no tiene sentido reintentar si no hay conectividad
            print(
                f"[Cliente] Error de red en reintento {intento}/{max_reintentos}. "
                f"Abandonando pedido."
            )
            return None

        # ── VERIFICAR SI EL REINTENTO TUVO ÉXITO ──────────────────────────────
        tipo_resp  = respuesta.get("tipo",   "")
        estado_resp = respuesta.get("estado", "")

        if tipo_resp == "error" and estado_resp == "rechazado":
            # Sigue siendo «cola llena»; continuar reintentando si quedan intentos
            if intento < max_reintentos:
                print(
                    f"[Cliente] Reintento {intento}/{max_reintentos} rechazado "
                    f"(cola aún llena). Continuando..."
                )
            continue  # Ir a la siguiente iteración del bucle

        # Cualquier otra respuesta (ok, encolado→procesado, rechazado por stock, etc.)
        # significa que el servidor recibió el pedido (aunque pueda ser un error de negocio)
        print(
            f"[Cliente] Pedido de '{producto}' x{cantidad} "
            f"aceptado por el servidor en reintento {intento}."
        )
        return respuesta

    # ── REINTENTOS AGOTADOS ────────────────────────────────────────────────────
    # Se llegó aquí porque todos los reintentos recibieron «cola llena»
    print(
        f"[Cliente] ✗ Se agotaron los {max_reintentos} reintentos para "
        f"'{producto}' x{cantidad}. Pedido marcado como FALLIDO."
    )
    return None


# ===========================================================================
# FUNCIÓN: procesar_respuesta
# ===========================================================================

def procesar_respuesta(respuesta, producto, cantidad):
    """
    Interpreta y muestra en consola la respuesta final del servidor.

    PARÁMETROS
    ----------
    respuesta : dict | None
        Respuesta del servidor. None indica error de red o reintentos agotados.
    producto  : str
        Nombre del producto (para mensajes de log).
    cantidad  : int
        Cantidad pedida (para mensajes de log).

    RETORNA
    -------
    str
        Estado del pedido: "exitoso", "fallido_negocio" o "fallido_red".

    POR QUÉ SEPARAR ESTA LÓGICA EN UNA FUNCIÓN
    -------------------------------------------
    Separar la interpretación de la respuesta del envío del pedido sigue el
    principio de responsabilidad única y hace que main() sea más legible:
    cada función hace una sola cosa.
    """
    if respuesta is None:
        print(
            f"[Cliente] ✗ Pedido '{producto}' x{cantidad}: "
            f"FALLIDO — sin respuesta del servidor."
        )
        return "fallido_red"

    estado = respuesta.get("estado", "desconocido")
    mensaje = respuesta.get("mensaje", "")

    if estado == "ok":
        stock_restante = respuesta.get("stock_restante", "N/A")
        print(
            f"[Cliente] ✓ Pedido '{producto}' x{cantidad}: "
            f"PROCESADO OK. Stock restante: {stock_restante}. — {mensaje}"
        )
        return "exitoso"

    elif estado == "rechazado":
        print(
            f"[Cliente] ✗ Pedido '{producto}' x{cantidad}: "
            f"RECHAZADO. — {mensaje}"
        )
        return "fallido_negocio"

    else:
        print(
            f"[Cliente] ? Pedido '{producto}' x{cantidad}: "
            f"Estado desconocido '{estado}'. — {mensaje}"
        )
        return "fallido_red"


# ===========================================================================
# FUNCIÓN: enviar_pedido_con_reintentos   ← NUEVA EN MODIFICACIÓN 4
# ===========================================================================

def enviar_pedido_con_reintentos(producto, cantidad):
    """
    Orquesta el envío de un pedido con lógica de reintentos automáticos.

    Esta función es el punto de entrada principal para enviar un pedido
    desde main(). Combina enviar_pedido_simple() y reintentar_pedido()
    de forma transparente para el llamador:

      1. Intenta enviar el pedido por primera vez.
      2. Si el servidor responde «cola llena» → delega a reintentar_pedido().
      3. Si el resultado final es exitoso o error de negocio → lo registra.
      4. Si los reintentos se agotan → registra como fallido.

    PARÁMETROS
    ----------
    producto : str   Nombre del producto.
    cantidad : int   Unidades solicitadas.

    RETORNA
    -------
    str
        "exitoso", "fallido_negocio" o "fallido_red".
    """
    print(f"\n[Cliente] ─── Enviando pedido: '{producto}' x{cantidad} ───")

    # ── PRIMER INTENTO ─────────────────────────────────────────────────────────
    respuesta = enviar_pedido_simple(producto, cantidad)

    if respuesta is None:
        # Error de red en el primer intento
        return procesar_respuesta(None, producto, cantidad)

    # ── DETECTAR «COLA LLENA» (tipo: error, estado: rechazado) ────────────────
    # MODIFICACIÓN 4: Este es el punto donde se activa la lógica de reintentos.
    # Condición: tipo=="error" Y estado=="rechazado"
    # Esta combinación específica la emite el servidor SOLO cuando la cola está
    # llena (en manejar_cliente cuando catch queue.Full).
    # No confundir con tipo=="respuesta" + estado=="rechazado", que indica
    # error de negocio (stock insuficiente, etc.) que NO debe reintentarse.
    if (respuesta.get("tipo") == "error"
            and respuesta.get("estado") == "rechazado"):
        # ── ACTIVAR REINTENTOS AUTOMÁTICOS ────────────────────────────────────
        respuesta_final = reintentar_pedido(producto, cantidad, MAX_REINTENTOS)
        return procesar_respuesta(respuesta_final, producto, cantidad)

    # ── RESPUESTA NORMAL (no es cola llena) ────────────────────────────────────
    return procesar_respuesta(respuesta, producto, cantidad)


# ===========================================================================
# FUNCIÓN PRINCIPAL: main
# ===========================================================================

def main():
    """
    Punto de entrada del cliente.

    Envía una lista predefinida de pedidos al servidor, usando la lógica de
    reintentos automáticos (Modificación 4) cuando la cola está llena.

    ESTRUCTURA DE main()
    --------------------
    1. Define la lista de pedidos a enviar.
    2. Itera sobre los pedidos llamando a enviar_pedido_con_reintentos().
    3. Acumula resultados en un diccionario de estadísticas.
    4. Imprime un resumen final.

    NOTAS SOBRE EL DISEÑO DE PRUEBA
    --------------------------------
    La lista de pedidos es deliberadamente larga (más de CAPACIDAD_MAXIMA_COLA=10)
    para provocar situaciones de «cola llena» y observar los reintentos en acción.
    Se añade un delay corto entre pedidos para simular llegada distribuida de clientes.
    """
    print("=" * 60)
    print("  CLIENTE — MODIFICACIÓN 4: Reintento automático")
    print("=" * 60)
    print(f"  Servidor:             {HOST}:{PORT}")
    print(f"  Máx. reintentos:      {MAX_REINTENTOS}")
    print(f"  Tiempo entre reintentos: {TIEMPO_ESPERA_REINTENTO} segundos")
    print("=" * 60)

    # ── LISTA DE PEDIDOS DE PRUEBA ─────────────────────────────────────────────
    # Lista extensa para provocar «cola llena» y disparar los reintentos.
    # En un sistema real, estos pedidos vendrían de input del usuario o de un archivo.
    pedidos = [
        ("Laptop",      2),
        ("Mouse",       5),
        ("Teclado",     3),
        ("Monitor",     1),
        ("Auriculares", 4),
        ("USB",         10),
        ("Cargador",    2),
        ("Webcam",      3),
        ("Laptop",      1),
        ("Mouse",       8),
        ("Teclado",     5),
        ("Monitor",     2),
        ("Auriculares", 6),
        ("USB",         15),
        ("Cargador",    7),
    ]

    # ── ESTADÍSTICAS ──────────────────────────────────────────────────────────
    resultados = {
        "exitoso":       0,
        "fallido_negocio": 0,
        "fallido_red":   0,
    }

    # ── BUCLE PRINCIPAL DE ENVÍO ───────────────────────────────────────────────
    for producto, cantidad in pedidos:
        estado = enviar_pedido_con_reintentos(producto, cantidad)
        resultados[estado] = resultados.get(estado, 0) + 1

        # Pequeña pausa entre pedidos para no saturar el servidor desde el inicio
        # y para que los logs sean legibles. En una prueba de stress se eliminaría.
        time.sleep(0.3)

    # ── RESUMEN FINAL ──────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  RESUMEN DE PEDIDOS")
    print("=" * 60)
    print(f"  ✓ Exitosos:               {resultados['exitoso']}")
    print(f"  ✗ Fallidos (negocio):     {resultados['fallido_negocio']}")
    print(f"  ✗ Fallidos (red/reintentos): {resultados['fallido_red']}")
    print(f"  Total pedidos enviados:   {len(pedidos)}")
    print("=" * 60)


# ===========================================================================
# PUNTO DE ENTRADA
# ===========================================================================

if __name__ == "__main__":
    main()
