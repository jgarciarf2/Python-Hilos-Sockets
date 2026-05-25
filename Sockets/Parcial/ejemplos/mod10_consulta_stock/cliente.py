"""
================================================================================
CLIENTE CON CONSULTA DE STOCK PREVIA (PRODUCTOR REMOTO)
MODIFICACIÓN 10: CONSULTA DE STOCK ANTES DE ENVIAR UN PEDIDO
================================================================================

PROPÓSITO GENERAL DEL ARCHIVO:
    - Implementar un CLIENTE TCP que se conecta al servidor de la central de pedidos.
    - El cliente actúa como PRODUCTOR: genera pedidos de productos.
    - MODIFICACIÓN 10 (NOVEDAD):
      Antes de enviar cualquier pedido, el cliente realiza una CONSULTA DE STOCK
      al servidor para comprobar si el producto deseado tiene unidades disponibles.
      * Si el stock disponible es > 0: envía el pedido normalmente.
      * Si el stock disponible es = 0: descarta ese producto, busca otro diferente
        e intenta de nuevo.
      * Si después de 3 intentos consecutivos todos los productos elegidos tienen
        stock 0, el cliente desiste (termina su ejecución) para no saturar al 
        servidor con consultas inútiles.

PROBLEMA QUE RESUELVE ESTA MODIFICACIÓN:
    - En sistemas concurrentes de alta transaccionalidad, enviar un pedido de un
      producto agotado representa un DESPERDICIO de recursos extremo:
        1. Se consume ancho de banda y latencia de red innecesariamente.
        2. Se ocupa un espacio en la cola de procesamiento del servidor, pudiendo
           causar que el semáforo bloquee a otros productores con pedidos válidos.
        3. El hilo procesador (consumidor) pierde tiempo retirando el pedido,
           adquiriendo el lock del stock para terminar rechazándolo al final.
    - Con la consulta previa de stock, filtramos los pedidos inviables en el lado
      del cliente, optimizando el rendimiento global del sistema distribuido.
    - NOTA DE CONCURRENCIA: A pesar de la consulta, puede darse una condición de
      carrera (race condition) si otro procesador consume el último stock justo en
      el milisegundo intermedio entre la consulta del cliente y el despacho del 
      servidor. Por eso, el servidor mantiene su lógica de validación atómica 
      (Lock) durante el despacho.

PROTOCOLO DE COMUNICACIÓN AMPLIADO (JSON sobre TCP):
    Cliente → Servidor:
        - Consulta Stock: {"tipo": "consulta_stock", "producto": "Laptop"}
        - Pedido:         {"tipo": "pedido", "producto": "Laptop", "cantidad": 2}
        - Fin:            {"tipo": "fin"}

    Servidor → Cliente:
        - Respuesta Stock: {"tipo": "respuesta_stock", "producto": "Laptop", "disponible": 10}
        - Bienvenida:      {"tipo": "bienvenida", "mensaje": "...", "productos_disponibles": [...], "tu_id": "..."}
        - Confirmación:    {"tipo": "confirmacion", "mensaje": "...", "estado": "en_cola"}
        - Error:           {"tipo": "error", "mensaje": "...", "estado": "rechazado"}

EJECUCIÓN:
    python cliente.py
================================================================================
"""

import socket
import time
import json
import random

# ==============================================================================
# CONSTANTES GLOBALES
# ==============================================================================
HOST = "127.0.0.1"
PORT = 65000
ENCODING = "utf-8"
BUFFER_SIZE = 4096

def log(mensaje):
    """
    Imprime un mensaje con marca de tiempo.
    """
    hora_actual = time.strftime("%H:%M:%S")
    print(f"[{hora_actual}] [Cliente-Stock] {mensaje}")

def consultar_stock(cliente_socket, producto):
    """
    [MOD10] Envía un mensaje de tipo 'consulta_stock' al servidor y espera la respuesta.
    Retorna la cantidad disponible reportada de manera atómica por el servidor.

    Parámetros:
        cliente_socket (socket.socket): Socket conectado al servidor.
        producto (str): Nombre del producto a consultar.

    Retorna:
        int: Cantidad de unidades disponibles en el stock del servidor.
    """
    log(f"📋 Consultando stock de '{producto}'...")
    
    consulta = {
        "tipo": "consulta_stock",
        "producto": producto
    }
    
    # Enviar consulta
    cliente_socket.sendall(json.dumps(consulta).encode(ENCODING))
    
    # Recibir respuesta
    datos_recibidos = cliente_socket.recv(BUFFER_SIZE)
    if not datos_recibidos:
        log("⚠ El servidor cerró la conexión al consultar el stock.")
        return 0

    respuesta = json.loads(datos_recibidos.decode(ENCODING))
    
    # Validar formato
    if respuesta.get("tipo") == "respuesta_stock":
        disponible = respuesta.get("disponible", 0)
        log(f"📋 Stock de '{producto}': {disponible} unidades disponibles.")
        return disponible
    else:
        log(f"⚠ Respuesta inesperada al consultar stock: {respuesta}")
        return 0

def enviar_pedido(cliente_socket, producto, cantidad):
    """
    Envía un pedido al servidor en formato JSON y recibe la confirmación síncrona.
    """
    pedido = {
        "tipo": "pedido",
        "producto": producto,
        "cantidad": cantidad
    }
    log(f"🛒 Enviando pedido: {cantidad}x {producto}...")
    cliente_socket.sendall(json.dumps(pedido).encode(ENCODING))
    
    # Esperar confirmación
    datos_recibidos = cliente_socket.recv(BUFFER_SIZE)
    if not datos_recibidos:
        log("⚠ El servidor cerró la conexión al enviar el pedido.")
        return False

    respuesta = json.loads(datos_recibidos.decode(ENCODING))
    tipo = respuesta.get("tipo")
    
    if tipo == "confirmacion":
        log(f"✓ Confirmado por servidor: {respuesta.get('mensaje')} [Estado: {respuesta.get('estado')}]")
        return True
    elif tipo == "error":
        log(f"✗ RECHAZADO por servidor: {respuesta.get('mensaje')} [Estado: {respuesta.get('estado')}]")
        return False
    else:
        log(f"Mensaje desconocido: {respuesta}")
        return False

def ejecutar_cliente():
    cliente_socket = None
    try:
        # --- PASO 1: Crear y conectar el socket TCP ---
        log(f"Conectando al servidor en {HOST}:{PORT}...")
        cliente_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cliente_socket.connect((HOST, PORT))
        
        # --- PASO 2: Recibir bienvenida y lista de productos ---
        datos_bienvenida = cliente_socket.recv(BUFFER_SIZE)
        if not datos_bienvenida:
            log("No se recibieron datos de bienvenida del servidor.")
            return

        bienvenida = json.loads(datos_bienvenida.decode(ENCODING))
        productos = bienvenida["productos_disponibles"]
        mi_id = bienvenida["tu_id"]
        
        print("\n" + "=" * 60)
        print(f"   CONEXIÓN ESTABLECIDA CON ÉXITO — {mi_id}")
        print(f"   Mensaje del Servidor: {bienvenida['mensaje']}")
        print(f"   Productos de catálogo: {productos}")
        print("=" * 60 + "\n")

        # --- PASO 3: Generar y procesar pedidos con consulta previa de stock ---
        num_pedidos_deseados = random.randint(2, 5)
        log(f"Se intentará realizar {num_pedidos_deseados} pedidos exitosos.")
        
        pedidos_exitosos = 0
        intentos_totales = 0
        
        # Mientras no alcancemos la meta y no nos hayamos agotado
        while pedidos_exitosos < num_pedidos_deseados:
            intentos_totales += 1
            if intentos_totales > 10:
                log("⚠ Se alcanzó el límite máximo de intentos globales del cliente (10). Parando.")
                break

            # Elegir un producto al azar
            producto_candidato = random.choice(productos)
            log(f"\n--- Intento Pedido #{pedidos_exitosos + 1} (Filtro Pre-Stock) ---")
            
            # [MOD10] CONSULTAR STOCK ANTES
            conteo_intentos_producto = 0
            stock_disponible = 0
            
            while conteo_intentos_producto < 3:
                stock_disponible = consultar_stock(cliente_socket, producto_candidato)
                
                if stock_disponible > 0:
                    # ¡Excelente! Hay stock disponible
                    break
                else:
                    conteo_intentos_producto += 1
                    log(f"⚠ Producto '{producto_candidato}' agotado (Stock = 0). "
                        f"Descartado [Intento {conteo_intentos_producto}/3].")
                    
                    if conteo_intentos_producto < 3:
                        # Seleccionamos otro candidato para probar
                        producto_candidato = random.choice(productos)
                        log(f"Cambiando a nuevo candidato: '{producto_candidato}'")
                    else:
                        log("✗ Se agotaron los 3 intentos consecutivos de selección de productos. "
                            "Todos los productos probados no tienen stock en este momento.")
            
            # Si después de los reintentos el stock sigue siendo 0, cancelamos la ejecución
            if stock_disponible == 0:
                log("🛑 Terminando ejecución del cliente prematuramente para evitar saturar al servidor.")
                break
            
            # Si hay stock, definimos cantidad (no mayor que el stock y máximo 4)
            cantidad = min(random.randint(1, 4), stock_disponible)
            
            # Enviar el pedido real
            exito = enviar_pedido(cliente_socket, producto_candidato, cantidad)
            if exito:
                pedidos_exitosos += 1
            
            # Esperar 2 segundos antes de la siguiente interacción
            time.sleep(2.0)

        # --- PASO 4: Enviar señal de FIN al servidor ---
        print("\n" + "-" * 50)
        log("Sesión finalizada. Enviando señal de FIN...")
        mensaje_fin = {"tipo": "fin"}
        cliente_socket.sendall(json.dumps(mensaje_fin).encode(ENCODING))
        
        # Esperar respuesta final
        datos_fin = cliente_socket.recv(BUFFER_SIZE)
        if datos_fin:
            respuesta_fin = json.loads(datos_fin.decode(ENCODING))
            log(f"Servidor confirma: {respuesta_fin.get('mensaje')}")

    except ConnectionRefusedError:
        log("ERROR: No se pudo conectar al servidor. ¿Está el servidor corriendo?")
    except Exception as e:
        log(f"ERROR INESPERADO: {e}")
    finally:
        if cliente_socket:
            cliente_socket.close()
            log("Socket cerrado correctamente. ¡Hasta luego!")

if __name__ == "__main__":
    ejecutar_cliente()
