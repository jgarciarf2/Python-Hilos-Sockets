"""
================================================================================
CLIENTE - CENTRAL DE PEDIDOS CON BROADCAST (PRODUCTOR REMOTO)
MODIFICACIÓN 6: BROADCAST DEL ESTADO DE LA COLA Y DESPACHOS
================================================================================

PROPÓSITO GENERAL DEL ARCHIVO:
    - Implementar un CLIENTE TCP que se conecta al servidor de la central de pedidos.
    - El cliente actúa como PRODUCTOR: genera pedidos aleatorios de productos.
    - MODIFICACIÓN 6 (NOVEDAD): 
      El cliente inicia un HILO SEPARADO de escucha para recibir notificaciones en 
      tiempo real (broadcasts) enviadas por el servidor a todos los clientes.
      Después de enviar todos sus pedidos, el cliente permanece conectado durante 
      10 segundos para observar y reportar los broadcasts de despachos que ocurren 
      en el servidor en tiempo real.

POR QUÉ SE USA UN HILO DE ESCUCHA (threading.Thread):
    - Los sockets TCP son bidireccionales y de flujo continuo, pero por defecto 
      las llamadas a socket.recv() son bloqueantes.
    - Si el hilo principal del cliente estuviera enviando pedidos o durmiendo 
      (time.sleep()), no podría leer los datos entrantes del socket de manera
      síncrona sin bloquear la interactividad o retrasar la recepción de eventos.
    - Al delegar la lectura en un HILO DE ESCUCHA BACKGROUND (daemon), logramos
      concurrencia de entrada/salida (I/O Concurrency): el cliente puede enviar
      mensajes y dormir libremente, mientras que cualquier broadcast enviado por
      el servidor es leído y procesado instantáneamente por el hilo de escucha.

PROTOCOLO DE COMUNICACIÓN (JSON sobre TCP):
    Cliente → Servidor:
        - Pedido:   {"tipo": "pedido", "producto": "Laptop", "cantidad": 2}
        - Fin:      {"tipo": "fin"}

    Servidor → Cliente:
        - Bienvenida: {"tipo": "bienvenida", "mensaje": "...", "productos_disponibles": [...], "tu_id": "..."}
        - Confirmación: {"tipo": "confirmacion", "mensaje": "...", "estado": "en_cola"}
        - Error:      {"tipo": "error", "mensaje": "...", "estado": "rechazado"}
        - Broadcast:  {"tipo": "broadcast", "mensaje": "Pedido de Cliente-1: 2x Laptop - DESPACHADO"}

EJECUCIÓN:
    python cliente.py
================================================================================
"""

import socket
import threading
import time
import random
import json

# ==============================================================================
# CONSTANTES GLOBALES
# ==============================================================================
HOST = "127.0.0.1"
PORT = 65000
ENCODING = "utf-8"
BUFFER_SIZE = 4096

# [MOD6] Constante para indicar cuánto tiempo esperar después de enviar todo
TIEMPO_ESPERA_BROADCASTS = 10.0

# Evento para coordinar la terminación segura del hilo de escucha
evento_terminar_escucha = threading.Event()

def log(mensaje):
    """
    Imprime un mensaje con marca de tiempo y el nombre del hilo ejecutor.
    """
    hora_actual = time.strftime("%H:%M:%S")
    nombre_hilo = threading.current_thread().name
    print(f"[{hora_actual}] [{nombre_hilo}] {mensaje}")

def hilo_escucha_broadcast(cliente_socket):
    """
    [MOD6] Función ejecutada por el hilo de escucha.
    Lee continuamente datos del socket TCP e interpreta los mensajes.

    Parámetros:
        cliente_socket (socket.socket): Socket conectado al servidor.
    """
    log("Hilo de escucha de broadcasts INICIADO.")
    
    # Configuramos un timeout en el socket para que no se quede bloqueado eternamente
    # y podamos revisar el evento de terminación periódicamente.
    cliente_socket.settimeout(1.0)

    while not evento_terminar_escucha.is_set():
        try:
            # Recibir datos del servidor
            datos = cliente_socket.recv(BUFFER_SIZE)
            
            if not datos:
                # Si se reciben bytes vacíos sin excepción, el servidor cerró la conexión
                log("El servidor ha cerrado la conexión (EOF). Terminando hilo de escucha.")
                break

            mensaje_texto = datos.decode(ENCODING)
            
            # En TCP, debido al efecto "coalescencia de paquetes" (Nagle), múltiples
            # mensajes JSON podrían llegar juntos en una sola lectura de buffer.
            # Por eso separamos por llaves de cierre/apertura o los parseamos con cuidado.
            # Asumimos que los mensajes JSON individuales están completos en cada envío.
            try:
                mensaje_datos = json.loads(mensaje_texto)
                tipo = mensaje_datos.get("tipo")

                if tipo == "broadcast":
                    # [MOD6] Mensaje de difusión general para todos los clientes
                    print(f"\n📢 [BROADCAST] {mensaje_datos.get('mensaje')}\n")
                elif tipo == "confirmacion":
                    log(f"Respuesta del Servidor (Pedido): {mensaje_datos.get('mensaje')} [Estado: {mensaje_datos.get('estado')}]")
                elif tipo == "error":
                    log(f"⚠ ERROR del Servidor: {mensaje_datos.get('mensaje')} [Estado: {mensaje_datos.get('estado')}]")
                elif tipo == "fin_confirmado":
                    log(f"Cierre de sesión confirmado por servidor: {mensaje_datos.get('mensaje')}")
                else:
                    log(f"Mensaje recibido: {mensaje_datos}")

            except json.JSONDecodeError:
                # Si falló por JSON incompleto o múltiple, intentamos manejar fragmentación simple.
                # Para simplificar y mantener robustez de taller, registramos el texto crudo.
                log(f"Mensaje crudo recibido del servidor: {mensaje_texto}")

        except socket.timeout:
            # El timeout de 1s expiró sin datos, volvemos a iterar para chequear evento_terminar_escucha
            continue
        except OSError as e:
            # El socket se cerró intencionalmente desde el hilo principal o hubo un fallo de red
            if evento_terminar_escucha.is_set():
                # Cierre esperado
                break
            log(f"Excepción en socket del hilo de escucha: {e}")
            break
        except Exception as e:
            log(f"Error inesperado en hilo de escucha: {e}")
            break

    log("Hilo de escucha de broadcasts FINALIZADO.")

def enviar_pedido(cliente_socket, producto, cantidad):
    """
    Envía un pedido de producto en formato JSON.
    """
    pedido = {
        "tipo": "pedido",
        "producto": producto,
        "cantidad": cantidad
    }
    log(f"Enviando pedido: {cantidad}x {producto}...")
    cliente_socket.sendall(json.dumps(pedido).encode(ENCODING))

def ejecutar_cliente():
    """
    Flujo principal de ejecución del cliente.
    """
    cliente_socket = None
    try:
        # --- PASO 1: Crear y conectar el socket TCP ---
        cliente_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cliente_socket.connect((HOST, PORT))
        
        # --- PASO 2: Recibir bienvenida con lista de productos ---
        # Inicialmente leemos de forma síncrona el mensaje de bienvenida
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
        print(f"   Productos disponibles en stock: {productos}")
        print("=" * 60 + "\n")

        # --- PASO 3: Iniciar HILO DE ESCUCHA para broadcasts concurrentes ---
        # Lanzamos el hilo antes de enviar pedidos para poder capturar cualquier
        # confirmación o broadcast instantáneo.
        hilo_lector = threading.Thread(
            target=hilo_escucha_broadcast,
            args=(cliente_socket,),
            name="Hilo-Escucha",
            daemon=True
        )
        hilo_lector.start()

        # --- PASO 4: Generar y enviar pedidos aleatorios ---
        num_pedidos = random.randint(1, 5)
        log(f"Se generarán y enviarán {num_pedidos} pedidos de forma secuencial.")

        for i in range(1, num_pedidos + 1):
            producto = random.choice(productos)
            cantidad = random.randint(1, 4)
            
            # Mandar pedido por el socket
            enviar_pedido(cliente_socket, producto, cantidad)
            
            # Pausa de 1 a 3 segundos entre pedidos
            pausa = random.uniform(1.0, 3.0)
            time.sleep(pausa)

        # --- PASO 5: [MOD6] Esperar y monitorear broadcasts del servidor ---
        log(f"Todos los pedidos fueron enviados. Entrando en modo MONITOREO de broadcasts.")
        log(f"El cliente permanecerá conectado {TIEMPO_ESPERA_BROADCASTS} segundos "
            "recibiendo notificaciones de despachos en tiempo real...")
        
        # Dormimos el hilo principal; el hilo de escucha imprimirá los broadcasts
        time.sleep(TIEMPO_ESPERA_BROADCASTS)

        # --- PASO 6: Enviar mensaje de FIN ---
        log("Terminó el tiempo de espera. Enviando señal de FIN al servidor...")
        mensaje_fin = {"tipo": "fin"}
        cliente_socket.sendall(json.dumps(mensaje_fin).encode(ENCODING))
        
        # Damos un pequeño momento para que llegue la confirmación de fin al hilo de escucha
        time.sleep(1.0)

    except ConnectionRefusedError:
        log("ERROR: No se pudo establecer conexión. ¿Está corriendo el servidor?")
    except Exception as e:
        log(f"ERROR INESPERADO: {e}")
    finally:
        # Apagar de forma segura el hilo de escucha
        evento_terminar_escucha.set()
        if cliente_socket:
            cliente_socket.close()
            log("Conexión del socket cerrada. ¡Hasta luego!")

if __name__ == "__main__":
    ejecutar_cliente()
