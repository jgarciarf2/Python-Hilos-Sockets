"""
================================================================================
CLIENTE INTERACTIVO - CENTRAL DE PEDIDOS (PRODUCTOR REMOTO)
MODIFICACIÓN 8: CANTIDAD FIJA DE PEDIDOS E INPUT DE USUARIO INTERACTIVO
================================================================================

PROPÓSITO GENERAL DEL ARCHIVO:
    - Implementar un CLIENTE TCP interactivo que se conecta al servidor de la 
      central de pedidos.
    - MODIFICACIÓN 8 (NOVEDAD):
      En lugar de generar pedidos aleatorios automáticamente, este cliente
      interactúa directamente con el usuario por consola para:
        1. Definir cuántos pedidos quiere enviar (validado entre 1 y 10).
        2. Seleccionar interactivamente el producto de una lista numerada de
           los productos que el servidor reportó como disponibles.
        3. Permitir elegir de forma aleatoria si el usuario simplemente presiona
           ENTER en la selección.
        4. Manejar de forma robusta e impecable la señal de interrupción por teclado
           (Ctrl+C o KeyboardInterrupt), de modo que si el usuario decide salir,
           se informe al servidor para no dejar sockets ni recursos "colgados" 
           en el servidor.

POR QUÉ ES IMPORTANTE ESTE MANEJO INTERACTIVO EN CONCURRENCIA:
    - En el diseño de sistemas distribuidos o cliente-servidor concurrentes,
      el servidor debe ser tolerante a fallos del cliente y a cierres lentos o
      rápidos.
    - El cliente interactivo puede tardar minutos en ingresar información. Mientras
      el usuario escribe, el hilo de conexión en el servidor está bloqueado en 
      recv(). Esto demuestra cómo el servidor concurrentemente mantiene hilos
      activos ("idle") esperando por entrada de red de diferentes clientes.
    - Validar las entradas de usuario en el cliente evita saturar la red con
      mensajes malformados o solicitudes que el servidor terminaría rechazando,
      ahorrando CPU y ancho de banda en la central de pedidos.

PROTOCOLO DE COMUNICACIÓN (JSON sobre TCP):
    Cliente → Servidor:
        - Pedido:   {"tipo": "pedido", "producto": "Laptop", "cantidad": 2}
        - Fin:      {"tipo": "fin"}

    Servidor → Cliente:
        - Bienvenida: {"tipo": "bienvenida", "mensaje": "...", "productos_disponibles": [...], "tu_id": "..."}
        - Confirmación: {"tipo": "confirmacion", "mensaje": "...", "estado": "en_cola"}
        - Error:      {"tipo": "error", "mensaje": "...", "estado": "rechazado"}

EJECUCIÓN:
    python cliente.py
================================================================================
"""

import socket
import time
import json
import random
import sys

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
    print(f"[{hora_actual}] [Cliente-Interactivo] {mensaje}")

def pedir_numero_pedidos():
    """
    [MOD8] Solicita interactivamente al usuario la cantidad de pedidos a realizar.
    Realiza un bucle de validación robusto garantizando un entero entre 1 y 10.

    Retorna:
        int: Número de pedidos validados a enviar.
    """
    while True:
        try:
            entrada = input("\n👉 ¿Cuántos pedidos desea enviar al servidor (1-10)? ").strip()
            
            # Validar si el usuario no escribió nada
            if not entrada:
                print("⚠ Entrada vacía. Por favor ingrese un número entero entre 1 y 10.")
                continue

            num = int(entrada)
            
            # Validar rango
            if 1 <= num <= 10:
                return num
            else:
                print("⚠ Número fuera de rango. Debe ser entre 1 y 10.")
        except ValueError:
            print("⚠ Entrada no válida. Debe ingresar un número entero.")
        except (KeyboardInterrupt, EOFError):
            # Propagamos la excepción de interrupción para que la maneje el flujo principal
            raise KeyboardInterrupt

def elegir_producto(productos):
    """
    [MOD8] Presenta al usuario una lista numerada de productos disponibles.
    Permite seleccionar uno ingresando su número o presionar ENTER para aleatorio.

    Parámetros:
        productos (list[str]): Lista de nombres de productos que reportó el servidor.

    Retorna:
        str: El nombre del producto elegido.
    """
    print("\n📦 Productos Disponibles:")
    for idx, prod in enumerate(productos, 1):
        print(f"   {idx}. {prod}")
    print("   [O simplemente presione ENTER para elegir un producto aleatorio]")

    while True:
        try:
            seleccion = input("👉 Seleccione el número de producto deseado: ").strip()
            
            # ENTER = Aleatorio
            if not seleccion:
                elegido = random.choice(productos)
                log(f"🎲 Selección aleatoria: Se ha elegido '{elegido}'")
                return elegido

            idx_elegido = int(seleccion)
            if 1 <= idx_elegido <= len(productos):
                elegido = productos[idx_elegido - 1]
                log(f"✓ Seleccionado: '{elegido}'")
                return elegido
            else:
                print(f"⚠ Número fuera de rango. Debe elegir entre 1 y {len(productos)}.")
        except ValueError:
            print("⚠ Entrada no válida. Ingrese un número o presione ENTER.")
        except (KeyboardInterrupt, EOFError):
            raise KeyboardInterrupt

def pedir_cantidad():
    """
    Pide al usuario la cantidad de unidades para el producto seleccionado (1 a 5).
    """
    while True:
        try:
            entrada = input("👉 Ingrese cantidad de unidades (1-5) [ENTER = aleatorio]: ").strip()
            if not entrada:
                cantidad = random.randint(1, 5)
                log(f"🎲 Cantidad aleatoria elegida: {cantidad} unidades")
                return cantidad
            
            cantidad = int(entrada)
            if 1 <= cantidad <= 5:
                return cantidad
            else:
                print("⚠ Cantidad fuera de rango. Debe ser de 1 a 5.")
        except ValueError:
            print("⚠ Entrada no válida. Ingrese un número o presione ENTER.")
        except (KeyboardInterrupt, EOFError):
            raise KeyboardInterrupt

def enviar_pedido(cliente_socket, producto, cantidad):
    """
    Envía el pedido en formato JSON sobre el socket TCP y espera la confirmación síncrona.
    """
    pedido = {
        "tipo": "pedido",
        "producto": producto,
        "cantidad": cantidad
    }
    
    # Enviar al servidor
    cliente_socket.sendall(json.dumps(pedido).encode(ENCODING))
    
    # Recibir respuesta del servidor
    datos_respuesta = cliente_socket.recv(BUFFER_SIZE)
    if not datos_respuesta:
        log("⚠ El servidor cerró la conexión abruptamente al enviar el pedido.")
        return False

    respuesta = json.loads(datos_respuesta.decode(ENCODING))
    tipo = respuesta.get("tipo")
    
    if tipo == "confirmacion":
        log(f"Confirmación: {respuesta.get('mensaje')} [Estado: {respuesta.get('estado')}]")
        return True
    elif tipo == "error":
        log(f"✗ RECHAZADO: {respuesta.get('mensaje')} [Estado: {respuesta.get('estado')}]")
        return False
    else:
        log(f"Respuesta inesperada: {respuesta}")
        return False

def ejecutar_cliente():
    cliente_socket = None
    try:
        # --- PASO 1: Crear y conectar el socket TCP ---
        log(f"Estableciendo conexión con el servidor en {HOST}:{PORT}...")
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
        print("=" * 60)

        # --- PASO 3: [MOD8] Interactuar con el usuario para definir cantidad de pedidos ---
        num_pedidos = pedir_numero_pedidos()
        log(f"Comenzando el envío interactivo de {num_pedidos} pedidos...")

        # --- PASO 4: Bucle interactivo para cada pedido ---
        for i in range(1, num_pedidos + 1):
            print(f"\n--- CONFIGURACIÓN DEL PEDIDO {i} de {num_pedidos} ---")
            
            # Permitir al usuario elegir el producto
            producto = elegir_producto(productos)
            
            # Permitir elegir la cantidad
            cantidad = pedir_cantidad()
            
            # Enviar el pedido y mostrar confirmación
            exito = enviar_pedido(cliente_socket, producto, cantidad)
            
            # Pequeña pausa decorativa
            time.sleep(1.0)

        # --- PASO 5: Enviar señal de FIN al servidor ---
        print("\n" + "-" * 50)
        log("Todos los pedidos ingresados han sido enviados. Solicitando cierre de sesión...")
        
        mensaje_fin = {"tipo": "fin"}
        cliente_socket.sendall(json.dumps(mensaje_fin).encode(ENCODING))
        
        # Recibir confirmación de fin
        datos_fin = cliente_socket.recv(BUFFER_SIZE)
        if datos_fin:
            respuesta_fin = json.loads(datos_fin.decode(ENCODING))
            log(f"Servidor confirma salida: {respuesta_fin.get('mensaje')}")

    except ConnectionRefusedError:
        log("ERROR: No se pudo conectar al servidor. ¿Está el servidor corriendo?")
    except KeyboardInterrupt:
        # [MOD8] Captura impecable de Ctrl+C
        print("\n\n⚠ [Ctrl+C] Conexión interrumpida por el usuario.")
        if cliente_socket:
            try:
                # Intentamos notificar elegantemente al servidor antes de forzar el cierre
                log("Enviando señal de salida rápida al servidor...")
                mensaje_fin = {"tipo": "fin"}
                cliente_socket.sendall(json.dumps(mensaje_fin).encode(ENCODING))
            except Exception:
                pass
    except Exception as e:
        log(f"ERROR INESPERADO: {e}")
    finally:
        if cliente_socket:
            cliente_socket.close()
            log("Socket cerrado de forma limpia. ¡Hasta luego!")

if __name__ == "__main__":
    ejecutar_cliente()
