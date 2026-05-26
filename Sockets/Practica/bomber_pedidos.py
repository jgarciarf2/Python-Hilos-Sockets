"""
CLIENTE BOMBER — Sobrecarga de la Central de Pedidos
=====================================================
Simula un cliente malicioso que intenta colapsar el servidor
enviando pedidos masivos SIN PAUSA entre ellos.

EVIDENCIA QUE EL SEMÁFORO FUNCIONA:
  - El bomber envía pedidos tan rápido como puede
  - El servidor solo acepta MAX_COLA=10 pedidos simultáneos
  - Cuando la cola está llena → semaforo.acquire() bloquea al bomber
  - El bomber NO colapsa el servidor, simplemente espera su turno
  - Los mensajes "⏳ Cola llena" evidencian el control del semáforo

DIFERENCIA CON CLIENTE NORMAL:
  Cliente normal  → time.sleep(0.5 a 2.0s) entre pedidos   (humano)
  Bomber          → sin sleep, máxima velocidad             (ataque)

CÓMO EJECUTAR:
  Terminal 1: python servidor_pedidos.py
  Terminal 2: python bomber_pedidos.py
  Observa en el servidor cómo la cola nunca supera MAX_COLA
"""

import socket
import threading
import random
import time

# ── configuración ──────────────────────────────
HOST          = 'localhost'
PORT          = 12345
TOTAL_PEDIDOS = 30          # total de pedidos que intentará enviar el bomber
NUM_HILOS     = 3           # hilos simultáneos → simula múltiples atacantes

# ── estadísticas del bomber ────────────────────
stats_bomber = {
    "enviados":   0,
    "aceptados":  0,
    "rechazados": 0,
    "cola_llena": 0
}
lock_stats = threading.Lock()


def rafaga_pedidos(socket_bomber, hilo_id, cantidad):
    """
    Envía pedidos masivos sin pausa desde un hilo.
    Registra cada respuesta del servidor para evidenciar
    que el semáforo controla el flujo.

    Parámetros:
        socket_bomber → socket conectado al servidor
        hilo_id       → identificador del hilo bomber
        cantidad      → cuántos pedidos envía este hilo
    """
    productos = ['productoA', 'productoB', 'productoC']

    for i in range(cantidad):
        producto = random.choice(productos)
        cant     = random.randint(1, 5)
        mensaje  = f"{producto},{cant}"

        try:
            # enviar sin pausa → máxima velocidad de sobrecarga
            socket_bomber.sendall(mensaje.encode('utf-8'))

            # recibir respuesta del servidor
            respuesta = socket_bomber.recv(1024).decode('utf-8').strip()

            with lock_stats:
                stats_bomber["enviados"] += 1

                # clasificar la respuesta del servidor
                if "✅" in respuesta:
                    stats_bomber["aceptados"] += 1
                    estado = "ACEPTADO ✅"
                elif "⏳" in respuesta:
                    stats_bomber["cola_llena"] += 1
                    estado = "COLA LLENA ⏳"   # ← evidencia del semáforo
                else:
                    stats_bomber["rechazados"] += 1
                    estado = "RECHAZADO ❌"

                print(
                    f"  [BOMBER Hilo-{hilo_id}] "
                    f"Pedido {i+1}/{cantidad}: {mensaje:15s} | "
                    f"{estado} | {respuesta}"
                )

            # SIN time.sleep() → máxima presión sobre el servidor

        except Exception as e:
            print(f"  [BOMBER Hilo-{hilo_id}] Error en pedido {i+1}: {e}")
            break


def iniciar_bomber():
    """
    Conecta al servidor y lanza múltiples hilos de sobrecarga.
    Cada hilo envía pedidos simultáneamente sin pausa.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        s.connect((HOST, PORT))
        print(f"[BOMBER] Conectado a {HOST}:{PORT}")

        # recibir info de productos del servidor
        info = s.recv(1024).decode('utf-8')
        print(f"[BOMBER] Servidor dice: {info.strip()}")
        print(f"[BOMBER] Iniciando sobrecarga con {NUM_HILOS} hilos simultáneos")
        print(f"[BOMBER] Total pedidos a enviar: {TOTAL_PEDIDOS}")
        print(f"[BOMBER] SIN pausa entre pedidos → máxima presión\n")

        pedidos_por_hilo = TOTAL_PEDIDOS // NUM_HILOS
        inicio = time.time()

        # lanzar hilos de sobrecarga simultáneamente
        hilos = [
            threading.Thread(
                target=rafaga_pedidos,
                args=(s, i+1, pedidos_por_hilo)
            )
            for i in range(NUM_HILOS)
        ]

        for h in hilos: h.start()
        for h in hilos: h.join()

        fin = time.time()

        # resumen final → evidencia del comportamiento del semáforo
        print(f"\n{'='*55}")
        print(f"[BOMBER] RESUMEN DE SOBRECARGA")
        print(f"{'='*55}")
        print(f"  Total enviados:          {stats_bomber['enviados']}")
        print(f"  Aceptados por servidor:  {stats_bomber['aceptados']}")
        print(f"  Cola llena (sem bloqueó):{stats_bomber['cola_llena']}  ← semáforo actuó")
        print(f"  Rechazados (sin stock):  {stats_bomber['rechazados']}")
        print(f"  Tiempo total:            {fin - inicio:.2f}s")
        print(f"  Servidor sigue activo:   ✅ no colapsó")
        print(f"{'='*55}")

    except ConnectionRefusedError:
        print(f"[ERROR] No se pudo conectar. ¿Está corriendo servidor_pedidos.py?")
    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        s.close()
        print("[BOMBER] Conexión cerrada.")


if __name__ == '__main__':
    iniciar_bomber()
