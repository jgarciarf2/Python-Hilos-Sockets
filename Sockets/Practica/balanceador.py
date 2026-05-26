"""
BALANCEADOR DE CARGA — Distribuye clientes entre Servidor A y B
================================================================
El balanceador es un servidor intermedio que:
  1. Recibe la conexión del cliente
  2. Consulta cuántos pedidos tienen en cola el servidor A y B
  3. Redirige al cliente al servidor con MENOS carga
  4. El cliente se reconecta directamente al servidor elegido

ARQUITECTURA:
                          ┌─────────────────┐
                          │  Servidor A     │
  Cliente ──→ Balanceador─┤  Puerto 12345   │
                          │  cola: N pedidos│
                          └─────────────────┘
                          ┌─────────────────┐
                          │  Servidor B     │
                        ──┤  Puerto 12346   │
                          │  cola: M pedidos│
                          └─────────────────┘

PROTOCOLO DE REDIRECCIÓN:
  Cliente conecta al balanceador (puerto 9000)
  Balanceador responde: "REDIRECT:12345" o "REDIRECT:12346"
  Cliente abre nueva conexión al puerto indicado

DECISIONES CLAVE:
  - Lock protege la consulta de carga → decisión atómica
  - Cada servidor expone un puerto de ESTADO (A:12347, B:12348)
    el balanceador consulta ese puerto para saber la carga actual
  - Si un servidor está caído → el balanceador redirige al otro
  - stats{} del balanceador registra cuántos clientes envió a cada uno

CÓMO EJECUTAR:
  Terminal 1: python servidor_worker.py 12345 A
  Terminal 2: python servidor_worker.py 12346 B
  Terminal 3: python balanceador.py
  Terminal 4: python cliente_balanceado.py   (repite para más clientes)
"""

import socket
import threading
import time

# ── puertos ────────────────────────────────────
PUERTO_BALANCEADOR = 9000

SERVIDORES = [
    {"nombre": "ServidorA", "host": "localhost", "puerto": 12345, "puerto_estado": 12347},
    {"nombre": "ServidorB", "host": "localhost", "puerto": 12346, "puerto_estado": 12348},
]

# ── estadísticas del balanceador ──────────────
stats = {"ServidorA": 0, "ServidorB": 0, "fallidos": 0}
lock_stats = threading.Lock()

# Lock que protege la decisión de balanceo
# garantiza que dos clientes simultáneos no lean la misma carga
# y ambos sean enviados al mismo servidor
lock_decision = threading.Lock()


def consultar_carga(servidor):
    """
    Consulta cuántos pedidos tiene en cola un servidor worker.
    Se conecta al puerto de estado del servidor y lee el número.

    Retorna el número de pedidos en cola, o 999 si el servidor está caído.

    DECISIÓN: puerto de estado separado del puerto de pedidos
    → el balanceador no interfiere con clientes reales
    → si el servidor está ocupado procesando, igual responde al estado
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)                              # si no responde en 2s → caído
        s.connect((servidor["host"], servidor["puerto_estado"]))
        carga = int(s.recv(1024).decode().strip())   # servidor responde solo el número
        s.close()
        return carga
    except:
        return 999   # 999 → servidor caído o no disponible


def elegir_servidor():
    """
    Consulta la carga de ambos servidores y elige el menos cargado.
    La decisión se toma dentro del lock_decision → operación atómica.

    Retorna el diccionario del servidor elegido, o None si ambos están caídos.
    """
    with lock_decision:
        cargas = []
        for srv in SERVIDORES:
            carga = consultar_carga(srv)
            cargas.append((carga, srv))
            print(f"  [BALANCEADOR] {srv['nombre']} → carga: {carga} pedidos")

        # ordenar por carga → el primero es el menos cargado
        cargas.sort(key=lambda x: x[0])

        # si el menos cargado tiene 999 → ambos caídos
        if cargas[0][0] == 999:
            return None

        return cargas[0][1]   # retorna el servidor con menos carga


def manejar_cliente_balanceador(conn, addr):
    """
    Atiende a un cliente que llegó al balanceador.
    Le indica a qué servidor conectarse según la carga actual.
    """
    print(f"\n[BALANCEADOR] Cliente desde {addr} solicita conexión")

    try:
        servidor_elegido = elegir_servidor()

        if servidor_elegido is None:
            # ambos servidores caídos → rechazar cliente
            conn.sendall("ERROR: Todos los servidores están caídos.\n".encode())
            with lock_stats:
                stats["fallidos"] += 1
            return

        # informar al cliente a qué servidor conectarse
        # formato: "REDIRECT:puerto"  → cliente lo parsea y se reconecta
        mensaje = f"REDIRECT:{servidor_elegido['puerto']}\n"
        conn.sendall(mensaje.encode())

        nombre = servidor_elegido["nombre"]
        print(f"  [BALANCEADOR] → Redirigiendo a {nombre} (puerto {servidor_elegido['puerto']})")

        with lock_stats:
            stats[nombre] += 1

        # mostrar distribución actual
        print(f"  [BALANCEADOR] Distribución: {stats}")

    except Exception as e:
        print(f"[BALANCEADOR] Error: {e}")
    finally:
        conn.close()


def iniciar_balanceador():
    """
    Arranca el balanceador en el puerto 9000.
    Solo redirige clientes, no procesa pedidos.
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('localhost', PUERTO_BALANCEADOR))
    srv.listen(20)

    print(f"[BALANCEADOR] Activo en puerto {PUERTO_BALANCEADOR}")
    print(f"[BALANCEADOR] Servidores registrados:")
    for s in SERVIDORES:
        print(f"  {s['nombre']} → puerto {s['puerto']} (estado: {s['puerto_estado']})")
    print()

    try:
        while True:
            conn, addr = srv.accept()
            threading.Thread(
                target=manejar_cliente_balanceador,
                args=(conn, addr),
                daemon=True
            ).start()
    except KeyboardInterrupt:
        print(f"\n[BALANCEADOR] Cerrando...")
        print(f"[BALANCEADOR] Distribución final: {stats}")
        srv.close()


if __name__ == '__main__':
    iniciar_balanceador()
