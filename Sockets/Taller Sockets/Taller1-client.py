"""Cliente del chat bidireccional con DOCUMENTACIÓN EXHAUSTIVA línea por línea.

PROPÓSITO GENERAL:
- Conecta al servidor en 127.0.0.1:50007.
- Solicita nombre (o usa "SPAMMER" si se activa ese modo).
- Recibe mensajes del servidor en un hilo aparte.
- Envía mensajes en otro hilo (interactivo o automático spammer).

MODOS DE OPERACIÓN:
1. MODO CHAT NORMAL: Lee de input() y envía al servidor interactivamente.
2. MODO SPAMMER: Envía 200 mensajes rápidamente para probar control de sobrecarga.

CONCEPTOS CLAVE:
1. SOCKET CLIENTE: Se conecta al servidor (diferente del socket servidor que ESPERA).
2. HILO RECEPTOR: Lee mensajes del servidor en paralelo (concurrencia).
3. SEMÁFORO DE IMPRESIÓN: Evita que dos hilos impriman simultáneamente (confusión de texto).
4. EVENT: Bandera booleana para señalar cuando parar los hilos.

EJECUCIÓN:
    # Modo chat normal:
    python Taller1-client.py
    
    # Modo spammer (envía 200 mensajes rápidamente):
    python Taller1-client.py --spammer
    
    # Con IP/puerto personalizados:
    python Taller1-client.py --host 192.168.1.100 --port 8080
"""

# from __future__ import annotations
# - Permite anotaciones de tipo pospuestas.
# - list[str] en lugar de typing.List[str].
# - Mejora legibilidad del código.
from __future__ import annotations

# import argparse
# - Módulo que parsea argumentos de línea de comandos.
# - Facilita crear CLIs con --flag arguments.
# - Ejemplo: --spammer, --host, --port.
import argparse

# import socket
# - Módulo socket para comunicación TCP/IP.
# - socket.socket() = crea un socket.
# - connect((host, port)) = conecta al servidor (CLIENTE).
# - send/recv = intercambia datos.
import socket

# import threading
# - Módulo threading para hilos y sincronización.
# - Thread() = crea hilo.
# - Semaphore() = contador protegido (evita impresiones entrelazadas).
# - Event() = bandera booleana para señales entre hilos.
import threading

# import time
# - Módulo de tiempo.
# - time.sleep(n) = pausa n segundos.
# - Usado en spammer para no saturar completamente.
import time


# ==============================================================================
# CONSTANTES GLOBALES
# ==============================================================================

# HOST = "127.0.0.1"
# - Dirección IP del servidor a conectarse.
# - "127.0.0.1" = localhost (máquina local).
# - Valor por defecto, se puede cambiar con --host en CLI.
HOST = "127.0.0.1"

# PORT = 50007
# - Puerto TCP del servidor.
# - Debe coincidir con el del servidor.
# - Valor por defecto, se puede cambiar con --port en CLI.
PORT = 50007

# ENCODING = "utf-8"
# - Codificación de caracteres para str <-> bytes.
# - UTF-8 = estándar universal (todos los idiomas).
ENCODING = "utf-8"

# BUFFER_SIZE = 2048
# - Máximo de bytes que recv() lee de una vez.
# - 2048 bytes = 2 KB (típico para chat).
BUFFER_SIZE = 2048


def run_client(host: str, port: int, spammer: bool = False) -> None:
    """Conecta al servidor y ejecuta cliente (normal o spammer).

    FLUJO:
    1. Solicita nombre (o usa "SPAMMER" si spammer=True).
    2. Crea socket y se conecta al servidor.
    3. Recibe respuesta "NOMBRE: " del servidor.
    4. Envía el nombre del cliente.
    5. Inicia hilo receptor (lee mensajes en paralelo).
    6. Ejecuta chat normal o modo spammer.
    7. Al terminar, señaliza parada y cierra socket.

    Args:
        host: IP o hostname del servidor (ej: "127.0.0.1").
        port: Puerto TCP del servidor (ej: 50007).
        spammer: Si es True, activa modo spammer (200 mensajes rápidos).

    Efectos:
        - Se conecta al servidor.
        - Abre socket cliente.
        - Inicia 2 hilos (receptor + chat/spammer).
        - Cierra socket al finalizar.
    """

    # name = input("Tu nombre: ").strip() if not spammer else "SPAMMER"
    # QUE HACE: Solicita nombre al usuario o usa "SPAMMER" si está en modo spammer.
    # - if not spammer = si NO está en modo spammer.
    # - input("Tu nombre: ") = pide nombre en consola.
    # - .strip() = elimina espacios al inicio/final.
    # - else "SPAMMER" = si está en modo spammer, usa ese nombre predeterminado.
    # - RESULTADO: name es un string (nombre del usuario o "SPAMMER").
    name = input("Tu nombre: ").strip() if not spammer else "SPAMMER"
    
    # print(f"Conectando como {name}...")
    # QUE HACE: Imprime log de que se está conectando.
    print(f"Conectando como {name}...")

    # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    # QUE HACE: Crea un socket TCP (lado cliente).
    # - socket.AF_INET = IPv4.
    # - socket.SOCK_STREAM = TCP (confiable).
    # - with = context manager: asegura que close() se ejecute al salir.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # sock.connect((host, port))
        # QUE HACE: Conecta el socket del cliente al servidor.
        # - ARGUMENTOS: (host, port) = ("127.0.0.1", 50007).
        # - BLOQUEANTE: espera hasta conectar o falla.
        # - EFECTO: a partir de aquí, sock está conectado al servidor.
        sock.connect((host, port))
        
        # prompt = sock.recv(BUFFER_SIZE).decode(ENCODING)
        # QUE HACE: Recibe el primer mensaje del servidor (solicitud de nombre).
        # - sock.recv(BUFFER_SIZE) = recibe hasta 2048 bytes.
        # - decode(ENCODING) = convierte bytes a str (b'NOMBRE: ' -> "NOMBRE: ").
        # - prompt = string del mensaje (ej: "NOMBRE: ").
        # - PROTOCOLO: servidor siempre envía "NOMBRE: " primero.
        prompt = sock.recv(BUFFER_SIZE).decode(ENCODING)
        
        # if prompt.startswith("NOMBRE"):
        # QUE HACE: Verifica que el mensaje sea una solicitud de nombre.
        # - prompt.startswith("NOMBRE") = ¿empieza con "NOMBRE"?.
        # - SEGURIDAD: confirma que el servidor está funcionando correctamente.
        if prompt.startswith("NOMBRE"):
            # sock.sendall(f"{name}\\n".encode(ENCODING))
            # QUE HACE: Envía el nombre del cliente al servidor.
            # - f"{name}\\n" = formatea nombre + salto de línea (ej: "Alice\\n").
            # - encode(ENCODING) = convierte str a bytes.
            # - sendall() = envía todos los bytes.
            # - EFECTO: servidor recibe el nombre y lo registra.
            sock.sendall(f"{name}\n".encode(ENCODING))

        # print_lock = threading.Semaphore(1)
        # QUE HACE: Crea un semáforo con 1 permiso para sincronizar impresiones.
        # - threading.Semaphore(1) = mutex (exclusión mutua).
        # - PROPÓSITO: evitar que dos hilos impriman simultáneamente.
        # - PROBLEMA EVITADO: texto entrelazado en consola.
        # - EJEMPLO SIN LOCK: hilo A imprime "Rec", hilo B interrumpe e imprime "Alice: ", resultado: "RecAlice: Alice: Hola"
        # - EJEMPLO CON LOCK: hilo A imprime "Recibido", luego hilo B imprime "Alice: Hola" (separado).
        print_lock = threading.Semaphore(1)
        
        # stop_event = threading.Event()
        # QUE HACE: Crea una bandera booleana para señalar cuando parar los hilos.
        # - threading.Event() = bandera compartida entre hilos.
        # - Inicialmente False (not set).
        # - .set() = pone en True.
        # - .is_set() = retorna True/False.
        # - PROPÓSITO: comunicar a hilos que deben parar.
        # - ESCENARIO: cuando usuario escribe "/exit", stop_event.set() dice a receptor que pare.
        stop_event = threading.Event()

        # recv_thread = threading.Thread(target=_client_receiver, args=(sock, print_lock, stop_event), daemon=True)
        # QUE HACE: Crea un hilo que recibe mensajes del servidor en paralelo.
        # - target = función a ejecutar: _client_receiver.
        # - args = argumentos: (sock, print_lock, stop_event).
        # - daemon = True: el hilo muere si el proceso principal muere.
        # - PROPÓSITO: receptor en paralelo, mientras usuario escribe en main loop.
        recv_thread = threading.Thread(
            target=_client_receiver,
            args=(sock, print_lock, stop_event),
            daemon=True,
        )
        
        # recv_thread.start()
        # QUE HACE: Inicia el hilo receptor (comienza a ejecutar _client_receiver).
        # - No es bloqueante, retorna inmediatamente.
        # - El hilo ejecuta _client_receiver() en paralelo.
        recv_thread.start()

        # if spammer:
        # QUE HACE: Verifica si está en modo spammer.
        if spammer:
            # _client_spam(sock, print_lock, stop_event)
            # QUE HACE: Ejecuta el cliente spammer (envía 200 mensajes rápidamente).
            # - ARGUMENTOS: sock (conectado), print_lock (sincroniza impresión), stop_event (parada).
            # - VER: función _client_spam() más abajo.
            _client_spam(sock, print_lock, stop_event)
        else:
            # _client_chat_loop(sock, stop_event)
            # QUE HACE: Ejecuta el chat interactivo normal.
            # - ARGUMENTOS: sock (conectado), stop_event (parada).
            # - VER: función _client_chat_loop() más abajo.
            # - BLOQUEANTE: espera a que usuario escriba en input().
            _client_chat_loop(sock, stop_event)


def _client_receiver(sock: socket.socket, print_lock: threading.Semaphore, stop_event: threading.Event) -> None:
    """Recibe mensajes del servidor de forma asincrónica en un hilo.

    PROPÓSITO:
    Ejecuta en paralelo con el loop de chat/spammer.
    Mientras el usuario escribe input(), este hilo recibe mensajes del servidor.
    Usa semáforo para sincronizar impresiones (evita texto entrelazado).
    Usa event para saber cuándo parar (cuando el usuario salga).

    Args:
        sock: Socket ya conectado al servidor.
        print_lock: Semáforo(1) para evitar mezclas de impresiones.
        stop_event: Evento que indica cuándo parar el hilo.
    """

    # while not stop_event.is_set():
    # QUE HACE: Loop que continúa mientras stop_event NO esté activado.
    # - stop_event.is_set() = retorna True si alguien llamó a set(), False si no.
    # - not False = True (continúa el loop).
    # - not True = False (sale del loop).
    # - PROPÓSITO: mantener recibiendo mensajes hasta que el usuario salga.
    while not stop_event.is_set():
        # try:
        # QUE HACE: Inicia bloque para manejar errores de socket.
        try:
            # data = sock.recv(BUFFER_SIZE)
            # QUE HACE: Recibe datos del servidor (BLOQUEA hasta recibir).
            # - sock = socket conectado al servidor.
            # - recv(BUFFER_SIZE) = recibe hasta 2048 bytes.
            # - data = bytes recibidos (ej: b'Alice: Hola').
            # - Si recv() retorna b'', el servidor cerró la conexión.
            data = sock.recv(BUFFER_SIZE)
            
            # if not data:
            # QUE HACE: Verifica si recv() retornó 0 bytes (servidor cerró).
            if not data:
                # break
                # QUE HACE: Sale del loop (servidor se desconectó).
                break
            
            # with print_lock:
            # QUE HACE: Adquiere el semáforo antes de imprimir.
            # - print_lock = Semaphore(1) con 1 permiso.
            # - Solo 1 hilo imprime a la vez (exclusión mutua).
            # - PROBLEMA EVITADO: texto entrelazado si 2 hilos imprimen simultáneamente.
            with print_lock:
                # print(data.decode(ENCODING), end="")
                # QUE HACE: Decodifica y imprime el mensaje.
                # - data.decode(ENCODING) = convierte bytes a str (b'Alice: Hola' -> "Alice: Hola").
                # - end="" = no añade salto de línea (el mensaje ya viene con \n del servidor).
                # - EFECTO: mensaje aparece en consola del cliente.
                print(data.decode(ENCODING), end="")
        
        # except OSError:
        # QUE HACE: Captura errores de socket (conexión cerrada, timeout, etc).
        except OSError:
            # break
            # QUE HACE: Sale del loop si hay error.
            break
    
    # stop_event.set()
    # QUE HACE: Asegura que stop_event esté activado al salir.
    # - stop_event.set() = pone la bandera en True.
    # - EFECTO: si el receptor falla, señaliza al chat loop que se cierre.
    stop_event.set()


def _client_chat_loop(sock: socket.socket, stop_event: threading.Event) -> None:
    """Lee desde teclado y envía mensajes al servidor (chat normal).

    PROPÓSITO:
    Loop interactivo que espera a que el usuario escriba.
    Cada línea se envía al servidor.
    Si escribe "/exit", cierra la conexión.
    Respeta stop_event para salir si el receptor falla.

    Args:
        sock: Socket ya conectado al servidor.
        stop_event: Evento para salir del loop (activado por receptor o usuario).
    """

    # while not stop_event.is_set():
    # QUE HACE: Loop mientras no haya señal de parada.
    # - stop_event.is_set() = retorna True si el receptor cerró.
    # - not False = True (continúa el loop).
    # - PROPÓSITO: esperar a que el usuario escriba (interactivo).
    while not stop_event.is_set():
        try:
            # message = input()
            # QUE HACE: Lee una línea del teclado (BLOQUEA hasta que usuario presione Enter).
            # - input() sin argumento = sin prompt visible.
            # - RETORNA: string que escribió el usuario.
            # - NOTA: si el usuario envía, input() incluye automáticamente el salto de línea.
            message = input()
        
        # except EOFError:
        # QUE HACE: Captura si el input llega al final (Ctrl+D, EOF).
        except EOFError:
            # message = "/exit"
            # QUE HACE: Si EOF, trata como comando de salida.
            message = "/exit"
        
        # if not message:
        # QUE HACE: Verifica si el mensaje es vacío (solo presionó Enter).
        if not message:
            # continue
            # QUE HACE: Salta a la siguiente iteración sin enviar nada.
            continue
        
        try:
            # sock.sendall(message.encode(ENCODING))
            # QUE HACE: Envía el mensaje al servidor.
            # - message.encode(ENCODING) = convierte str a bytes (UTF-8).
            # - sendall() = envía todos los bytes (reintentos si es necesario).
            # - EFECTO: servidor recibe el mensaje en su recv().
            sock.sendall(message.encode(ENCODING))
        
        # except OSError:
        # QUE HACE: Captura si hay error al enviar (conexión cerrada, etc).
        except OSError:
            # break
            # QUE HACE: Sale del loop si no se puede enviar.
            break
        
        # if message.lower() == "/exit":
        # QUE HACE: Verifica si el usuario escribió el comando de salida.
        # - message.lower() = convierte a minúsculas ("/EXIT" -> "/exit").
        if message.lower() == "/exit":
            # break
            # QUE HACE: Sale del loop (cierra el chat).
            break
    
    # stop_event.set()
    # QUE HACE: Señaliza la parada (asegura que el receptor también cierre).
    # - stop_event.set() = pone la bandera en True.
    # - EFECTO: el hilo receptor verá que stop_event está activado y saldrá.
    stop_event.set()


def _client_spam(sock: socket.socket, print_lock: threading.Semaphore, stop_event: threading.Event) -> None:
    """Cliente especial que intenta sobrecargar el servidor con muchos mensajes rápidamente.

    PROPÓSITO:
    Envía 200 mensajes en corto tiempo para probar que el servidor controla la sobrecarga.
    Si el servidor tiene control de carga (semáforo in_flight_limit), descartará algunos.
    Este cliente demuestra que el servidor NO se cuelga ni se desborda con spam.

    ESTRATEGIA:
    - Envía 200 mensajes "SPAM 1", "SPAM 2", etc.
    - Pausa 0.01 seg entre mensajes (no completamente saturado).
    - Envía "/exit" al final.
    - Imprime que finalizó.

    Args:
        sock: Socket conectado al servidor.
        print_lock: Semáforo para proteger la salida en consola.
        stop_event: Evento para detener el spam (si el receptor cierra).
    """

    # total = 200
    # QUE HACE: Define la cantidad de mensajes spam a enviar.
    # - 200 mensajes = suficientes para probar control de carga.
    # - Valor elegido: equilibrio entre rapidez y no saturar completamente.
    total = 200
    
    # for i in range(1, total + 1):
    # QUE HACE: Loop que envía 200 mensajes (i va de 1 a 200).
    # - range(1, 201) = [1, 2, 3, ..., 200].
    # - total + 1 = 200 + 1 = 201 (range es exclusivo en el final).
    for i in range(1, total + 1):
        # if stop_event.is_set():
        # QUE HACE: Verifica si se debe parar (receptor cerró o fallo).
        if stop_event.is_set():
            # break
            # QUE HACE: Sale del loop si hay señal de parada.
            break
        
        # payload = f"SPAM {i}"
        # QUE HACE: Crea un mensaje con número de secuencia.
        # - f"SPAM {i}" = formatea string (ej: "SPAM 1", "SPAM 2", ..., "SPAM 200").
        # - payload = string listo para enviar.
        payload = f"SPAM {i}"
        
        try:
            # sock.sendall(payload.encode(ENCODING))
            # QUE HACE: Envía el mensaje spam al servidor.
            # - payload.encode(ENCODING) = convierte str a bytes (UTF-8).
            # - sendall() = envía todos los bytes.
            # - EFECTO: servidor recibe "SPAM 1", "SPAM 2", etc.
            sock.sendall(payload.encode(ENCODING))
        
        # except OSError:
        # QUE HACE: Si hay error al enviar (servidor desconectó), para.
        except OSError:
            # break
            # QUE HACE: Sale del loop (no hay más conexión).
            break
        
        # time.sleep(0.01)
        # QUE HACE: Pausa 0.01 segundos (10 milisegundos) entre mensajes.
        # - PROPÓSITO: dar al servidor tiempo de procesar (no saturación total).
        # - EFECTO: ritmo más realista de spam (no instantáneo).
        # - NOTA: 200 mensajes * 0.01 seg = 2 segundos aproximadamente.
        time.sleep(0.01)

    # with print_lock:
    # QUE HACE: Adquiere semáforo antes de imprimir (evita texto entrelazado).
    with print_lock:
        # print("Spammer finalizado. Enviando /exit...")
        # QUE HACE: Imprime que el spam terminó.
        # - Mensaje informativo para el usuario/debugging.
        print("Spammer finalizado. Enviando /exit...")
    
    try:
        # sock.sendall(b"/exit")
        # QUE HACE: Envía el comando de salida al servidor.
        # - b"/exit" = bytes (ya está codificado).
        # - EFECTO: cierra la conexión en el servidor.
        sock.sendall(b"/exit")
    
    # except OSError:
    # QUE HACE: Si hay error, ignora (ya se va a cerrar de todas formas).
    except OSError:
        # pass
        # QUE HACE: No hacer nada, simplemente ignorar el error.
        pass
    
    # stop_event.set()
    # QUE HACE: Señaliza que debe parar (acaba el spammer).
    # - stop_event.set() = pone la bandera en True.
    # - EFECTO: el hilo receptor verá que stop_event está activado y saldrá.
    stop_event.set()


def parse_args() -> argparse.Namespace:
    """Parsea argumentos de línea de comandos.

    PROPÓSITO:
    Permite personalizar el cliente desde la CLI sin modificar código.
    Ejemplos:
    - python Taller1-client.py                      # Chat normal
    - python Taller1-client.py --spammer             # Modo spammer
    - python Taller1-client.py --host 192.168.1.100  # IP personalizada
    - python Taller1-client.py --port 8080           # Puerto personalizado

    Returns:
        argparse.Namespace: Objeto con atributos host, port, spammer.
    """

    # parser = argparse.ArgumentParser(description="Cliente del chat bidireccional")
    # QUE HACE: Crea un parser de argumentos CLI.
    # - ArgumentParser() = objeto que parsea argumentos de línea de comandos.
    # - description = texto de ayuda que aparece en --help.
    # - RESULTADO: parser es un ArgumentParser configurado.
    parser = argparse.ArgumentParser(description="Cliente del chat bidireccional")
    
    # parser.add_argument("--host", default=HOST)
    # QUE HACE: Agrega argumento --host (opcional, default="127.0.0.1").
    # - "--host" = nombre del argumento (con --).
    # - default=HOST = si no se especifica, usa "127.0.0.1".
    # - type = str (por defecto, sin especificar type).
    # - USO: python cliente.py --host 192.168.1.100
    parser.add_argument("--host", default=HOST)
    
    # parser.add_argument("--port", type=int, default=PORT)
    # QUE HACE: Agrega argumento --port (opcional, type=int, default=50007).
    # - "--port" = nombre del argumento.
    # - type=int = convierte string a entero.
    # - default=PORT = si no se especifica, usa 50007.
    # - USO: python cliente.py --port 8080
    parser.add_argument("--port", type=int, default=PORT)
    
    # parser.add_argument("--spammer", action="store_true", help="Activa el cliente especial")
    # QUE HACE: Agrega argumento --spammer (bandera booleana).
    # - "--spammer" = nombre del argumento.
    # - action="store_true" = si --spammer está presente, spammer=True, si no spammer=False.
    # - help = texto que aparece en --help.
    # - TIPO: argumento booleano (bandera).
    # - USO: python cliente.py --spammer (activa spam), sin --spammer (chat normal).
    parser.add_argument("--spammer", action="store_true", help="Activa el cliente especial")
    
    # return parser.parse_args()
    # QUE HACE: Parsea los argumentos de sys.argv y retorna un Namespace.
    # - parse_args() = parsea los argumentos CLI.
    # - RETORNA: Namespace con atributos host, port, spammer.
    # - EJEMPLO: Namespace(host='192.168.1.100', port=8080, spammer=True).
    return parser.parse_args()


def main() -> None:
    """Punto de entrada principal del cliente.

    FLUJO:
    1. Parsea argumentos CLI (--host, --port, --spammer).
    2. Extrae los valores del Namespace.
    3. Llama a run_client() con los argumentos parseados.
    """

    # args = parse_args()
    # QUE HACE: Parsea los argumentos de línea de comandos.
    # - parse_args() = parsea sys.argv y retorna un Namespace.
    # - args = Namespace(host="...", port=..., spammer=True/False).
    args = parse_args()
    
    # run_client(args.host, args.port, spammer=args.spammer)
    # QUE HACE: Ejecuta el cliente con los argumentos parseados.
    # - args.host = valor de --host (ej: "192.168.1.100" o "127.0.0.1").
    # - args.port = valor de --port (ej: 8080 o 50007).
    # - spammer = valor de --spammer (True si presente, False si no).
    # - EFECTO: inicia el cliente.
    run_client(args.host, args.port, spammer=args.spammer)


# if __name__ == "__main__":
# QUE HACE: Verifica si el script se ejecuta directamente.
# - __name__ es una variable especial.
# - Si ejecutas: python Taller1-client.py -> __name__ = "__main__".
# - Si importas: from archivo import algo -> __name__ = "archivo".
# - BENEFICIO: permite usar el archivo como ejecutable O como librería.
if __name__ == "__main__":
    # main()
    # QUE HACE: Llama a la función main() que inicia el cliente.
    # - Solo se ejecuta si el script se ejecuta directamente.
    # - No se ejecuta si el script se importa.
    main()
