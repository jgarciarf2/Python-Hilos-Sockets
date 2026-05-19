import socket
import threading
import time

# creamos un socket TCP/IP
serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# especificamos la direccion y el puerto del servidor
server_address = ('', 8090) 
# El * antes del server_adress es para desempaquetar la tupla server_address
print('Iniciando servidor en {} puerto {}'.format(*server_address))

# El método bind() asocia la IP y el puerto al socket
serverSocket.bind(server_address)

# definimos una función para el hilo de procesamiento de clientes
def procesar_cliente(conn, addr, semaforo, barrera):
    try:
        print('Conexión desde', addr)

        # enviamos un mensaje de bienvenida
        mensaje_bienvenida = 'Bienvenido al servidor! ¿Cómo puedo ayudarte?\n'
        #Al cliente que se conecta enviamos un mensaje de bienvenida
        conn.sendall(mensaje_bienvenida.encode('utf-8'))

        # esperamos a que el cliente envíe un mensaje
        while True:
            data = conn.recv(1024)
            if not data:
                break
            mensaje = data.decode('utf-8').strip()
            print('\a Mensaje recibido desde', addr, ':', mensaje)

            # enviamos una respuesta al cliente
            respuesta = 'Tu mensaje "{}" ha sido recibido. Gracias por contactarnos!\n'.format(mensaje)
            conn.sendall(respuesta.encode('utf-8'))

            # simulamos un procesamiento que toma tiempo
            time.sleep(1)

        # cerramos la conexión con el cliente
        conn.close()
        print('Conexión cerrada con', addr)

    finally:
        # liberamos el semáforo y esperamos a que todos los hilos terminen
        semaforo.release()
        barrera.wait()

"""
En este ejemplo, utilizaremos hilos, semaforos y barreras para controlar la 
ejecucion de los sockets
"""

# definimos el número máximo de clientes que podemos atender simultáneamente para el hilo
max_clientes = 3

# creamos un semáforo con el número máximo de hilos permitidos
semaforo = threading.Semaphore(max_clientes)

# creamos una barrera para esperar a que todos los hilos terminen
barrera = threading.Barrier(max_clientes)

# iniciamos el servidor con 1 cliente a la vez
serverSocket.listen(1)

while True:
    # esperamos a que llegue una conexión
    print('Esperando una conexión...')
    #Se acepta la peticion de un cliente
    conn, addr = serverSocket.accept()

    # Se adquiere el recurso a utilizar por el semaforo
    semaforo.acquire()

    # creamos un nuevo hilo para procesar al cliente
    cliente_thread = threading.Thread(target=procesar_cliente, args=(conn, addr, semaforo, barrera))
    cliente_thread.start()