#Import para la definicion de un objeto socket
import socket

def serverSocket():

    #Se requiere el dominio de la direccin y el tipo de socket
    """
    socket.AF_INET: Especifica el dominio de dirección del socket. 
        AF_INET indica que se utilizará el protocolo de direcciones IPv4. 
        Si se desea utilizar el protocolo de direcciones IPv6, se usa AF_INET6.
    socket.SOCK_STREAM: Especifica el tipo de socket. 
        SOCK_STREAM indica que se utilizará un socket orientado a la conexión, que en este caso es TCP. 
        Si se desea crear un socket UDP, se usa SOCK_DGRAM.
    """
    # AF__NET6 --> IPv6
    # SOCK_DGRAM --> UDP
    # SOCK_STREAM --> TCP
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 

    # El método bind() asocia la IP y el puerto al socket
    server_socket.bind(('localhost', 12345))

    
    # conexiones entrantes. Se indica por parametro el numero maximo de conexiones que 
    # encolara antes de empezar a rechazar mas conexiones adicionales simultaneas
    server_socket.listen(1)
                            
    print("Servidor escuchando en el puerto 12345...")

    # El método accept() bloquea la ejecución del programa hasta que
    # se establezca una conexión entrante, y luego devuelve un nuevo
    # objeto socket que representa la conexión con el cliente. Este método
    # aquí está retornando la dirección IP de la conexión y el socket cliente
    client_socket, client_address = server_socket.accept() 

    print(f"Conexión entrante desde {client_address}")

    # recibe hasta 1024 bytes de datos del socket, y luego decodifica estos 
    # datos como una cadena de caracteres utilizando la codificación 'utf-8'.
    message = client_socket.recv(1024).decode('utf-8') 

    print(f"Mensaje recibido del cliente: {message}")

    #Se le responde al cliente
    client_socket.send("Mensaje recibido por el servidor.".encode('utf-8')) 

    client_socket.close() # Se cierra el socket cliente
    server_socket.close() # Se cierra el socket servidor



serverSocket()