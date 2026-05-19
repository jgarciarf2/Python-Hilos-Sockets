import socket

def clientSocket():

    """
    socket.AF_INET: Especifica el dominio de dirección del socket. 
        AF_INET indica que se utilizará el protocolo de direcciones IPv4. 
        Si quisieras utilizar el protocolo de direcciones IPv6, usarías AF_INET6.
    socket.SOCK_STREAM: Especifica el tipo de socket. 
        SOCK_STREAM indica que se utilizará un socket orientado a la conexión, que en este caso es TCP. 
        Si quisieras crear un socket UDP, usarías SOCK_DGRAM.
    """
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    client_socket.connect(('localhost', 12345)) # Método para conectarse al socket en la IP:puerto especificado.

    #Se envia el mensaje al servidor
    client_socket.send("Hola, servidor!".encode('utf-8'))

    #Se espera la respuesta del servidor
    response = client_socket.recv(1024).decode('utf-8')

    print(f"Respuesta del servidor: {response}")

    client_socket.close()


clientSocket()
