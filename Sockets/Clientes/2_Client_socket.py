import socket

"""
socket.AF_INET: Especifica el dominio de dirección del socket. 
    AF_INET indica que se utilizará el protocolo de direcciones IPv4. 
    Si quisieras utilizar el protocolo de direcciones IPv6, usarías AF_INET6.
socket.SOCK_STREAM: Especifica el tipo de socket. 
    SOCK_STREAM indica que se utilizará un socket orientado a la conexión, que en este caso es TCP. 
    Si quisieras crear un socket UDP, usarías SOCK_DGRAM.
"""
# creamos un socket TCP/IP
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# especificamos la direccion y el puerto del servidor
server_address = ('172.20.10.4', 8090) # ESTOS PARÁMETROS SE CONFIGURAN CON LOS DATOS
print('Conectándose a {} puerto {}'.format(*server_address))
client.connect(server_address)

try:
    # recibimos el mensaje de bienvenida del servidor
    data = client.recv(1024)
    mensaje_bienvenida = data.decode('utf-8')
    print(mensaje_bienvenida)

    while True:
        # pedimos al usuario que ingrese un mensaje
        mensaje = input('Ingrese un mensaje para enviar al servidor: ')

        # verificamos si el usuario quiere salir
        if mensaje.lower() == 'salir':
            break

        # enviamos el mensaje al servidor seguido de un carácter de nueva línea
        client.sendall((mensaje + '\n').encode('utf-8'))

        # esperamos la respuesta del servidor
        data = client.recv(1024)
        respuesta = data.decode('utf-8')
        print('Respuesta del servidor:', respuesta)

finally:
    # cerramos la conexión con el servidor
    print('Cerrando conexión')
    client.close()