"""
Un Lock es un objeto que se utiliza para bloquear el acceso a 
una sección crítica del código, 
es decir, una sección que sólo puede ser ejecutada por un hilo a la vez.
"""

import threading
import random

# Vaciar el archivo resultados.txt
with open("resultados.txt", "w") as f:
    f.truncate(0) # Se elimina el contenido del archivo pasando su tamaño a 0

# creamos el Lock
lock_acceso_fichero = threading.Lock()

# Función donde se hace uso del .acquire() (BLOQUEA CODIGO) y del .release() (LIBERA CODIGO)

def escribir_valor(autor, valor):
    lock_acceso_fichero.acquire()                 # Solicitar acceso al bloque de codigo
    with open('resultados.txt', 'a') as fichero:  # Abrir archivo para añadir contenido al final
        fichero.write(f'{autor} - {valor}\n')     # Escribir
    lock_acceso_fichero.release()                 # Liberar el recurso al finalizar escritura

# Función llamada desde los hilos
def ejecutar():
    valor = round(100*random.random())
    escribir_valor(threading.current_thread().name, valor)

# creamos los hilos
hilo1 = threading.Thread(target=ejecutar, name='Hilo 1')
hilo2 = threading.Thread(target=ejecutar, name='Hilo 2')
hilo3 = threading.Thread(target=ejecutar, name='Hilo 3')



# ejecutamos los hilos
hilo1.start()
hilo2.start()
hilo3.start()

# esperar todos los hilos ejecutados
hilo1.join()
hilo2.join()
hilo3.join()

# mostrar archivo escrito
with open('resultados.txt', 'r') as archivo:
    contenido = archivo.read()
    print(contenido)