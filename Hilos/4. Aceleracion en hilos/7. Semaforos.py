"""
A diferencia del lock, se puede indicar cuantos hilos pueden acceder a 
un determinado recurso.

Los semáforos permiten regular el acceso a un recurso compartido 
por un número preestablecido de hilos.

Cada hilo que desee acceder al recurso debe llamar al método acquire y, 
cuando haya terminado, debe llamar al método release.

La clase Semaphore cumple con el protocolo de gestión de contexto, lo 
que significa que se puede utilizar con la construcción with para garantizar 
que el recurso compartido se libere correctamente, incluso en caso de errores o excepciones.
"""

import threading
import time

def imprimir_mensaje(num, semaforo):
    semaforo.acquire()
    print("Hola, soy el hilo", num)
    time.sleep(3)
    semaforo.release()

# Creamos un semáforo con capacidad para n hilos
n = 2
semaforo = threading.Semaphore(n)

# Creamos dos hilos que intentarán acceder al recurso compartido
hilo1 = threading.Thread(target=imprimir_mensaje, args=(1, semaforo))
hilo2 = threading.Thread(target=imprimir_mensaje, args=(2, semaforo))
hilo3 = threading.Thread(target=imprimir_mensaje, args=(3, semaforo))
hilo4 = threading.Thread(target=imprimir_mensaje, args=(4, semaforo))
hilo5 = threading.Thread(target=imprimir_mensaje, args=(5, semaforo))
hilo6 = threading.Thread(target=imprimir_mensaje, args=(6, semaforo))
hilo7 = threading.Thread(target=imprimir_mensaje, args=(7, semaforo))

# Iniciamos los hilos
hilo1.start()
hilo2.start()
hilo3.start()
hilo4.start()
hilo5.start()
hilo6.start()
hilo7.start()

# Esperamos a que terminen los hilos
hilo1.join()
hilo2.join()
hilo3.join()
hilo4.join()
hilo5.join()
hilo6.join()
hilo7.join()