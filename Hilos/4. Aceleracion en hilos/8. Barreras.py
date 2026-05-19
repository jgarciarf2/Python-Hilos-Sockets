"""      Barreras - Barrier

Las barreras son útiles cuando un grupo de hilos necesita esperar mutuamente unos por otros.

Para utilizarlas, creamos una instancia de la clase Barrier con el número de hilos que deben 
quedar en espera en la barrera.

Cuando un hilo llega a la barrera, llama al método wait y espera a que los demás hilos también 
lo hagan. Cuando todos los hilos han llamado a wait, la barrera se abre y todos los hilos continúan 
su ejecución.

Las barreras son útiles cuando los hilos tienen funcionalidades diferentes y 
necesitan esperarse mutuamente hasta que estén preparados.
"""

import threading
import time
import random

"""
Vamos a establecer una barrera de forma que nadie pueda despedirse si no ha saludado todo el 
mundo (es decir, los hilos).
"""

def worker(num, barrier):
    time.sleep(random.random() * 2)
    print(f"worker {num} saluda...")
    barrier.wait()  # esperar a que todos los hilos lleguen a la barrera
    time.sleep(random.random() * 2)
    print(f"worker {num} se despide...")

# crear barrera para 3 hilos
barrier = threading.Barrier(3)

# crear e iniciar hilos
thread1 = threading.Thread(target=worker, args=(1, barrier))
thread2 = threading.Thread(target=worker, args=(2, barrier))
thread3 = threading.Thread(target=worker, args=(3, barrier))

thread1.start()
thread2.start()
thread3.start()