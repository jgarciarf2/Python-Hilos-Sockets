import threading # Manejo de hilos
import time      # librería para tiempo

def first_function():
    print (threading.current_thread().name+str(' iniciando...\n'))
    time.sleep(1)
    print (threading.current_thread().name+str(' finalizó\n'))
    return

def second_function():
    print (threading.current_thread().name+str(' iniciando...\n'))
    time.sleep(5)
    print (threading.current_thread().name+str(' finalizó \n'))
    return


t1 = threading.Thread(name='Mi propio Thread', target=first_function) # Nombre de hilo asignado
t2 = threading.Thread(target=second_function) # Toma nombre de hilo por defecto automáticamente
t1.start()
t2.start()

    