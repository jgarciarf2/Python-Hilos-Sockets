# Ejercicio de Control de Acceso con Semáforos (Simulación de Sala de Servidores) 
# Descripción: Simular una sala de servidores a la que solo pueden ingresar un máximo de tres técnicos al mismo tiempo.
# Cada técnico debe simular una reparación que dure entre 1 y 3 segundos. Al completar la reparación, debe liberar el acceso para permitir que otros entren.
# Restricción: Implementar threading.Semaphore para gestionar el acceso controlado a la sala.
import threading
import time
import numpy as np

#Crear un semáforo con capacidad para 3 técnicos
n = 3
semaforo = threading.Semaphore(n)

def tecnico(id_tecnico):
    print(f"Técnico {id_tecnico} intentando ingresar a la sala de servidores...")
    semaforo.acquire()
    print(f"Técnico {id_tecnico} ha ingresado a la sala de servidores.")
    tiempo_reparacion = np.random.randint(1, 4)  # Simula una reparación que dura entre 1 y 3 segundos
    time.sleep(tiempo_reparacion)
    print(f"Técnico {id_tecnico} ha completado la reparación y está saliendo de la sala.")
    semaforo.release()
    
inicio = time.time()
    
# Crear varios hilos para simular a los técnicos
hilo1 = threading.Thread(target=tecnico, args=(1,))
hilo2 = threading.Thread(target=tecnico, args=(2,))
hilo3 = threading.Thread(target=tecnico, args=(3,))
hilo4 = threading.Thread(target=tecnico, args=(4,))
hilo5 = threading.Thread(target=tecnico, args=(5,))
hilo6 = threading.Thread(target=tecnico, args=(6,))

# Iniciar los hilos
hilo1.start()
hilo2.start()
hilo3.start()
hilo4.start()
hilo5.start()
hilo6.start()

# Esperar a que terminen los hilos
hilo1.join()
hilo2.join()
hilo3.join()
hilo4.join()
hilo5.join()
hilo6.join()

fin = time.time()
tiempo_total = fin - inicio
print(tiempo_total,"segundos")