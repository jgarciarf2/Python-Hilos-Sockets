# Ejercicio de Cuenta Regresiva con Semáforo Binario (Turnos de Trabajo Alternados)
# Descripción: Crear un programa donde dos hilos representan dos trabajadores alternando turnos de cuenta regresiva
# (imprimiendo de 10 a 1). Los trabajadores deben alternar la impresión de los números (uno imprime 10, el otro imprime
# 9, y así sucesivamente).
# • Restricción: Usar un semáforo binario para alternar el control entre los hilos en cada paso de la cuenta regresiva

import threading
import time

semaforo = threading.Semaphore(1)  # Semáforo binario
contador = 10
turno = 1  # Variable para controlar el turno de forma estricta

def trabajador(id_trabajador):
    global contador, turno
    while True:
        semaforo.acquire()
        if contador <= 0:
            semaforo.release()
            break
        
        # Si es el turno de este trabajador, imprime y cede el turno
        if turno == id_trabajador:
            print(f"Trabajador {id_trabajador} imprime: {contador}")
            contador -= 1
            # Cambiar turno: si es 1 pasa a 2, si es 2 pasa a 1
            turno = 2 if id_trabajador == 1 else 1
        
        semaforo.release()
            
        time.sleep(1)
        
inicio = time.time()

hilo1 = threading.Thread(target=trabajador, args=(1,))
hilo2 = threading.Thread(target=trabajador, args=(2,))

hilo1.start()
hilo2.start()

hilo1.join()
hilo2.join()

fin = time.time()
tiempo_total = fin - inicio
print(tiempo_total,"segundos")