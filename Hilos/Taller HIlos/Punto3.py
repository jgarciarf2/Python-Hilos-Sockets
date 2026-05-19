# Ejercicio de Bloqueos en una Cuenta Bancaria (Gestión de Transacciones)
# Descripción: Implementar una simulación de una cuenta bancaria compartida. Dos hilos representarán transacciones de
# depósito y retiro. Los depósitos suman una cantidad aleatoria entre 10 y 100 unidades, y los retiros restan una cantidad
# similar. El Restricción es evitar inconsistencias en el balance debido a operaciones concurrentes.
# • Restricción: Aplicar threading.Lock para bloquear la cuenta mientras una transacción está en curso.

import threading
import time
import random

cuenta = [0]  # Balance inicial de la cuenta, se usa una lista para que sea mutable entre hilos

#Crear el lock
lock_acceso_fichero = threading.Lock()
def depositar(cuenta, cantidad):
    lock_acceso_fichero.acquire() # Solicitar acceso al bloque de codigo
    cuenta[0] += cantidad
    print(f"Depósito: {cantidad}, Balance: {cuenta[0]}")
    lock_acceso_fichero.release() # Liberar el recurso al finalizar escritura
    
def retirar(cuenta, cantidad):
    lock_acceso_fichero.acquire() # Solicitar acceso al bloque de codigo
    if cuenta[0] >= cantidad:
        cuenta[0] -= cantidad
        print(f"Retiro: {cantidad}, Balance: {cuenta[0]}")
    else:
        print(f"Retiro fallido: {cantidad}, Balance insuficiente: {cuenta[0]}")
    lock_acceso_fichero.release() # Liberar el recurso al finalizar escritura

inicio = time.time()

hilo1 = threading.Thread(target=depositar, args=(cuenta, random.randint(10, 100)))
hilo2 = threading.Thread(target=retirar, args=(cuenta, random.randint(10, 100)))

#Simular más transacciones
hilo3 = threading.Thread(target=depositar, args=(cuenta, random.randint(10, 100)))
hilo4 = threading.Thread(target=retirar, args=(cuenta, random.randint(10, 100)))

hilo1.start()
hilo2.start()
hilo3.start()
hilo4.start()

hilo1.join()
hilo2.join()  
hilo3.join()
hilo4.join()

fin = time.time()
tiempo_total = fin - inicio
print(tiempo_total,"segundos")

