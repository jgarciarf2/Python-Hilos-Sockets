# Ejercicio de Sincronización de Hilos para Cálculo Paralelo (Cálculo de la Suma de una Gran Matriz)
# Descripción: Dividir una matriz grande de números enteros en cuatro secciones (filas o columnas). Cada hilo debe sumar los elementos de una sección y almacenar el resultado parcial. Al finalizar, una barrera asegura que los cuatro resultados parciales se sumen para obtener el total.
# Restricción: Practicar la sincronización con threading.Barrier para reunir los resultados después de la suma paralela.

import threading
import numpy as np
import time

#Crear una barrera para sincronizar los hilos
barrier = threading.Barrier(4)

matriz = np.random.randint(1, 2, (100, 100))
fila = [list(range(0, 25)), list(range(25, 50)), list(range(50, 75)), list(range(75, 100))]

def sumar_matriz(id_hilo, fila):
    suma_parcial = sum(matriz[fila])
    print("\n")
    print(f"Suma parcial del hilo {id_hilo}: {suma_parcial}")   
    print("\n")
    barrier.wait()  # Esperar a que todos los hilos terminen
    
    if id_hilo == 0:  # Solo un hilo se encargará de sumar los resultados parciales
        suma_total = sum(suma_parcial for suma_parcial in [sum(matriz[i]) for i in range(100)])
        print(f"Suma total de la matriz: {suma_total}")
        
    

inicio = time.time()

hilo0 = threading.Thread(target=sumar_matriz, args=(0, fila[0]))
hilo1 = threading.Thread(target=sumar_matriz, args=(1, fila[1]))
hilo2 = threading.Thread(target=sumar_matriz, args=(2, fila[2]))
hilo3 = threading.Thread(target=sumar_matriz, args=(3, fila[3]))

hilo0.start()
hilo1.start()
hilo2.start()
hilo3.start()

hilo0.join()
hilo1.join()
hilo2.join()
hilo3.join()

fin = time.time()
tiempo_total = fin - inicio
print(tiempo_total,"segundos")