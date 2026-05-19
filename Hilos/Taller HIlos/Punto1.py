#  Ejercicio de Sincronización de Hilos con Barreras (Análisis de Datos en Partes)
# Descripción: Crear un programa donde tres hilos procesan diferentes columnas de un conjunto de datos (Genere un data set de 10 atributos numéricos y 100 registros). Cada hilo calculará la media de su columna. Una vez que todos los hilos terminan, los resultados deben sumarse.
# Restricción: Usar threading.Barrier para que cada hilo espere hasta que los otros hayan terminado de procesar sus respectivas columnas antes de combinar los resultados.

import threading
import numpy as np
import time

datos = np.round(5 * np.random.rand(100, 10)).astype(np.int8)
print("Datos:\n", datos)

barrier = threading.Barrier(3)
resultados = [0.0, 0.0, 0.0]

def calcular_media_columnas(id_hilo, columnas):
    medias = [np.mean(datos[:, columna]) for columna in columnas]
    resultados[id_hilo] = sum(medias)
    barrier.wait()

    if id_hilo == 0:
        print("La suma de las medias es:", sum(resultados))

columnas_por_hilo = [list(range(0, 3)), list(range(3, 6)), list(range(6, 10))]

inicio = time.time()

hilo1 = threading.Thread(target=calcular_media_columnas, args=(0, columnas_por_hilo[0]))
hilo2 = threading.Thread(target=calcular_media_columnas, args=(1, columnas_por_hilo[1]))
hilo3 = threading.Thread(target=calcular_media_columnas, args=(2, columnas_por_hilo[2]))

hilo1.start()
hilo2.start()
hilo3.start()  

hilo1.join()
hilo2.join()
hilo3.join()

fin = time.time()
tiempo_total = fin - inicio
print(tiempo_total,"segundos")