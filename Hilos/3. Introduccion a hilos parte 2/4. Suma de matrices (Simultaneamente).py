"""TODOS LOS HILOS EJECUTADOS SIMULTANEAMENTE"""

# Librerias que utilizaremos
import threading   # Manejo de hilos
import numpy as np # Manejo de matrices
import time        # Manejo de tiempos

# Definir las 3 matrices de 4x10, con números enteros entre 0 y 10
matrix1 = np.round(10*np.random.rand(4, 10)).astype(np.int8)
matrix2 = np.round(10*np.random.rand(4, 10)).astype(np.int8)
matrix3 = np.round(10*np.random.rand(4, 10)).astype(np.int8)

# Definir una matriz resultado de 4x10 con todos los valores en cero
result_matrix = np.zeros((4, 10)).astype(np.int8)

# Definir una función para sumar las filas de las matrices
def sum_rows(row_idx):
    global result_matrix
    """Al trabajar con Numpy, toma la fila completa como un vector, por eso al sumar la fila 
    completa suma independientemente cada elemento de la fila, y no la fila completa"""
    result_matrix[row_idx] = matrix1[row_idx] + matrix2[row_idx] + matrix3[row_idx]
    time.sleep(2) # Se demora 2 segundos sumando una fila

# Iniciar medición del tiempo
inicio = time.time()

# Crear 4 hilos, uno para cada fila
hilo1 = threading.Thread(target=sum_rows, args=(0,)) # Suma fila 1
hilo2 = threading.Thread(target=sum_rows, args=(1,)) # Suma fila 2
hilo3 = threading.Thread(target=sum_rows, args=(2,)) # Suma fila 3
hilo4 = threading.Thread(target=sum_rows, args=(3,)) # Suma fila 4

"""SE INICIAN TODOS LOS HILOS SIN ENCOLARLOS PARA QUE SE EJECUTEN SIMULATEAMENTE"""
# Poner a trabajar a los hilos
hilo1.start()
hilo2.start()
hilo3.start()
hilo4.start()

"""DESPUES DE EJECUTADOS SE ENCOLAN PARA QUE SE DEBA ESPERAR QUE TODOS TERMINEN PARA 
QUE CONTINUE LA EJECUCION"""

# Esperar a que todos los hilos terminen
hilo1.join()
hilo2.join()
hilo3.join()
hilo4.join()

"""TAN PRONTO TERMINAN LOS HILOS SE PROCEDE CON LA EJECUCION"""
# Imprimir las matrices y la matriz resultado
print("Matrix1:\n", matrix1)
print("Matrix2:\n", matrix2)
print("Matrix3:\n", matrix3)
print("Result Matrix:\n", result_matrix)

# Mostrar tiempo requerido
fin = time.time()
tiempo_total = fin - inicio

#SE EVIDENCIA UNA MEJORA EN EL TIEMPO DE EJECUCION
# al ejecutar los hilos simultaneamente
print(tiempo_total,"segundos")