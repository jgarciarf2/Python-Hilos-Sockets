""" SUMA DE LAS MATRICES HILO POR HILO 

Se realizará la suma de las filas de tres matrices, 
cada una con 4 filas y 10 columnas. Se tiene a disposición 
4 hilos de procesamiento y 
se utilizara un hilo para procesar cada conjunto de filas de las tres matrices.
El proceso de suma de filas tarda aproximadamente 2 segundos en ejecutarse."""

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
    """Python por defecto asume que las variables manejadas en la funcion son locales. 
    Sin el global, el crearia una nueva variable result_matrix, y por ende, no actualizaria 
    la variable global previamente creada"""
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

#Se encolan los hilos para procesar la matriz

hilo1.start() # Poner a trabajar hilo 1
hilo1.join()  # Esperar a que el hilo 1 termine

hilo2.start() # Poner a trabajar hilo 2
hilo2.join()  # Esperar a que el hilo 2 termine

hilo3.start() # Poner a trabajar hilo 3
hilo3.join()  # Esperar a que el hilo 3 termine

hilo4.start() # Poner a trabajar hilo 4
hilo4.join()  # Esperar a que el hilo 4 termine

# Imprimir las matrices y la matriz resultado
print("Matrix1:\n", matrix1)
print("Matrix2:\n", matrix2)
print("Matrix3:\n", matrix3)
print("Result Matrix:\n", result_matrix)

# Mostrar tiempo requerido
fin = time.time()
tiempo_total = fin - inicio
print(tiempo_total,"segundos")