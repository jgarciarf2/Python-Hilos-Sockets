# Ejercicio de Productor-Consumidor con Semáforos (Producción y Consumo de Productos)
# Descripción: Crear un programa donde un grupo de hilos actúan como productores que generan productos y los colocan en una cola, mientras que otro grupo actúa como consumidores que retiran productos de la cola. Cada productor debe producir un producto cada 2 segundos, y cada consumidor debe consumir un producto cada segundo.
# Restricción: Usar threading.Semaphore para limitar el tamaño máximo de la cola y sincronizar la producción y el consumo.

import threading
import time

cola_productos = []
n = 5
# Semáforo para controlar los espacios disponibles (inicia en n)
semaforo_espacios = threading.Semaphore(n)
# Semáforo para controlar los productos disponibles (inicia en 0)
semaforo_items = threading.Semaphore(0)

def productor(id_productor):
    for i in range(5):  # Cada productor produce 5 productos
        semaforo_espacios.acquire()  # Esperar si la cola está llena
        producto = f"Producto_{id_productor}_{i}"
        cola_productos.append(producto)
        print(f"Productor {id_productor} produjo: {producto}")
        print(f"Cola actual: {cola_productos}")
        semaforo_items.release()  # Avisar que hay un nuevo item para consumir
        time.sleep(2)  # Simula el tiempo de producción
        
def consumidor(id_consumidor):
    for i in range(5):  # Cada consumidor consume 5 productos
        semaforo_items.acquire()  # Esperar si no hay productos (cola vacía)
        producto = cola_productos.pop(0)
        print(f"Consumidor {id_consumidor} consumió: {producto}")
        print("\n")
        print(f"Cola actual: {cola_productos}")
        semaforo_espacios.release()  # Avisar que hay un espacio libre
        time.sleep(1)  # Simula el tiempo de consumo

        
inicio = time.time()

hilo1 = threading.Thread(target=productor, args=(1,))
hilo2 = threading.Thread(target=productor, args=(2,))
hilo3 = threading.Thread(target=consumidor, args=(1,))
hilo4 = threading.Thread(target=consumidor, args=(2,))

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

print("Tiempo total:", tiempo_total,"segundos")
