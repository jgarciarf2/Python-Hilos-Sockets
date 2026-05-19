import threading
import time

# Crear un evento para detener el hilo
#Objeto de tipo Event para detener el hilo
evento_detener = threading.Event()

# Definir la función que ejecutará el hilo
def mi_funcion():
    while not evento_detener.is_set():  # Verifica si el evento ha sido activado
        print("El hilo está en ejecución...")
        time.sleep(1)  # Simula trabajo con una pausa
    print("El hilo se ha detenido.")

# Crear el hilo
hilo = threading.Thread(target=mi_funcion)

# Iniciar el hilo
hilo.start()
print("El hilo ha comenzado.")

# Simular alguna operación en el hilo principal
time.sleep(5)  # Espera 5 segundos antes de detener el hilo

# Detener el hilo activando el evento
evento_detener.set()

# Esperar a que el hilo termine con join()
hilo.join()
print("El hilo principal ha terminado.")
