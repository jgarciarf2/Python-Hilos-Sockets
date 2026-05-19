import threading
import time

# Definir una variable de control para detener el hilo
detener_hilo = False

# Definir la función que ejecutará el hilo
def mi_funcion():
    while not detener_hilo:
        print("El hilo está en ejecución...")
        time.sleep(1)  # Simular trabajo con una pausa
    print("El hilo se ha detenido.")

# Crear el hilo
hilo = threading.Thread(target=mi_funcion)

# Iniciar el hilo
hilo.start()
print("El hilo ha comenzado.")


# Simular una operación en el hilo principal
time.sleep(5)  # Esperar 5 segundos antes de detener el hilo

# Detener el hilo estableciendo la variable de control a True
detener_hilo = True

# Aseguramos que el hilo principal espere hasta que mi_funcion termine antes de continuar.
print("El hilo principal ha terminado.")
