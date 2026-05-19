# Crear y ejecutar hilos con temporizador
import threading

def ejecutar(tiempo_s):
    print(f'El hilo {threading.current_thread().name} te saluda luego de tu espera de {tiempo_s} segundos')

# creamos un temporizador
tiempo_s = 5
temporizador = threading.Timer(tiempo_s, function=ejecutar, args=(tiempo_s,))  # Crear el hilo con temporizador
temporizador.start()  # El hilo empezará cuando pasen segundos dados
print("No te vayas, espera...")
  