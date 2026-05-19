import threading        # Manejo de hilos
from time import sleep  # Pausar tiempos
import random           # Números aleatorios

def ejecutar():
    print(f'Comienza {threading.current_thread().name}')
    sleep(1.5*random.random()) #Tiempo de 0<t<1.5
    print(f'Termina {threading.current_thread().name}')

# Crear los hilos
hilo1 = threading.Thread(target=ejecutar, name='Hilo 1')
hilo2 = threading.Thread(target=ejecutar, name='Hilo 2')
hilo3 = threading.Thread(target=ejecutar, name='Hilo 3')

hilo4 = threading.Thread(target=ejecutar, name='Hilo 4')
hilo5 = threading.Thread(target=ejecutar, name='Hilo 5')
hilo6 = threading.Thread(target=ejecutar, name='Hilo 6')

# Ejecutar los hilos
hilo1.start()
hilo2.start()
hilo3.start()

#COMO INICIAMOS LOS HILOS Y LUEGOS LOS ENCOLAMOS NO HAY UN ORDEN CLARO DE EJECUCION, 
#AQUI LO UNICO QUE INDICAMOS ES QUE NO CONTINUARA LA EJECUCION HASTA QUE LOS 3 HILOS 
#ANTERIORES TERMINEN.

# Esperar a que terminen los hilos ejecutados
hilo1.join()
hilo2.join()
hilo3.join()



#A CONTINUACION SI SE EJECUTARAN SECUENCIALMENTE, YA QUE TAN PRONTO DEFINIMOS ENCOLAMOS.

# Ejecutar los hilos secuencialmente
hilo4.start()
hilo4.join()
hilo5.start()
hilo5.join()
hilo6.start()
hilo6.join()

print('El hilo principal sí espera por el resto de hilos.')