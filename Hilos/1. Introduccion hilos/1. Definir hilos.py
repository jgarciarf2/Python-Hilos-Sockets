import threading        # Manejo de hilos
from time import sleep  # Pausar tiempo
import random           # Números aleatorios

#Funcion que sera asignada posteriormente al hilo
#SI RECIBIERAMOS VARIOS ARGs, simplemente se definen los ARGs como paramentros, ejemplo: 
#def function(a, b, c):
def function(i):
    #Se fuerme aleatoriamente el hilo entre 0 y 1.5 segundos
    sleep(1.5*random.random()) #Tiempo de 0<t<1.5 
    return print (f"Función llamada por el hilo {i}") #Imprime el hilo que se esta ejecutando

#Se definen 5 hilos, mandando por paramentro su indice
for i in range(5):
    #Target (Funcion que ejecutara)  args (Parametros que se desean enviar a la funcion como tuplas).
    #Se recibe el parámetro args como una tupla para proporcionar una manera sencilla y flexible de 
    # pasar múltiples argumentos a la función que se va a ejecutar en el hilo.     
    t = threading.Thread(target=function, args=(i,)) # Instanciarlo con una función de destino
    t.start() # Comience a funcionar
    
    #Si establecemos el JOIN, el hilo se encola, y hasta que no termine el hilo anterior este no se ejecutara.
    #t.join()  # Esperar por otro hilo