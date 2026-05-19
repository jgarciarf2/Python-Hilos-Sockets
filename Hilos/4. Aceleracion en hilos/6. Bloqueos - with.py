import threading
import random


# Vaciar el archivo resultados.txt
with open("resultados.txt", "w") as f:
    f.truncate(0)

# creamos el Lock
lock_acceso_fichero = threading.Lock()

# Función donde se hace uso del .acquire() y del .release()

# Función donde se hace uso del WITH de Lock
# Cuando hacemos with con un lock no tenemos que invocar a acquire ni a release, pues ya se hace automáticamente.
def escribir_valor(autor, valor):
    with lock_acceso_fichero:
        with open('resultados.txt', 'a') as fichero:  # abrimos el fichero para añadir contenido al final
            fichero.write(f'{autor} - {valor}\n')           


# Función llamada desde los hilos
def ejecutar():
    #time.sleep(2*random.random())
    valor = round(100*random.random())
    escribir_valor(threading.current_thread().name, valor)

# creamos los hilos
hilo1 = threading.Thread(target=ejecutar, name='Hilo 1')
hilo2 = threading.Thread(target=ejecutar, name='Hilo 2')
hilo3 = threading.Thread(target=ejecutar, name='Hilo 3')



# ejecutamos los hilos
hilo1.start()
hilo2.start()
hilo3.start()

# esperar todos los hilos ejecutados
hilo1.join()
hilo2.join()
hilo3.join()

# mostrar archivo escrito
with open('resultados.txt', 'r') as archivo:
    contenido = archivo.read()
    print(contenido)