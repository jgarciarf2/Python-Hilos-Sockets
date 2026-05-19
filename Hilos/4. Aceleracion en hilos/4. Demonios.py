""" DAEMONS O DEMONIOS

Un hilo daemon (o hilo demonio) es un hilo en segundo plano que se ejecuta de manera continua 
mientras el programa principal está en ejecución, pero que se detiene automáticamente cuando el 
programa principal finaliza. 

Los hilos daemon son útiles para tareas que no deben interrumpir la finalización del programa, 
como monitoreo en segundo plano, limpieza de datos o tareas periódicas.

DIFERENCIA ENTRE HILO NORMAL Y UN DEMONIO

Hilo normal: El programa principal esperará a que todos los hilos normales terminen su ejecución antes 
de finalizar. Esto significa que si hay hilos normales activos, el programa no se cerrará hasta que 
todos hayan terminado.

Hilo daemon: Se detendrá automáticamente cuando el programa principal termine, sin importar si el hilo 
daemon ha terminado su trabajo. Esto permite que el programa se cierre sin esperar a que estos hilos 
concluyan.

"""

# Definimos la ruta y el nombre del archivo que queremos ejecutar
archivo = './daemon.py'

"""
SI SE DEFINE EL DAEMON EN FALSE, ASI TERMINE EL PROGRAMA PRINCIPAL EL HILO SE SIGUE EJECUTANTO HASTA
QUE TERMINE. 

SI SE DEFINE EL DAEMON EL FALSE, TAN PRONTO TERMINE LA EJECUCION PRINCIPAL SE DETIENE EL HILO A MEDIO 
CAMINO
"""

# Escribir el código Python en un archivo llamado daemon.py
with open(archivo, 'w') as file:
    file.write("""

import threading
import time


def tarea():
    for i in range(7):
        print("Hilo ejecutandose")
        time.sleep(0.5)

hilo = threading.Thread(target=tarea, daemon=True) 
hilo.start()
time.sleep(2)
print("Final del programa")

""")
file.close()

"""

"""

import subprocess

"""
subprocess permite ejecutar y gestionar comandos del sistema operativo desde el propio código Python. 
Este módulo proporciona una interfaz para iniciar procesos, interactuar con ellos (enviar datos de 
entrada y capturar su salida) y manejar su finalización. 
"""


# Ejecutamos el archivo utilizando subprocess
resultado = subprocess.run(['python', archivo])

