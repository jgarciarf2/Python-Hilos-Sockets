#  Ejercicio de Lectores-Escritores (Sistema de Consulta y Actualización de un Registro de Usuarios)
# Descripción: Simular un registro de usuarios con operaciones de lectura y escritura. Tres hilos deben consultar (leer) información de usuarios sin afectar a otros lectores. Otros dos hilos deben actualizar la información de algunos usuarios (escribir) de forma exclusiva. Si un escritor está actualizando, los lectores deben esperar.
# Restricción: Implementar threading.Lock para controlar el acceso concurrente al registro en escenarios de lectores y escritores.

import threading
import time

registro_usuarios = {"usuario1": "info1", "usuario2": "info2", "usuario3": "info3"}
lock = threading.Lock()

def lector(id_lector):
    print("\n")
    print(f"Lector {id_lector} intentando leer el registro de usuarios...")
    lock.acquire()  # Adquirir el lock para leer
    print(f"Lector {id_lector} ha leído el registro: {registro_usuarios}")
    lock.release()  # Liberar el lock después de leer

def escritor(id_escritor):
    print("\n")
    print(f"Escritor {id_escritor} intentando escribir en el registro de usuarios...")
    lock.acquire()  # Adquirir el lock para escribir
    usuario_a_actualizar = f"usuario{(id_escritor % 3) + 1}"
    nueva_info = f"nueva_info{(id_escritor % 3) + 1}"
    registro_usuarios[usuario_a_actualizar] = nueva_info
    print(f"Escritor {id_escritor} ha actualizado {usuario_a_actualizar} a {nueva_info}")
    lock.release()  # Liberar el lock después de escribir

inicio = time.time()

hilo1 = threading.Thread(target=lector, args=(1,))
hilo2 = threading.Thread(target=lector, args=(2,))
hilo3 = threading.Thread(target=lector, args=(3,))
hilo4 = threading.Thread(target=escritor, args=(1,))
hilo5 = threading.Thread(target=escritor, args=(2,))

hilo1.start()
hilo2.start()
hilo3.start()
hilo4.start()
hilo5.start()

hilo1.join()
hilo2.join()
hilo3.join()
hilo4.join()
hilo5.join()


fin = time.time()
tiempo_total = fin - inicio
print(tiempo_total,"segundos")
