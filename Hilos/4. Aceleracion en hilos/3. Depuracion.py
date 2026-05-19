import threading
import logging
import time

# Configuramos el nivel de registro para DEBUG, INDICANDO EL FORMATO EN EL CUAL SERAN 
# MOSTRADOS LOS MENSAJES, EN ESTE CADO (levelname = TIPO DE MENSAJE, threanName = NOMBRE HILO, MENSAJE)
logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] - %(threadName)-10s : %(message)s')

def worker(indice):
    # Registramos un mensaje de depuración al inicio de la función
    logging.debug('Inicio depuración')

    # Hacemos una pausa de 2 segundos
    time.sleep(2)

    # Generamos un caso correcto y una excepción IndexError
    my_list = [1, 2, 4]
    try:
        print(my_list[indice])
        # Registramos un mensaje de información
        logging.info('Código funciona correctamente')
    except IndexError:
        # Si se produce una excepción IndexError, se registrará un mensaje de registro de nivel ERROR
        logging.error('Índice fuera de rango')#, exc_info=True) # True muestra detalles

    # Registramos un mensaje de depuración al final de la función
    logging.debug('Fin depuración')


# Creamos un hilo y lo iniciamos -> Funciona bien - Indice dentro del rango
w1 = threading.Thread(target=worker, name='Hilo 1', args=(2,))
w1.start()
w1.join()


# Creamos un hilo y lo iniciamos -> Error - Indice fuera del rango
w2 = threading.Thread(target=worker, name='Hilo 2', args=(3,))
w2.start()
w2.join()