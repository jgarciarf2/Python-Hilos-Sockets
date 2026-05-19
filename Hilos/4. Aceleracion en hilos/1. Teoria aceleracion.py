"""
                             ¿Qué hay de la aceleración?

Relacionemos los tiempos del algoritmo ejecutado con diferente cantidad de hilos.

    * Hilo por hilo le toma al algoritmo 8 segundos aprox.

    * Con 2 hilos le toma al algoritmo 4 segundos aprox.

    * Con 4 hilos le toma al algoritmo 2 segundos aprox.

¿cuánto es la aceleración del algoritmo si con 2 hilos la realiza en 4 segundos aprox.?
"""

t_1 = 8 # Tiempo con 1 hilo
t_2 = 4 # Tiempo con 2 hilos
t_3 = 2 # Tiempo con 4 hilos

Aceleracion = t_1/t_2
print(f" La aceleración es de {Aceleracion} veces")

Aceleracion2 = t_1/t_3
print(f" La aceleración es de {Aceleracion2} veces")