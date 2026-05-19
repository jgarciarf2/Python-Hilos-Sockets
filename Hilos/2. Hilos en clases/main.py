from Thread.myThread import myThread

# Crear hilos

# Objetos del tipo myThread 
# Hilo que muestra la hora cada 5 segundos 4 veces (ID, NOMBRE HILO, CONTADOR)
thread1 = myThread(1, "Thread-1", 4) 

#Objetos del tipo myThread # Hilo que muestra la hora cada 5 segundos 3 veces (ID, NOMBRE HILO, CONTADOR)
thread2 = myThread(2, "Thread-2", 3) 

# Iniciar los hilos
thread1.start() #la ejecución del hilo comienza cuando se llama al método start().
thread2.start() #la ejecución del hilo comienza cuando se llama al método start().