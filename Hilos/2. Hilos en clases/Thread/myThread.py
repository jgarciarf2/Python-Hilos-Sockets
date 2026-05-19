import threading
import time

#Definimos nuestra clase que hereda de threading.Thread
class myThread (threading.Thread):
    
    def __init__(self, threadID, name, counter):
        #Se llama el constructor de la clase padre
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter

    #Sobre escribimos la funcion run en la clase Thread
    def run(self):        
        print ("Iniciando " + self.name)        
        self.print_time(self.name, 5,self.counter)
        print ("Finalizando " + self.name)

    #Funcion que muestra la hora desde un determinado hilo, N veces segun el valor de counter.
    def print_time(self,threadName, delay, counter):
        while counter:
            time.sleep(delay)               
            print (f"{threadName} : {time.strftime('%H:%M:%S', time.localtime(time.time()))}") # Thread-#: HH:MM:SS
            counter -= 1