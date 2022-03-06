from multiprocessing import Process
from multiprocessing import BoundedSemaphore, Semaphore, Lock
from multiprocessing import current_process
from multiprocessing import Value, Array
from time import sleep
from random import random


N = 100
K = 10
NPROD = 3
NCONS = 3


def delay(factor = 3):
    sleep(random()/factor)


def add_data(storage, index, data, mutex):
    mutex.acquire()
    try:
        storage[index.value] = data
        delay()
        index.value = index.value + 1
    finally:
        mutex.release()


def get_data(storage, index, mutex):
    mutex.acquire()
    try:
        data = storage[0]
        index.value = index.value - 1
        delay()
        for i in range(index.value):
            storage[i] = storage[i + 1]
        storage[index.value] = -1

    finally:
        mutex.release()
    return data

def producer(storage, index, empty, non_empty, mutex):
    for v in range(N):
        delay()
        empty.acquire()
        add_data(storage, index, v, mutex)
        non_empty.release()
        print (f"producer {current_process().name} almacenado {v}")
    empty.acquire()
    add_data(storage, index, -1, mutex)
    non_empty.release()
    print (f"producer {current_process().name} terminado")


def consumer(storage, index, empty, non_empty, mutex, final):
    running = []
    running = [True for i in range(NPROD)]
    for k in range(NPROD*N):
        for idx in range(len(running)):
            if running[idx]:
                non_empty[idx].acquire()
                running[idx] = False
                
        valor = []
        for j in range(len(storage)):
            valor.append(storage[j][0])
        for j in range(NPROD):
            if valor[j] == -1:
                valor[j] = 1000
        minimo = min(valor)
        final[k] = minimo
        i = valor.index(minimo)
        dato = get_data(storage[i], index[i], mutex[i])
        empty[i].release()
        print (f"Desalmacenando del proceso {i} el valor {dato}")
        delay()

def main():
    storage = []
    index = []
    non_empty = [Semaphore(0) for i in range(NPROD)] 
    empty = [BoundedSemaphore(K) for i in range(NPROD)] 
    mutex = [Lock() for i in range(NPROD)] 
    prodList = []

    listafinal = Array('i', NPROD*N)
    for i in range(NPROD):
        aux_storage = Array('i', K)
        for i in range(K):
            aux_storage[i] = -1
        storage.append(aux_storage)
        
        index.append(Value('i', 0))
        
        
    for i in range(NPROD):
        prodList.append(Process(target=producer,
                        name=f'prod_{i}',
                        args=(storage[i], index[i], empty[i], non_empty[i], mutex[i])))

    merge = Process(target=consumer,
                      name=f"merge",
                      args=(storage, index, empty, non_empty, mutex, listafinal))

    for p in prodList:
        p.start()
    merge.start()

    for p in prodList:
        p.join()
    merge.join()
    
    print('ALMACEN FINAL', listafinal[:])


if __name__ == '__main__':
    main()