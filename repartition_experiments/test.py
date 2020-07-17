import numpy as np
import psutil, os
from memory_profiler import profile 
def print_mem_info():
    process = psutil.Process(os.getpid())
    print(process.memory_info().vms // 1024 // 1024)

def main():
    print("program starts")
    print_mem_info()

    print("creating samples...")
    a_list = list()
    for i in range(4):
        a_list.append(np.random.rand(100,100,100))
    print_mem_info()

    print("creating array...")
    arr = np.empty((400,100,100))
    print_mem_info()

    print("filling the array...")
    for i, a_tmp in enumerate(a_list):
        arr[i*100:(i+1)*100,:,:] = a_tmp
        del a_tmp
    print_mem_info()

    print("deleting the array...")
    del arr
    del a_list
    print_mem_info()

if __name__ == "__main__":
    main()