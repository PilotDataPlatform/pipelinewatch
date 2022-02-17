from threading import Timer

def loop_start(func, second: int):
    #every second call func
    while True:
        timer = Timer(second, func)
        timer.start()
        timer.join()
 