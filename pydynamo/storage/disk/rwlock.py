import threading


class RWLock(object):
    def __init__(self):
        self.counter = 0
        self.write_number = 0
        self.lock = threading.Lock()
        self.read_ok = threading.Condition(self.lock)
        self.write_ok = threading.Condition(self.lock)

    def rlock_acquire(self):
        self.read_ok.acquire()
        while self.counter < 0:
            self.read_ok.wait()
        self.counter += 1
        self.read_ok.release()

    def wlock_acquire(self):
        self.write_ok.acquire()
        while self.counter != 0:
            self.write_number += 1
            self.write_ok.wait()
            self.write_number -= 1
        self.counter -= 1
        self.write_ok.release()

    def release(self):
        self.lock.acquire()
        if self.counter < 0:
            self.counter = 0
        else:
            self.counter -= 1
        self.lock.release()
        if self.write_number == 0:
            with self.read_ok:
                self.read_ok.notifyAll()
        elif self.counter == 0 and self.write_number > 0:
            with self.write_ok:
                self.write_ok.notify()
