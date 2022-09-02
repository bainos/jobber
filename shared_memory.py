import threading
from multiprocessing import shared_memory
from jobber_slim import Jobber
import time
import signal


class JobberSHM:
    def __init__(self, size: int):
        self._size: int = size
        self._shm = shared_memory.SharedMemory(create=True, size=size)
        self._buf = self._shm.buf

    def put(self, word: str):
        assert(isinstance(word, str))
        assert(len(word) < self._size)
        bword = word.encode()
        self._buf[:len(bword)] = bword

    def get(self) -> str:
        word_arr = list()
        for b in self._buf:
            if b > 0:
                word_arr.append(chr(b))
        return ''.join(word_arr)

    def close(self):
        self._shm.close()
        self._shm.unlink()


class JSubscriber:
    def __init__(
        self,
        name: str,
        jshm: JobberSHM,
        lock: threading.Lock
    ):
        self._name = name
        self._shm = jshm
        self._lock = lock
        self._word: str = ''

    def listen(self):
        word = ''
        while True:
            if self._lock.locked():
                continue

            self._lock.acquire()
            word = self._shm.get()
            self._lock.release()

            if word != self._word:
                self._word = word
                print(f'{self._name}: {self._word}')

            if word == 'SHUTDOWN':
                break
        print(f'{self._name} exit')


class JPublisher:
    def __init__(
        self,
        jshm: JobberSHM,
        lock: threading.Lock
    ):
        self._shm = jshm
        self._lock = lock

    def publish(self, word: str):
        if not self._lock.locked():
            self._lock.acquire()
            self._shm.put(word)
            self._lock.release()
        else:
            self.publish(word)
            time.sleep(0.5)

    def shutdown(self):
        if not self._lock.locked():
            self._lock.acquire()
            self._shm.put('SHUTDOWN')
            self._lock.release()
        else:
            self.shutdown()
            time.sleep(0.5)

        print('pub exit')


# set concunrrency (best is number of cores)
# and initialize Jobber
concurrency = 4
jobber = Jobber(concurrency)

# get the decorator
jobberd = jobber.decorator

jshm = JobberSHM(32)
lock = threading.Lock()
pub = JPublisher(jshm, lock)
sub1 = JSubscriber('sub1', jshm, lock)
sub2 = JSubscriber('sub2', jshm, lock)


@jobberd()
def listener1():
    sub1.listen()


@jobberd()
def listener2():
    sub2.listen()


@jobberd()
def writer():
    time.sleep(3)
    for i in range(3):
        pub.publish(f'Hello {i}')
        time.sleep(3)

    pub.shutdown()
    print('writer exit')


def sigint_handler(sugnal, frame):
    print(signal, frame)
    jshm.close()
    raise SystemExit(1)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)

    try:
        jobber.work()
        print('end')
    except Exception as e:
        print(e)

    jshm.close()
