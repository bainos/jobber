from multiprocessing import shared_memory
from jobber import Jobber
import time

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
    def __init__(self, jshm: JobberSHM):
        self._shm = jshm
        self._word: str = ''

    def listen(self):
        word = ''
        while word != 'SHUTDOWN':
            word = jshm.get()
            if word != self._word:
                print(f'{word} != {self._word}')
                self._word = word
        jshm.close()

class JPublisher:
    def __init__(self, jshm: JobberSHM):
        self._shm = jshm

    def publish(self, word: str):
        self._shm.put(word)

    def shutdown(self):
        self._shm.put('SHUTDOWN')

# set concunrrency (best is number of cores)
# and initialize Jobber
concurrency = 2
jobber = Jobber(concurrency)

# get the decorator
jobberd = jobber.decorator

jshm = JobberSHM(32)
pub = JPublisher(jshm)
sub1 = JSubscriber(jshm)
sub2 = JSubscriber(jshm)

@jobberd
def listener1():
    sub1.listen()

@jobberd
def listener2():
    sub2.listen()


if __name__ == '__main__':
    jobber.work()

    time.sleep(3)
    for i in range(3):
        pub.publish(f'Hello {i}')
        time.sleep(3)

    pub.shutdown()