import threading
from multiprocessing import shared_memory
from jobber_slim import Jobber
import time
import signal
from abc import ABC, abstractmethod


class JobberSHM:
    def __init__(self, size: int):
        self._size: int = size
        self._shm = shared_memory.SharedMemory(create=True, size=size)
        self._buf = self._shm.buf

    def put(self, action: str):
        assert(isinstance(action, str))
        assert(len(action) < self._size)
        baction = action.encode()
        self._buf[:len(baction)] = baction

    def get(self) -> str:
        action_arr = list()
        for b in self._buf:
            if b > 0:
                action_arr.append(chr(b))
        return ''.join(action_arr)

    def close(self):
        self._shm.close()
        self._shm.unlink()


class JSubscriber(ABC):
    def __init__(
        self,
        name: str,
        jshm: JobberSHM,
        lock: threading.Lock,
        states: tuple(str),
        actions: tuple(str),
    ):
        self._name = name
        self._shm = jshm
        self._lock = lock
        self._states = states
        self._actions = actions
        self._state: str = ''
        self._action: str = ''
        self.triggers: dict = dict()
        assert(isinstance(self._states, tuple))
        assert(isinstance(self._actions, tuple))
        assert(all(isinstance(s, str) for s in self._states))
        assert(all(isinstance(s, str) for s in self._actions))
        for action in self._actions:
            try:
                self.triggers[action]
            except KeyError:
                self.triggers[action] = dict()
            for state in self._states:
                self.triggers[action][state] = lambda *args: False

    @property
    @abstractmethod
    def states(self):
        return self._states

    @property
    @abstractmethod
    def actions(self):
        return self._actions

    @abstractmethod
    def handle_state(self):
        pass

    @abstractmethod
    def handle_action(self):
        pass

    def listen(self):
        # state = ''
        action = ''
        while True:
            if self._lock.locked():
                continue

            # self._lock.acquire()
            action = self._shm.get()
            # self._lock.release()

            if action != self._action:
                self._action = action
                print(f'{self._name}: {self._action}')

            if action == 'SHUTDOWN':
                break


class JPublisher:
    def __init__(
        self,
        jshm: JobberSHM,
        lock: threading.Lock
    ):
        self._shm = jshm
        self._lock = lock

    def publish(self, action: str):
        if not self._lock.locked():
            self._lock.acquire()
            self._shm.put(action)
            self._lock.release()
        else:
            self.publish(action)
            time.sleep(0.5)

    def shutdown(self):
        if not self._lock.locked():
            self._lock.acquire()
            self._shm.put('SHUTDOWN')
            self._lock.release()
        else:
            self.shutdown()
            time.sleep(0.1)


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
