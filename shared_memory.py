from threading import Lock
from multiprocessing import shared_memory
# from jobber_slim import Jobber
import time
import signal
from enum import Enum, auto


class JShm:
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


class JPubSub(object):
    __instance = None

    def __init__(
        self,
        name: str,
        states: tuple,
        actions: tuple,
    ):
        self._name = name
        self._states = states
        self._actions = actions
        self.triggers: dict = dict()

        for action in self._actions:
            try:
                self.triggers[action]
            except KeyError:
                self.triggers[action] = dict()
            for state in self._states:
                self.triggers[action][state] = lambda *args: False

    def __new__(cls):
        if JPubSub.__instance is None:
            JPubSub.__instance = object.__new__(cls)

        JPubSub.__instance._lock = Lock()
        JPubSub.__instance._state = None
        JPubSub.__instance._action = None
        return JPubSub.__instance

    def listen(self):
        action = self._action
        while True:
            if self._lock.locked():
                continue

            if action != self._action:
                action = self._action
                self.triggers[action][self._state]()

            if self._action == 'SHUTDOWN':
                break

    def shout(self, action: str):
        if not self._lock.locked():
            self._lock.acquire()
            self._action = action
            self._lock.release()
        else:
            self.shout(action)
            time.sleep(0.1)

    def shutdown(self):
        self.shout('SHUTDOWN')


class States(Enum):
    WAIT = 0
    COMMAND = auto()
    INSERT = auto()
    VISUAL = auto()
    PLAYBACK = auto()


class Events(Enum):
    CURSOR_MOVE = 0
    K_LEFT = auto()
    K_UP = auto()
    K_RIGHT = auto()
    K_DOWN = auto()


class ComponentBase(JPubSub):
    def __init__(self, name: str):
        super().__init__(
            name = name,
            states = States(),
            actions = Events(),
        )

class Component1(ComponentBase):
    def __init__(self):
        super().__init__('Component1')
        


# # set concunrrency (best is number of cores)
# # and initialize Jobber
# concurrency = 4
# jobber = Jobber(concurrency)

# # get the decorator
# jobberd = jobber.decorator

def sigint_handler(sugnal, frame):
    print(signal, frame)
    raise SystemExit(1)


class Borg(object):
    __instance = None

    def __new__(cls, val):
        if Borg.__instance is None:
            Borg.__instance = object.__new__(cls)
        Borg.__instance.val = val
        return Borg.__instance

    def set_val(self, val):
        self.val = val


class Singleton(Borg):
    def __init__(self, arg):
        self.val = arg

    def __str__(self): return self.val


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)

    a = Singleton('A')
    b = Singleton('B')
    c = Singleton('C')

    print(a)
    print(b)
    print(c)
    a.set_val('UO')
    print(a)
    print(b)
    print(c)
    b.val = 'SORBOLE!'
    print(a)
    print(b)
    print(c)
