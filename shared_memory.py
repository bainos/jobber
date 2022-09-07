from threading import Lock
from multiprocessing import shared_memory
from jobber_slim import Jobber
import time
import signal
from enum import Enum, auto
from abc import ABCMeta, abstractmethod


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


class JPubSub(metaclass=ABCMeta):

    def __init__(
        self,
        states: Enum,
        events: Enum,
    ):
        self._states = states
        self._events = events
        self.triggers: dict = dict()

        for event in self._events:
            try:
                self.triggers[event.value]
            except KeyError:
                self.triggers[event.value] = dict()
            for state in self._states:
                self.triggers[event.value][state.value] = lambda *args: False

    __instance = None

    @classmethod
    def singleton(cls, name):
        if cls.__instance is None:
            cls.__instance = cls.create_singleton(name)
            cls.__instance.lock = Lock()
            cls.__instance.state = None
            cls.__instance.event = None
        return cls.__instance

    @classmethod
    @abstractmethod
    def create_singleton(cls, name):
        ...

    def listen(self):
        event = self.event
        while True:
            if self.lock.locked():
                continue

            if event != self.event:
                event = self.event
                self.triggers[event.value][self.state.value]()

            if self.event == 'SHUTDOWN':
                break

    def shout(self, event):
        if not self.lock.locked():
            self.lock.acquire()
            self.event = event
            self.lock.release()
        else:
            self.shout(event)
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
    def __init__(self):
        super().__init__(
            states = States,
            events = Events,
        )

    @classmethod
    def create_singleton(cls, name='ComponentBase'):
        return ComponentBase()

class Component1(ComponentBase):
    def __init__(self, name: str):
        super().__init__()
        self.name = name
        self.triggers[self._states.WAIT.value][self._events.CURSOR_MOVE.value] = self.on_wait_cursor_move
        self.triggers[self._states.INSERT.value][self._events.K_UP.value] = self.on_insert_k_up

    @classmethod
    def create_singleton(cls, name):
        return Component1(name)

    def _helper(self):
        print(f"{self.name}: {self.event} | {self.state}")

    def on_wait_cursor_move(self):
        self._helper()

    def on_insert_k_up(self):
        self._helper()

class Component2(ComponentBase):
    def __init__(self, name: str):
        super().__init__()
        self.name = name
        self.triggers[self._states.WAIT.value][self._events.CURSOR_MOVE.value] = self.on_wait_cursor_move
        self.triggers[self._states.INSERT.value][self._events.K_DOWN.value] = self.on_insert_k_down

    @classmethod
    def create_singleton(cls, name):
        return Component2(name)

    def _helper(self):
        print(f"{self.name}: {self.event} | {self.state}")

    def on_wait_cursor_move(self):
        self._helper()

    def on_insert_k_down(self):
        self._helper()


# # set concunrrency (best is number of cores)
# # and initialize Jobber
concurrency = 4
jobber = Jobber(concurrency)

# get the decorator
jobberd = jobber.decorator

def sigint_handler(sugnal, frame):
    print(signal, frame)
    raise SystemExit(1)


# class Borg(object):
#     __instance = None

#     def __new__(cls, val):
#         if Borg.__instance is None:
#             Borg.__instance = object.__new__(cls)
#         Borg.__instance.val = val
#         return Borg.__instance

#     def set_val(self, val):
#         self.val = val


# class Singleton(Borg):
#     def __init__(self, arg):
#         self.val = arg

#     def __str__(self): return self.val


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint_handler)

    c2 = Component1.singleton('c1-pluto')
    c1 = Component1.singleton('c1-pippo')
    c3 = Component2.singleton('c2-paperino')
    c4 = Component2.singleton('c2-paperone')

    print(c1.name, c1.state)
    print(c2.name, c2.state)
    print(c3.name, c3.state)
    print(c4.name, c4.state)

    c1.state = States.WAIT

    print(c1.name, c1.state)
    print(c2.name, c2.state)
    print(c3.name, c3.state)
    print(c4.name, c4.state)
    # a = Singleton('A')
    # b = Singleton('B')
    # c = Singleton('C')

    # print(a)
    # print(b)
    # print(c)
    # a.set_val('UO')
    # print(a)
    # print(b)
    # print(c)
    # b.val = 'SORBOLE!'
    # print(a)
    # print(b)
    # print(c)
