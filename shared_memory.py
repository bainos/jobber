from multiprocessing import shared_memory


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

