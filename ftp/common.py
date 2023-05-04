from locale import LC_ALL, setlocale as _setlocale
from threading import Lock
from contextlib import contextmanager
from asyncio import IncompleteReadError

__all__ = (
    "StreamIO",
    "wrap_with_container",
    "AbstractAsyncLister",
    "setlocale",
)

class AsyncStreamIterator:
    def __init__(self, read_coro):
        self.read_coro = read_coro

    def __aiter__(self):
        return self

    async def __anext__(self):
        data = await self.read_coro()
        if data:
            return data
        else:
            raise StopAsyncIteration

class AbstractAsyncLister:
    async def _to_list(self):
        items = []
        async for item in self:
            items.append(item)
        return items

    def __aiter__(self):
        return self

    def __await__(self):
        return self._to_list().__await__()

def wrap_with_container(o):
    if isinstance(o, str):
        o = (o,)
    return o

class StreamIO:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    async def readline(self):
        return await self.reader.readline()

    async def read(self, count=-1):
        return await self.reader.read(count)

    async def readexactly(self, count):
        try:
            return await self.reader.readexactly(count)
        except IncompleteReadError as e:
            return e.partial

    async def write(self, data):
        self.writer.write(data)
        await self.writer.drain()

    def close(self):
        self.writer.close()

    async def __aenter__(self):
        pass

    async def __aexit__(self, *args, **kwargs):
        self.close()

    def iter_by_block(self, count=8192):
        return AsyncStreamIterator(lambda: self.readexactly(count))

LOCALE_LOCK = Lock()

@contextmanager
def setlocale(name):
    with LOCALE_LOCK:
        old_locale = _setlocale(LC_ALL)
        try:
            yield _setlocale(LC_ALL, name)
        finally:
            _setlocale(LC_ALL, old_locale)
