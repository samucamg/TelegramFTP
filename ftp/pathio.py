from asyncio import CancelledError, get_event_loop, gather, sleep as asleep
from collections import namedtuple
from functools import wraps
from io import BytesIO
from os import environ
from pathlib import PurePosixPath
from sys import exc_info
from time import time
from uuid import uuid4

from .errors import PathIOError
from .tg import File

__all__ = (
    "AbstractPathIO",
    "PathIONursery",
    "MongoDBPathIO",
)

def universal_exception(coro):
    @wraps(coro)
    async def wrapper(*args, **kwargs):
        try:
            return await coro(*args, **kwargs)
        except (CancelledError, NotImplementedError, StopAsyncIteration):
            raise
        except Exception as exc:
            raise PathIOError(reason=exc_info()) from exc

    return wrapper

class PathIONursery:
    def __init__(self, factory):
        self.factory = factory
        self.state = None

    def __call__(self, *args, **kwargs):
        instance = self.factory(*args, state=self.state, **kwargs)
        if self.state is None:
            self.state = instance.state
        return instance

class AbstractPathIO:
    def __init__(self, connection=None):
        self.connection = connection

class Node:
    def __init__(self, type, name, ctime=None, mtime=None, size=0, parent="/", parts=None, **k):
        if parts is None:
            parts = []
        self.type = type
        self.name = name
        self.ctime = ctime or int(time())
        self.mtime = mtime or int(time())
        self.size = size
        self.parent = parent
        self.path = str(PurePosixPath(parent) / name)
        self.parts = parts

class MongoDBMemoryIO:
    def __init__(self, node, mode, tg, db):
        self._node = node
        self._mode = mode
        self._tg = tg
        self._db = db
        self.offset = 0
        self._uploadingParts = []

    async def __aenter__(self):
        pass

    async def __aexit__(self, *args, **kwargs):
        pass

    async def seek(self, offset=0):
        self.offset = offset

    async def _writePart(self, data, size, partId, uploadId):
        msg = await self._tg.send_document(int(environ.get("CHAT_ID")), data, file_name="file", force_document=True)
        await self._db.files.update_one({"uploadId": uploadId}, {"$inc": {"size": size}, "$push": {"parts": {"part_id": partId, "tg_file": msg.document.file_id, "tg_message": msg.id}}})
        self._uploadingParts.remove(partId)

    async def write_stream(self, stream):
        loop = get_event_loop()
        u = str(uuid4())
        await self._db.files.update_one({"name": self._node.name, "parent": str(self._node.parent)}, {"$set": {"uploadId": u}})
        partNumber = 0 if "a" not in self._mode else max([p["part_id"] for p in self._node.parts])+1
        async for data in stream.iter_by_block(MongoDBPathIO.chunk_size):
            size = len(data)
            data = BytesIO(data)
            self._uploadingParts.append(partNumber)
            loop.create_task(self._writePart(data, size, partNumber, u))
            partNumber += 1
        while len(self._uploadingParts) != 0:
            await asleep(0.1)
        await self._db.files.update_one({"uploadId": u}, {"$unset": {"uploadId": 1}})

    def _chunks(self, lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    async def iter_by_block(self, block_size):
        loop = get_event_loop()
        parts = self._node.parts
        parts.sort(key=lambda x: x["part_id"])
        for part in parts:
            file = File(part["tg_file"], self._tg)
            downloaded = False
            o = 0
            while not downloaded:
                downloading = [
                    loop.create_task(file.getChunkAt(_o*1024*1024))
                    for _o in range(o, o+MongoDBPathIO.download_workers)
                ]
                o += MongoDBPathIO.download_workers
                for d in downloading:
                    res = await gather(d)
                    chunk = res[0] if res else b''
                    if len(chunk) != 1024*1024:
                        downloaded = True
                    for ch in self._chunks(chunk, block_size):
                        yield ch

class MongoDBPathIO(AbstractPathIO):
    db = None
    tg = None
    chunk_size = 1024 * 1024 * 16
    download_workers = 2
    Stats = namedtuple(
        "Stats", (
            "st_size",
            "st_ctime",
            "st_mtime",
            "st_nlink",
            "st_mode",
        )
    )

    def __init__(self, *args, state=None, cwd=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.cwd = PurePosixPath("/")

    @property
    def state(self):
        return []

    def __repr__(self):
        return repr(self.fs)

    def _absolute(self, path):
        if not path.is_absolute():
            path = self.cwd / path
        return path

    async def get_node(self, path):
        if str(path) == "/":
            return Node("dir", "", 0, 0, size=0, parent="/")
        node = await self.db.files.find_one({"name": path.parts[-1], "parent": str(path.parents[0])})
        node = Node(**node) if node else node
        return node

    @universal_exception
    async def exists(self, path):
        path = self._absolute(path)
        node = await self.get_node(path)
        return node is not None

    @universal_exception
    async def is_dir(self, path):
        path = self._absolute(path)
        node = await self.get_node(path)
        return not (node is None or node.type != "dir")

    @universal_exception
    async def is_file(self, path):
        path = self._absolute(path)
        node = await self.get_node(path)
        return not (node is None or node.type != "file")

    @universal_exception
    async def mkdir(self, path, *, exist_ok=False):
        path = self._absolute(path)
        node = await self.get_node(path)
        if node:
            if node.type != "dir" or not exist_ok:
                raise FileExistsError
        else:
            parent = await self.get_node(path.parent)
            if parent is None:
                raise FileNotFoundError
            if parent.type != "dir":
                raise NotADirectoryError
            await self.db.files.insert_one({
                "type": "dir",
                "ctime": int(time()),
                "mtime": int(time()),
                "name": path.parts[-1],
                "parent": str(path.parents[0]),
                "size": 0
            })

    @universal_exception
    async def rmdir(self, path):
        path = self._absolute(path)
        node = await self.get_node(path)
        if node is None:
            raise FileNotFoundError
        if node.type != "dir":
            raise NotADirectoryError

        await self.db.files.delete_one({"name": path.parts[-1], "parent": str(path.parents[0])})
        await self.db.files.delete_many({"parent": str(path)})

    @universal_exception
    async def unlink(self, path):
        path = self._absolute(path)
        node = await self.get_node(path)
        if node is None:
            raise FileNotFoundError
        if node.type != "file":
            raise IsADirectoryError

        await self.db.files.delete_one({"name": path.parts[-1], "parent": str(path.parents[0])})

    def list(self, path):
        path = self._absolute(path)
        class Lister:
            iter = None

            def __aiter__(self):
                return self

            @universal_exception
            async def __anext__(cls):
                if cls.iter is None:
                    cls.iter = self.db.files.find({"parent": str(path), "uploadId": {"$exists": False}})
                try:
                    return path / (await cls.iter.__anext__())["name"]
                except StopAsyncIteration:
                    raise

        return Lister()

    @universal_exception
    async def stat(self, path):
        path = self._absolute(path)
        node = await self.get_node(path)
        if node is None:
            raise FileNotFoundError

        if node.type == "file":
            size = node.size
            mode = 0x8000 | 0o666
        else:
            size = 0
            mode = 0x4000 | 0o777
        return MongoDBPathIO.Stats(size, node.ctime, node.mtime, 1, mode)

    @universal_exception
    async def open(self, path, mode="rb", *args, **kwargs):
        path = self._absolute(path)
        if mode == "wb":
            await self.db.files.delete_one({"name": path.parts[-1], "parent": str(path.parents[0])})
            await self.db.files.insert_one({
                "type": "file",
                "name": path.parts[-1],
                "ctime": int(time()),
                "mtime": int(time()),
                "size": 0,
                "parent": str(path.parents[0]),
                "parts": []
            })
        node = await self.get_node(path)
        if not node and mode == "rb":
            raise FileNotFoundError
        if not node and mode == "ab":
            await self.db.files.insert_one({
                "type": "file",
                "name": path.parts[-1],
                "ctime": int(time()),
                "mtime": int(time()),
                "size": 0,
                "parent": str(path.parents[0]),
                "parts": []
            })
            node = await self.get_node(path)
        return MongoDBMemoryIO(node, mode, self.tg, self.db)

    @universal_exception
    async def rename(self, source, destination):
        source = self._absolute(source)
        destination = self._absolute(destination)
        if source != destination:
            source = await self.get_node(source)
            destination = Node(
                type=source.type,
                name=destination.parts[-1],
                parent=destination.parents[0]
            )
            await self.db.files.update_one(
                {"name": source.name, "parent": source.parent},
                {"$set": {"name": destination.name, "parent": str(destination.parent), "ctime": int(time())}}
            )
            if source.type == "dir":
                await self.db.files.update_many({"parent": source.path}, {"$set": {"parent": str(destination.path)}})