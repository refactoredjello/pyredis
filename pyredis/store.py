import asyncio
import random
from asyncio import Future
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, Optional, Tuple

from pyredis.protocol import Integer, NullBulkString, PyRedisData, SimpleString


@dataclass
class Record:
    value: PyRedisData
    expiry: Optional[datetime]


class DataStoreCommands(Enum):
    SET = "SET"
    GET = "GET"
    DEL = "DEL"
    SIZE = "SIZE"


class KeyIndexStore:
    def __init__(self):
        self._keys = []
        self._indices = {}

    def append(self, key):
        self._keys.append(key)
        self._indices[key] = len(self._keys) - 1

        return self

    def delete(self, key):
        if not self._keys:
            return self

        idx = self._indices[key]
        del self._indices[key]
        tail = self._keys.pop()

        if tail == key:
            return self
        self._indices[tail] = idx
        self._keys[idx] = tail

        return self

    def get_random_key(self):
        if not self._keys:
            return None

        random_index = random.randint(0, len(self._keys) - 1)
        return self._keys[random_index]


class DataStore:
    def __init__(self):
        self._data: Dict[str, Record] = {}
        self.key_index: KeyIndexStore = KeyIndexStore()
        self._queue: asyncio.Queue[
            Tuple[
                DataStoreCommands,
                str | None,
                PyRedisData | None,
                datetime | None,
                Future,
            ]
        ] = asyncio.Queue()

    async def run_worker(self):
        print("Data Store: up")
        while True:
            command, key, value, expiry, future = await self._queue.get()
            current_time = datetime.now()
            try:
                match command:
                    case DataStoreCommands.SIZE:
                        future.set_result(len(self._data))
                    case DataStoreCommands.SET:
                        self._data[key] = Record(value, expiry)
                        self.key_index.append(key)
                        future.set_result(True)
                    case DataStoreCommands.DEL:
                        if key in self._data:
                            del self._data[key]
                            self.key_index.delete(key)
                            future.set_result(True)
                        else:
                            future.set_result(False)
                    case DataStoreCommands.GET:
                        result = self._data.get(key)
                        if result and result.expiry and result.expiry < current_time:
                            del self._data[key]
                            self.key_index.delete(key)
                            print(
                                f'Deleted key `{key}` after expiry {result.expiry.strftime("%Y-%m-%d %H:%M:%S")}'
                            )
                            future.set_result(None)
                        else:
                            future.set_result(result)
                    case _:
                        future.set_exception(ValueError(f"Unknown command: {command}"))
            except Exception as e:
                future.set_exception(e)

    async def size(self):
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        await self._queue.put((DataStoreCommands.SIZE, None, None, None, future))
        return await future

    async def delete(self, key) -> bool:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        await self._queue.put((DataStoreCommands.DEL, key, None, None, future))
        return await future

    async def set(self, key, value, expiry=None) -> SimpleString:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        await self._queue.put((DataStoreCommands.SET, key, value, expiry, future))
        return await future

    async def get(self, key) -> Record | None:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        await self._queue.put((DataStoreCommands.GET, key, None, None, future))
        return await future
