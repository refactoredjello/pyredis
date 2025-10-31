import asyncio
from asyncio import Future
from dataclasses import dataclass
from typing import Optional, Dict, Tuple
from enum import Enum

from pyredis.protocol import PyRedisData, SimpleString, Integer, NullBulkString
from datetime import datetime


@dataclass
class Record:
    value: PyRedisData
    expiry: Optional[datetime]


class DataStoreCommands(Enum):
    SET = "SET"
    GET = "GET"
    EXISTS = "EXISTS"
    DEL = 'DEL'
    INCR = 'INCR'
    DECR = 'DECR'


class DataStore:
    def __init__(self):
        self._data: Dict[str, Record] = {}
        self._queue: asyncio.Queue[
            Tuple[DataStoreCommands, str, PyRedisData | None, datetime | None, Future]
        ] = asyncio.Queue()

    async def run_worker(self):
        print("Data store worker started.")
        while True:
            command, key, value, expiry, future = await self._queue.get()
            current_time = datetime.now()
            try:
                match command:
                    case DataStoreCommands.DECR:
                        record = self._data.get(key)
                        if record and isinstance(record.value, Integer):
                            new_value = record.value.decode() - 1
                            new_record = Record(Integer(str(new_value).encode()), record.expiry)
                            self._data[key] = new_record
                            future.set_result(new_record.value)
                        else:
                            future.set_result(False)
                    case DataStoreCommands.INCR:
                        record = self._data.get(key)
                        if record and isinstance(record.value, Integer):
                            new_value = record.value.decode() + 1
                            new_record = Record(Integer(str(new_value).encode()), record.expiry)
                            self._data[key] = new_record
                            future.set_result(new_record.value)
                        else:
                            future.set_result(False)
                    case DataStoreCommands.SET:
                        self._data[key] = Record(value, expiry)
                        future.set_result(True)
                    case DataStoreCommands.EXISTS:
                        future.set_result(key in self._data)
                    case DataStoreCommands.DEL:
                        if key in self._data:
                            del self._data[key]
                            future.set_result(True)
                        else:
                            future.set_result(False)
                    case DataStoreCommands.GET:
                        result = self._data.get(key)
                        if result and result.expiry and result.expiry < current_time:
                            del self._data[key]
                            print(
                                f'Deleted Key `{key}` after expiry {result.expiry.strftime("%Y-%m-%d %H:%M:%S")}'
                            )
                            future.set_result(None)
                        else:
                            future.set_result(result)
                    case _:
                        future.set_exception(ValueError(f"Unknown command: {command}"))
            except Exception as e:
                future.set_exception(e)

    async def exists(self, key) -> bool:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        await self._queue.put((DataStoreCommands.EXISTS, key, None, None, future))
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

    async def get(self, key) -> Record:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        await self._queue.put((DataStoreCommands.GET, key, None, None, future))
        return await future

    async def incr(self, key) -> Integer | bool:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        await self._queue.put((DataStoreCommands.INCR, key, None, None, future))
        return await future

    async def decr(self, key) -> Integer | bool:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        await self._queue.put((DataStoreCommands.DECR, key, None, None, future))
        return await future


