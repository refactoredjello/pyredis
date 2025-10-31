import asyncio
from dataclasses import dataclass
from typing import Optional, Dict
from enum import Enum

from pyredis.protocol import PyRedisData, SimpleString
from datetime import datetime


@dataclass
class Record:
    value: PyRedisData
    expiry: Optional[datetime]


class DataStoreCommands(Enum):
    SET = "SET"
    GET = "GET"


class DataStore:
    def __init__(self):
        self._data: Dict[str, Record] = {}
        self._queue = asyncio.Queue()

    async def run_worker(self):
        print("Data store worker started.")
        while True:
            command, key, value, expiry, future = await self._queue.get()
            current_time = datetime.now()
            try:
                if command == DataStoreCommands.SET:
                    self._data[key] = Record(value, expiry)
                    future.set_result(True)
                elif command == DataStoreCommands.GET:
                    result = self._data.get(key)
                    if result and result.expiry and result.expiry < current_time:
                        future.set_result(None)
                    else:
                        future.set_result(result)
                else:
                    future.set_exception(ValueError(f"Unknown command: {command}"))
            except Exception as e:
                future.set_exception(e)

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
