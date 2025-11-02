import asyncio
import os.path
import traceback

from pyredis.commands import Command
from pyredis.config import BUFFER_SIZE
from pyredis.protocol import Array, parse_frame
from pyredis.store import DataStoreWithLock


class AOF:
    def __init__(self, filename: str, datastore: DataStoreWithLock):
        self._queue: asyncio.Queue[Array] = asyncio.Queue()
        self.filename = filename
        self.datastore = datastore

    def _write_line(self, value: Array):
        with open(self.filename, "ab") as f:
            f.write(value.serialize())
            f.flush()

    async def run_worker(self):
        print("CMD Logger: up")
        while True:
            value = await self._queue.get()
            try:
                await asyncio.to_thread(self._write_line, value)

            except Exception:
                print(f"Task error: {value.decode() if value else ''}")
                traceback.print_exc()
            finally:
                self._queue.task_done()

    def log(self, value: Array):
        self._queue.put_nowait(value)

    async def replay(self):
        if os.path.exists(self.filename):
            with open(self.filename, "rb") as f:
                frame_buffer = bytearray()
                while True:
                    buffer = f.read(BUFFER_SIZE)
                    if not buffer:
                        return
                    frame_buffer.extend(buffer)
                    frame, size = parse_frame(frame_buffer)
                    if frame:
                        del frame_buffer[:size]
                        await Command(frame, self.datastore, None).exec()
