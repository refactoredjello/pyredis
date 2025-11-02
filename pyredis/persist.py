import asyncio
import traceback

from pyredis.protocol import Array


class AOF:
    def __init__(self, filename: str):
        self._queue: asyncio.Queue[Array] = asyncio.Queue()
        self.filename = filename

    def _write_line(self, value: Array):
        with open(self.filename, 'ab') as f:
            f.write(value.serialize())
            f.flush()

    async def run_worker(self):
        print('CMD Logger: up')
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
