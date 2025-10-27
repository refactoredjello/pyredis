import asyncio

class DataStore:
    def __init__(self):
        self._data = {}
        self._queue = asyncio.Queue()

    async def run_worker(self):
        print("Data store worker started.")
        while True:
            # Safely wait for the next command
            command, key, value, future = await self._queue.get()

            try:
                if command == 'SET':
                    self._data[key] = value
                    future.set_result('OK')
                elif command == 'GET':
                    result = self._data.get(key)
                    future.set_result(result)
                else:
                    future.set_exception(ValueError(f"Unknown command: {command}"))
            except Exception as e:
                future.set_exception(e)

    async def set(self, key, value):
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        await self._queue.put(('SET', key, value, future))
        return await future

    async def get(self, key):
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        await self._queue.put(('GET', key, None, future))
        return await future