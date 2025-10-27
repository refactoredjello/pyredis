import inspect

from pyredis.protocol import Array, Error, SimpleString, parse_frame, Null

_cmd_registry = {}

# TODO Add error handling for malformed get set commands
def register_command(name):
    def decorator(func):
        _cmd_registry[name] = func
        return func

    return decorator


class Commands:
    # ECHO  *2\r\n$4\r\nECHO\r\n$11\r\nhello world\r\n
    @staticmethod
    @register_command('ECHO')
    def echo(request):
        return request.data[1]

    # *1\r\n$4\r\nPING\r\n
    @staticmethod
    @register_command('PING')
    def ping(_):
        return SimpleString(b'PONG')

    @staticmethod
    @register_command('NOT_FOUND')
    def not_found(cmd):
        return Error(f"ERR command `{cmd}` not found".encode())

    @staticmethod
    @register_command("SET")
    async def set(request: Array, datastore):
        if len(request.data) != 3:
            return Error(b"ERR wrong number of arguments for 'set' command")
        key = request.data[1].decode()
        value = request.data[2]

        await datastore.set(key, value)
        return SimpleString(b"OK")

    @staticmethod
    @register_command("GET")
    async def get(request: Array, datastore):
        key = request.data[1].decode()
        value = await datastore.get(key)
        if value is None:
            return Null()
        return value


class Command:
    def __init__(self, request: Array, datastore):
        self.cmd = request.data[0].decode()
        self.request = request
        self.handler = _cmd_registry.get(self.cmd, Commands.not_found)
        self.datastore = datastore

    async def exec(self):
        if self.handler == Commands.not_found:
            return self.handler(self.cmd)

        if inspect.iscoroutinefunction(self.handler):
            return await self.handler(self.request, self.datastore)

        return self.handler(self.request)
