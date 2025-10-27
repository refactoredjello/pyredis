from pyredis.protocol import Array, Error, SimpleString, parse_frame

_cmd_registry = {}


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


class Command:
    def __init__(self, request: Array):
        self.cmd = request.data[0].decode()
        self.request = request
        self.handler = _cmd_registry.get(self.cmd, Commands.not_found)

    def exec(self):
        if self.handler == Commands.not_found:
            return self.handler(self.cmd)
        return self.handler(self.request)
