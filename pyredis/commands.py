import inspect
from enum import Enum

from pyredis.protocol import Array, Error, SimpleString, NullBulkString, BulkString


class CommandParserException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class SetArgs(Enum):
    EX = "EX"
    PX = "PX"
    EXAT = "EXAT"
    PXAT = "PXAT"
    NX = "NX"
    XX = "XX"
    KEEPTTL = "KEEPTTL"
    GET = "GET"


class ParseSetArgs:
    def __init__(self, request: Array):
        self.args: list[BulkString] = request.decode()[3:]
        self.commands = []
        self.get_flag = False
        self.set_flag = False
        self.expiry_opt = set()
        self.keep_ttl_flag = False

    def parse_set_args(self):
        i = 0

        while i < len(self.args):
            try:
                arg = SetArgs(self.args[i])
                match arg:
                    case SetArgs.GET:
                        self.get_flag = True
                    case (
                        SetArgs.EX
                        | SetArgs.PX
                        | SetArgs.EXAT
                        | SetArgs.PXAT
                        | SetArgs.KEEPTTL
                    ):
                        if len(self.expiry_opt) or self.keep_ttl_flag:
                            raise CommandParserException(
                                "Cannot use more than one expiry arg."
                            )

                        if arg == SetArgs.KEEPTTL:
                            self.keep_ttl_flag = True
                            continue

                        i += 1
                        expiry_time = int(self.args[i].decode())
                        if expiry_time < 0:
                            raise CommandParserException(
                                f"{arg} must be greater than 0"
                            )
                        self.expiry_opt.add(arg)
                        self.expiry_opt.add(expiry_time)

            except ValueError:
                raise CommandParserException(
                    f"The arg `{self.args[i]}` is not valid for SET command. Must be one of {', '.join([v.value for v in SetArgs])}"
                )
            i += 1
        return self

    def opts_exist(self):
        return (
            self.get_flag or self.set_flag or len(self.expiry_opt) or self.keep_ttl_flag
        )


_cmd_registry = {}


def register_command(name):
    def decorator(func):
        _cmd_registry[name] = func
        return func

    return decorator


class Command:
    def __init__(self, request: Array, datastore):
        self.cmd = request.data[0].decode()
        self.request = request
        self.handler = _cmd_registry.get(self.cmd, self.not_found)
        self.datastore = datastore

    async def exec(self):
        if self.handler == self.not_found:
            return self.handler(self)

        if inspect.iscoroutinefunction(self.handler):
            return await self.handler(self)

        return self.handler(self)

    # ECHO  *2\r\n$4\r\nECHO\r\n$11\r\nhello world\r\n
    @register_command("ECHO")
    def echo(self):
        return self.request.data[1]

    # *1\r\n$4\r\nPING\r\n
    @register_command("PING")
    def ping(self):
        return SimpleString(b"PONG")

    @register_command("NOT_FOUND")
    def not_found(self):
        return Error(f"ERR command `{self.cmd}` not found".encode())

    # *3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n
    @register_command("SET")
    async def set_key(self):
        parser = None
        try:
            parser = ParseSetArgs(self.request).parse_set_args()
        except Exception as e:
            return Error(f"Failed to parse set args: {e}".encode())

        if parser.opts_exist():
            pass
        else:
            pass
        key = self.request.data[1].decode()
        value = self.request.data[2]

        await self.datastore.set(key, value)
        return SimpleString(b"OK")

    # *2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n
    @register_command("GET")
    async def get_key(self):
        if len(self.request.data) != 2:
            return Error(b"ERR GET does not require more than one argument")
        key = self.request.data[1].decode()
        value = await self.datastore.get(key)
        if value is None:
            return NullBulkString()
        return value
