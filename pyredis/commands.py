import inspect
from dataclasses import dataclass
from enum import Enum
from typing import Literal
from datetime import datetime, timedelta

from pyredis.protocol import Array, Error, SimpleString, NullBulkString, BulkString
from pyredis.store import DataStore


class CommandParserException(Exception):
    def __init__(self, message, request=None):
        self.message = message
        self.request = request
        super().__init__(self.message)


class SetArgs(Enum):
    EX = "EX"
    PX = "PX"
    EXAT = "EXAT"
    PXAT = "PXAT"
    KEEPTTL = "KEEPTTL"
    NX = "NX"
    XX = "XX"
    GET = "GET"


ExpiryType = Literal[SetArgs.EX, SetArgs.PX, SetArgs.EXAT, SetArgs.PXAT]


@dataclass
class ExpiryOptions:
    expiry_type: ExpiryType
    value: int


def get_expiry_time(opts: ExpiryOptions):
    try:
        match opts.expiry_type:
            case SetArgs.EX:
                return datetime.now() + timedelta(seconds=opts.value)
            case SetArgs.PX:
                return datetime.now() + timedelta(milliseconds=opts.value)
            case SetArgs.EXAT:
                return datetime.fromtimestamp(opts.value)
            case SetArgs.PXAT:
                return datetime.fromtimestamp(opts.value // 1000)
            case _:
                raise ValueError(
                    f"No valid expiry argument given `{opts.expiry_type}`"
                )
    except Exception as e:
        raise CommandParserException(
            f"Failed to set expiry from value {opts.value}: {e}"
        )


class ParseSetArgs:
    def __init__(self, request: Array):
        self.args: list = request.decode()[3:]
        self.commands = []
        self.get_flag = False
        self.set_flag = None
        self.expiry_opt: ExpiryOptions | None = None
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
                        if self.expiry_opt or self.keep_ttl_flag:
                            raise CommandParserException(
                                "Cannot use more than one expiry arg."
                            )

                        if arg == SetArgs.KEEPTTL:
                            self.keep_ttl_flag = True
                            continue

                        i += 1
                        expiry_offset = int(self.args[i])
                        if expiry_offset < 0:
                            raise CommandParserException(
                                f"{arg} must be greater than 0"
                            )
                        self.expiry_opt = ExpiryOptions(arg, expiry_offset)
                    case SetArgs.NX | SetArgs.XX:
                        if self.set_flag is not None:
                            raise CommandParserException(
                                "Can only set NX or XX, not both"
                            )
                        self.set_flag = arg
            except ValueError:
                raise CommandParserException(
                    f"The arg `{self.args[i]}` is not valid for SET command. Must be one of {', '.join([v.value for v in SetArgs])}"
                )
            i += 1
        return self

    def opts_exist(self):
        return (
            self.get_flag or self.set_flag or self.expiry_opt or self.keep_ttl_flag
        )


_cmd_registry = {}


def register_command(name):
    def decorator(func):
        _cmd_registry[name] = func
        return func

    return decorator


class Command:
    def __init__(self, request: Array, datastore: DataStore):
        self.cmd = request.data[0].decode().upper()
        self.request = request
        self.handler = _cmd_registry.get(self.cmd)
        self.datastore = datastore

    async def exec(self):
        if self.handler is None:
            return self.not_found()

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

    @register_command("INFO")
    def info(self):
        return SimpleString(B'Running')

    @register_command("COMMAND")
    def info(self):
        return SimpleString(B'Not Implemented')

    # *3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n
    @register_command("SET")
    async def set_key(self):
        if len(self.request.data) < 3:
            return Error(b"Wrong number of arguments for `set` command")

        expiry = None
        old_record = None
        key = self.request.data[1].decode()
        value = self.request.data[2]

        try:
            parser = ParseSetArgs(self.request).parse_set_args()
        except CommandParserException as e:
            return Error(f"Invalid SET arguments: {e}".encode())

        if parser.set_flag is not None or parser.get_flag is not None:
            old_record = await self.datastore.get(key)
            if parser.set_flag == SetArgs.NX and old_record is not None:
                return Error(f"Key {key} already exists and NX sent".encode())
            elif parser.set_flag == SetArgs.XX and old_record is None:
                return Error(f"Key {key} does not exist and XX sent".encode())

        if parser.expiry_opt:
            expiry = get_expiry_time(parser.expiry_opt)

        is_set = await self.datastore.set(key, value, expiry)

        if parser.get_flag:
            if old_record is None:
                return NullBulkString()
            if not isinstance(old_record.value, BulkString):
                return Error(
                    f"ERR old key {old_record.value.decode()} is not string".encode()
                )
            else:
                return old_record.value

        return SimpleString(b"OK") if is_set else Error(b"Failed to set key:value")

    # *2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n
    @register_command("GET")
    async def get_key(self):
        if len(self.request.data) != 2:
            return Error(b"ERR GET does not require more than one argument")
        key = self.request.data[1].decode()
        result = await self.datastore.get(key)
        if result is None:
            return NullBulkString()
        return result.value
