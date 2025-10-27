from typing import Tuple, TypeAlias, Optional, Any, ClassVar
from dataclasses import dataclass, field, InitVar

CRLF = b'\r\n'


# TODO:
# add empty and nil bulk string
# b"$0\r\n\r\n" -> empty = serialized version is empty string
# b"$-1\r\n" -> nil = serialized version is 'nil'
# Add test cases for null bytes for array/bulk string and null data type
# handle incomplete frames
# Add command echo and ping

class PyRedisType:
    prefix: ClassVar[str]
    data: bytes

    def decode(self, encoding='utf-8'):
        return self.data.decode()

    def serialize(self):
        return self.prefix.encode() + self._serialize_data() + CRLF

    def _serialize_data(self):
        return self.data


# Error "-Error message\r\n”
@dataclass(frozen=True)
class Error(PyRedisType):
    prefix = '-'
    data: bytes

    def _serialize_data(self):
        return self.data


# b"+full\r\n"
@dataclass(frozen=True)
class SimpleString(PyRedisType):
    prefix = '+'
    data: bytes


# Integer ":100\r\n"
@dataclass(frozen=True)
class Integer(PyRedisType):
    prefix = ':'
    init_data: InitVar[bytes]
    data: int = field(init=False)

    def decode(self, encoding='utf-8'):
        return self.data

    def _serialize_data(self):
        return b'%(data)i' % {b'data': self.data}

    def __post_init__(self, init_data: bytes):
        try:
            object.__setattr__(self, 'data', int(init_data))
        except ValueError as e:
            raise ValueError(f'Failed to convert init data to an int:\n {e}')


# Bulk String "$11\r\nhello world\r\n"
@dataclass(frozen=True)
class BulkString(PyRedisType):
    prefix = '$'
    data: bytes

    def _serialize_data(self):
        return b'%(len)i%(CRLF)s%(data)s' % {b'len': len(self.data), b'CRLF': CRLF, b'data': self.data}


# Arrays "*2\r\n:1\r\n:2\r\n"
@dataclass(frozen=True)
class Array(PyRedisType):
    prefix = '*'
    data: list['Frame']

    def decode(self, encoding='utf-8'):
        return [val.decode(encoding) for val in self.data]

    def _serialize_data(self):
        res = b''
        for part in self.data:
            res += part.serialize()
        return b'%(len)i%(CRLF)s%(data)s' % {b'len': len(self.data), b'CRLF': CRLF, b'data': res}


Frame: TypeAlias = SimpleString | Error | Integer | BulkString | Array
ParseResult = Tuple[Optional[Frame], int]


def parse_bulk_string(buffer: bytes, offset: int) -> Tuple[BulkString | None, int]:
    if buffer.rfind(CRLF) <= offset:
        return None, 0

    length = int(buffer[0:offset])
    content = buffer[offset + 1:offset + length + 1]

    return BulkString(content), buffer.find(content) + length + 2


def parse_array(buffer: bytes, offset: int) -> Tuple[Array | None, int]:
    count = int(buffer[0:offset])

    if buffer[offset + 1:].count(CRLF) < count:
        return None, 0

    size = offset + 1
    res = []

    for _ in range(0, count):
        data, current_size = parse_frame(buffer[size:])
        res.append(data)
        size += current_size

    return Array(res), size + 1


def parse_frame(buffer: bytes) -> ParseResult:
    delim = buffer.find(CRLF)
    if delim == -1:
        return None, 0

    size = delim + 2
    match chr(buffer[0]):
        case SimpleString.prefix:
            return SimpleString(buffer[1:delim]), size
        case Error.prefix:
            return Error(buffer[1:delim]), size
        case Integer.prefix:
            return Integer(buffer[1:delim]), size
        case BulkString.prefix:
            return parse_bulk_string(buffer[1:], delim)
        case Array.prefix:
            return parse_array(buffer[1:], delim)
        case _:
            return None, 0


