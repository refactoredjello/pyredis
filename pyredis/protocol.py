from typing import Tuple, TypeAlias, Optional, Any
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, InitVar

CRLF = b'\r\n'

class PyRedisType(ABC):
    data = Any

    @abstractmethod
    def decode(self, encoding='utf-8'):
        raise NotImplemented

# b"+full\r\n"
@dataclass(frozen=True)
class SimpleString(PyRedisType):
    data: bytes
    def decode(self, encoding='utf-8'):
        return self.data.decode(encoding)

# Error "-Error message\r\n”
@dataclass(frozen=True)
class Error(PyRedisType):
    data: bytes
    def decode(self, encoding='utf-8'):
        return self.data.decode(encoding)


# Integer ":100\r\n"
@dataclass(frozen=True)
class Integer(PyRedisType):
    init_data: InitVar[bytes]
    data: int = field(init=False)
    def decode(self, encoding='utf-8'):
        return self.data

    def __post_init__(self, init_data: bytes):
        try:
            object.__setattr__(self, 'data', int(init_data))
        except ValueError as e:
            raise ValueError(f'Failed to convert init data to an int:\n {e}')


# Bulk String "$11\r\nhello world\r\n"
@dataclass(frozen=True)
class BulkString(PyRedisType):
    data: bytes
    def decode(self, encoding='utf-8'):
        return self.data.decode(encoding)


# Arrays "*2\r\n:1\r\n:2\r\n"
@dataclass(frozen=True)
class Array(PyRedisType):
    data: list
    def decode(self, encoding='utf-8'):
        return [val.decode(encoding) for val in self.data]

Frame: TypeAlias = SimpleString | Error | Integer | BulkString | Array
ParseResult = Tuple[Optional[Frame], int]

def parse_bulk_string(buffer: bytes, offset: int) -> Tuple[BulkString | None, int]:
    if buffer.rfind(CRLF) <= offset:
        return None, 0

    length = int(buffer[0:offset])
    content = buffer[offset + 1:offset + length + 1]

    return BulkString(content), buffer.find(content) + length + 2


def parse_arrays(buffer: bytes, offset: int) -> Tuple[Array | None, int]:
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
        case '+':
            return SimpleString(buffer[1:delim]), size
        case '-':
            return Error(buffer[1:delim]), size
        case ':':
            return Integer(buffer[1:delim]), size
        case '$':
            return parse_bulk_string(buffer[1:], delim)
        case '*':
            return parse_arrays(buffer[1:], delim)

    return None, 0
