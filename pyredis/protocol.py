from typing import Tuple
from dataclasses import dataclass, field, InitVar

CRLF = b'\r\n'


# Simple String "+OK\r\n"
@dataclass
class SimpleString:
    data: str


# Error "-Error message\r\n”
@dataclass
class Error:
    data: str


# Integer ":100\r\n"
@dataclass
class Integer:
    init_data: InitVar[str]
    data: int = field(init=False)

    def __post_init__(self, init_data: str):
        try:
            self.data = int(init_data)
        except ValueError as e:
            raise ValueError(f'Failed to convert init data to an int:\n {e}')


# Bulk String "$11\r\nhello world\r\n"
@dataclass
class BulkString:
    data: str


# Arrays "*2\r\n:1\r\n:2\r\n"
@dataclass
class Array:
    data: list


def parse_binary_string(buffer: bytes, length_delim: int) -> Tuple[BulkString | None, int]:
    if buffer.rfind(CRLF) <= length_delim:
        return None, 0

    length = int(buffer[0:length_delim])
    content = buffer[length_delim + 1:length_delim + length + 1]

    return BulkString(content.decode()), buffer.find(content) + length + 2


def parse_arrays(buffer: bytes, length_delim: int) -> Tuple[Array | None, int]:
    count = int(buffer[0:length_delim])
    if buffer[length_delim + 1:].count(CRLF) < count:
        return None, 0

    size = length_delim + 1
    res = []
    for _ in range(0, count):
        data, current_size = parse_frame(buffer[size:])
        res.append(data)
        size += current_size

    return Array(res), size + 1


def parse_frame(buffer: bytes) -> Tuple[SimpleString | Error | Integer | BulkString | Array | None, int]:
    delim = buffer.find(CRLF)
    if delim == -1:
        return None, 0

    size = delim + 2
    match chr(buffer[0]):
        case '+':
            return SimpleString(buffer[1:delim].decode()), size
        case '-':
            return Error(buffer[1:delim].decode()), size
        case ':':
            return Integer(buffer[1:delim].decode()), size
        case '$':
            return parse_binary_string(buffer[1:], delim)
        case '*':
            return parse_arrays(buffer[1:], delim)

    return None, 0
