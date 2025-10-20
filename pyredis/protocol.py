from typing import Tuple
from dataclasses import dataclass

@dataclass
class SimpleString:
    data: str

@dataclass
class Error:
    data: str

@dataclass
class Integer:
    data: int

    def decode(self):
        return int(self.data)

@dataclass
class BinaryString:
    data: str

@dataclass
class Array:
    data: list


# Requirements:
# =============
# We will need a module to extract messages from the stream.
#
# Each time we read from the stream we will get either:
# A partial message.
# A whole message.
# A whole message, followed by either 1 or 2.
#
# We will need to remove parsed bytes from the stream.

# Examples:
# =========
# Simple String "+OK\r\n"
# Error "-Error message\r\n”
# Integer ":100\r\n"
# Bulk String "$11\r\nhello world\r\n" # $5\r\nredis\r\n
# Arrays "*2\r\n:1\r\n:2\r\n"

CRLF = b'\r\n'

def parse_binary_string(buffer, length_delim) -> Tuple[BinaryString|None, int]:
    if buffer.rfind(CRLF) <= length_delim:
        return None, 0

    length = int(buffer[0:length_delim])
    content = buffer[length_delim+1:length_delim+length+1]

    return BinaryString(content.decode()), buffer.find(content)+length+2

def parse_arrays(buffer, length_delim):
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


def parse_frame(buffer):
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
            return Integer(int(buffer[1:delim].decode())), size
        case '$':
            return parse_binary_string(buffer[1:], delim)
        case '*':
            return parse_arrays(buffer[1:], delim)

    return None, 0