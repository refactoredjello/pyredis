from typing import Tuple, TypeAlias, Optional, ClassVar
from dataclasses import dataclass, field, InitVar

CRLF = b"\r\n"


class PyRedisType:
    prefix: ClassVar[str]
    data: bytes

    def decode(self, encoding="utf-8"):
        return self.data.decode()

    def serialize(self):
        return self.prefix.encode() + self._serialize_data() + CRLF

    def _serialize_data(self):
        return self.data


# Error "-Error message\r\n”
@dataclass(frozen=True)
class Error(PyRedisType):
    prefix = "-"
    data: bytes


# b"+full\r\n"
@dataclass(frozen=True)
class SimpleString(PyRedisType):
    prefix = "+"
    data: bytes


# Integer ":100\r\n"
@dataclass(frozen=True)
class Integer(PyRedisType):
    prefix = ":"
    init_data: InitVar[bytes]
    data: int = field(init=False)

    def decode(self, encoding="utf-8"):
        return self.data

    def _serialize_data(self):
        return b"%(data)i" % {b"data": self.data}

    def __post_init__(self, init_data: bytes):
        try:
            object.__setattr__(self, "data", int(init_data))
        except ValueError as e:
            raise ValueError(f"Failed to convert init data to an int:\n {e}")


# Bulk String "$11\r\nhello world\r\n"
@dataclass(frozen=True)
class BulkString(PyRedisType):
    prefix = "$"
    data: bytes

    def _serialize_data(self):
        return b"%(len)i%(CRLF)s%(data)s" % {
            b"len": len(self.data),
            b"CRLF": CRLF,
            b"data": self.data,
        }


# Null BulkString "$0\r\n\r\n"
@dataclass(frozen=True)
class NullBulkString(BulkString):
    data: bytes = field(init=False, default=b"")

    def _serialize_data(self):
        return b"-1"


class ArrayNode:
    def __init__(self, val: PyRedisType, next_node=None):
        self.val = val
        self.next = next_node

    def decode(self):
        return self.val.decode()

    def serialize(self):
        return self.val.serialize()


# Arrays "*2\r\n:1\r\n:2\r\n"
@dataclass(frozen=False)
class Array(PyRedisType):
    prefix = "*"
    data: ArrayNode
    tail: ArrayNode | None
    _len: int

    def __iter__(self):
        return self

    def __next__(self):
        if self.data.next:
            return self.data.next
        else:
            raise StopIteration

    def __len__(self):
        if self._len:
            return self._len

        count = 0
        for _ in self:
            count += 1
        return count

    def __getitem__(self, index):
        curr = self.data
        if isinstance(index, int):

            if index < 0:
                index = +len(self)
            if index > len(self) or index < 0:
                raise IndexError("Index out of range")

            for _ in range(index):
                curr = curr.next
            return curr

        elif isinstance(index, slice):
            new_array = None
            start, stop, step = index.indices(len(self))
            for i in range(stop):
                if i >= start - 1:
                    new_array = Array(curr, None, stop - start + 1)
                if curr.next:
                    curr = curr.next
                else:
                    new_array.tail = curr
                    break
            new_array.tail.next = None
            return new_array

        raise TypeError("Index must be an int or slice")

    def decode(self, encoding="utf-8"):
        return [node.val.decode(encoding) for node in self]

    def _serialize_data(self):
        res = b""
        for part in self.data:
            res += part.serialize()
        return b"%(len)i%(CRLF)s%(data)s" % {
            b"len": len(self),
            b"CRLF": CRLF,
            b"data": res,
        }


@dataclass(frozen=True)
class NullArray(PyRedisType):
    data: list = field(init=False, default_factory=lambda: [])

    def _serialize_data(self):
        return b"0"


# Null b'_\r\n'
@dataclass(frozen=True)
class Null(PyRedisType):
    prefix = "_"
    data = None

    def decode(self, encoding="utf-8"):
        return ""

    def _serialize_data(self):
        return bytes()


PyRedisData: TypeAlias = SimpleString | Error | Integer | BulkString | Array | Null
ParseResult = Tuple[Optional[PyRedisData], int]


def parse_bulk_string(
    buffer: bytes, offset: int
) -> Tuple[BulkString | NullBulkString | None, int]:
    length = int(buffer[0:offset])
    if length == -1:
        return NullBulkString(), 5

    if buffer.rfind(CRLF) <= offset:
        return None, 0

    content_start = offset + len(CRLF)
    content_end = content_start + length
    if length != 0:
        content = buffer[content_start:content_end]
        return BulkString(content), offset + len(CRLF) + length + len(CRLF) + 1

    return BulkString(b""), 6


def parse_array(buffer: bytes, offset: int) -> Tuple[Array | NullArray | None, int]:
    count = int(buffer[0:offset])

    if buffer[offset + 1 :].count(CRLF) < count:
        return None, 0

    size = offset + 1

    if count == 0:
        return NullArray(), size + 1

    sentinel = ArrayNode(Null())
    curr = sentinel

    for _ in range(0, count):
        data, current_size = parse_frame(buffer[size:])
        curr.next = ArrayNode(data)
        curr = curr.next
        size += current_size

    return Array(sentinel.next, curr, count), size + 1


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
            return parse_bulk_string(buffer[1:], delim - 1)
        case Array.prefix:
            return parse_array(buffer[1:], delim)
        case Null.prefix:
            return Null(), 3
        case _:
            return None, 0
