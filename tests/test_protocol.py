import pytest
from pyredis.protocol import parse_frame, SimpleString, Error, BulkString, Array, Integer

@pytest.mark.parametrize("buffer, expected", [
    (b"+part", (None, 0)),
    (b"+full\r\n", (SimpleString("full"), 7)),
    (b"+full\r\n+part", (SimpleString("full"), 7)),
    (b":100", (None, 0)),
    (b":100\r\n", (Integer(100), 6)),
    (b":100\r\n:200", (Integer(100), 6)),
    (b"-parterror", (None, 0)),
    (b"-Error\r\n", (Error("Error"), 8)),
    (b"-Error\r\n+part", (Error("Error"), 8)),
    (b"$5\r\nredis", (None, 0)),
    (b"$5\r\nredis\r\n", (BulkString("redis"), 10)),
    (b"$5\r\nredis\r\n$4\r\npart", (BulkString("redis"), 10)),
    (b"*2\r\n:1\r\n:2", (None, 0)),
    (b"*2\r\n:1\r\n:2\r\n", (Array([Integer(1), Integer(2)]), 12)),
    (b"*2\r\n:1\r\n:2\r\n*2\r\n:3", (Array([Integer(1), Integer(2)]), 12)),
])

def test_parse_frame(buffer, expected):
    got = parse_frame(buffer)
    assert got == expected

