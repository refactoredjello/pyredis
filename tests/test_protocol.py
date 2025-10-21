import pytest
from pyredis.protocol import parse_frame, SimpleString, Error, BulkString, Array, Integer

@pytest.mark.parametrize("buffer, expected", [
    (b"+part", (None, 0)),
    (b"+full\r\n", (SimpleString(b"full"), 7)),
    (b"+full\r\n+part", (SimpleString(b"full"), 7)),
    (b":100", (None, 0)),
    (b":100\r\n", (Integer(100), 6)),
    (b":100\r\n:200", (Integer(100), 6)),
    (b"-parterror", (None, 0)),
    (b"-Error\r\n", (Error(b"Error"), 8)),
    (b"-Error\r\n+part", (Error(b"Error"), 8)),
    (b"$5\r\nredis", (None, 0)),
    (b"$5\r\nredis\r\n", (BulkString(b"redis"), 10)),
    (b"$5\r\nredis\r\n$4\r\npart", (BulkString(b"redis"), 10)),
    (b"*2\r\n:1\r\n:2", (None, 0)),
    (b"*2\r\n:1\r\n:2\r\n", (Array([Integer(1), Integer(2)]), 12)),
    (b"*2\r\n:1\r\n:2\r\n*2\r\n:3", (Array([Integer(1), Integer(2)]), 12)),
    (b"*3\r\n:1\r\n:2\r\n*1\r\n+full\r\n", (Array([Integer(b"1"), Integer(b"2"), Array([SimpleString(b"full")])]), 23)),
])

def test_parse_frame(buffer, expected):
    got = parse_frame(buffer)
    assert got == expected


def test_simple_string_decode():
    s = SimpleString(b'OK')
    assert s.decode() == 'OK'

def test_error_decode():
    e = Error(b'Error message')
    assert e.decode('utf-8') == 'Error message'
    
    # Test with a different encoding
    e_jp = Error('エラー'.encode('shift-jis'))
    assert e_jp.decode('shift-jis') == 'エラー'

def test_integer_decode():
    # The decode method for Integer should just return the number itself.
    i = Integer(123)
    assert i.decode() == 123

def test_bulk_string_decode():
    b = BulkString(b'hello world')
    assert b.decode() == 'hello world'

def test_array_decode():
    arr = Array([
        BulkString(b'hello'),
        Integer(42),
        SimpleString(b'OK'),
        Array([Integer(1), Integer(2)])
    ])
    
    expected = ['hello', 42, 'OK', [1, 2]]
    assert arr.decode() == expected
