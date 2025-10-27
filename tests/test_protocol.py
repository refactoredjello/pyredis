import pytest
from pyredis.protocol import parse_frame, SimpleString, Error, BulkString, Array, Integer, CRLF, Null, NullBulkString, \
    NullArray


@pytest.mark.parametrize("buffer, expected", [
    (b'none\r\n', (None, 0)),
    (b"+part", (None, 0)),
    (b"+full\r\n", (SimpleString(b"full"), 7)),
    (b"+full\r\n+part", (SimpleString(b"full"), 7)),
    (b":100", (None, 0)),
    (b":100\r\n", (Integer(b'100'), 6)),
    (b":100\r\n:200", (Integer(b'100'), 6)),
    (b"-parterror", (None, 0)),
    (b"-Error\r\n", (Error(b"Error"), 8)),
    (b"-Error\r\n+part", (Error(b"Error"), 8)),
    (b"$5\r\nredis", (None, 0)),
    (b"$5\r\nredis\r\n", (BulkString(b"redis"), 11)),
    (b"$0\r\n\r\n", (BulkString(b""), 6)),
    (b"$5\r\nredis\r\n$4\r\npart", (BulkString(b"redis"), 11)),
    (b"$-1\r\n", (NullBulkString(), 5)),
    (b"*2\r\n:1\r\n:2", (None, 0)),
    (b"*2\r\n:1\r\n:2\r\n", (Array([Integer(b'1'), Integer(b'2')]), 12)),
    (b"*2\r\n:1\r\n:2\r\n*2\r\n:3", (Array([Integer(b'1'), Integer(b'2')]), 12)),
    (b"*3\r\n:1\r\n:2\r\n*1\r\n+full\r\n", (Array([Integer(b"1"), Integer(b"2"), Array([SimpleString(b"full")])]), 23)),
    (b"*1\r\n$4\r\nPING\r\n", (Array([BulkString(B'PING')]), 14)),
    (b'*0\r\n', (NullArray(), 4)),
    (b'_\r\n', (Null(), 3))
])
def test_parse_frame(buffer, expected):
    got = parse_frame(buffer)
    assert got == expected


def test_error_decode():
    e = Error(b'Error message')
    assert e.decode() == 'Error message'


def test_simple_string_decode():
    s = SimpleString(b'OK')
    assert s.decode() == 'OK'


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
        Integer(b'42'),
        SimpleString(b'OK'),
        Array([Integer(b'1'), Integer(b'2')])
    ])

    expected = ['hello', 42, 'OK', [1, 2]]
    assert arr.decode() == expected


def test_null_array_decode():
    assert NullArray().decode() == []


def test_null_decode():
    assert Null().decode() == ''


def test_null_bulk_string_decode():
    assert NullBulkString().decode() == ''


def test_error_serialize():
    e = Error(b'Error message')
    assert e.serialize() == b'-Error message%(CRLF)s' % {b'CRLF': CRLF}


def test_simple_string_serialize():
    s = SimpleString(b'OK')
    assert s.serialize() == b'+OK%(CRLF)s' % {b'CRLF': CRLF}


def test_integer_serialize():
    i = Integer(b'10')
    assert i.serialize() == b':10%(CRLF)s' % {b'CRLF': CRLF}


def test_bulk_string_serialize():
    b = BulkString(b'full')
    assert b.serialize() == b'$4%(CRLF)sfull%(CRLF)s' % {b'CRLF': CRLF}


def test_null_bulk_string_serialize():
    assert NullBulkString().serialize() == b'$-1%(CRLF)s' % {b'CRLF': CRLF}


def test_array_serialize():
    a = Array([Integer(b"1"), Integer(b"2"), Array([SimpleString(b"full")]), BulkString(b'full')])
    assert a.serialize() == b'*4\r\n:1\r\n:2\r\n*1\r\n+full\r\n\r\n$4\r\nfull\r\n\r\n'


def test_null_array_serialize():
    assert NullArray().serialize() == b'*0%(CRLF)s' % {b'CRLF': CRLF}


def test_null_serialize():
    assert Null().serialize() == b'_%(CRLF)s' % {b'CRLF': CRLF}
