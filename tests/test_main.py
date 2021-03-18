import gzip
import lzma
from pytest import fixture
import zlib

from bloom.main import construct_match_array, array_is_subset, construct_file_array, index_file, match_file, open_database


def test_construct_match_array():
    array = construct_match_array(16, ['hello', 'world'], sample_sizes=[4])
    assert isinstance(array, bytes)
    assert array.hex() == '00002000600000000000000000001000'


def test_array_is_subset():
    assert array_is_subset(bytes.fromhex('00'), bytes.fromhex('00')) is True
    assert array_is_subset(bytes.fromhex('00'), bytes.fromhex('01')) is True
    assert array_is_subset(bytes.fromhex('01'), bytes.fromhex('00')) is False
    assert array_is_subset(bytes.fromhex('01'), bytes.fromhex('02')) is False
    assert array_is_subset(bytes.fromhex('01'), bytes.fromhex('03')) is True


def test_construct_file_array_simple(temp_dir):
    p = temp_dir / 'one.txt'
    p.write_text('hello\nworld\n')
    with p.open(mode='rb') as f:
        array = construct_file_array(f, array_bytesize=16, sample_sizes=[4])
    assert isinstance(array, bytes)
    assert array.hex() == '00002000600000000000000000001000'


def test_construct_file_array_gzip(temp_dir):
    p = temp_dir / 'one.txt.gz'
    with gzip.open(p, mode='wb') as f:
        f.write(b'hello\nworld\n')
    with p.open(mode='rb') as f:
        array = construct_file_array(f, array_bytesize=16, sample_sizes=[4])
    assert isinstance(array, bytes)
    assert array.hex() == '00002000600000000000000000001000'


def test_construct_file_array_xz(temp_dir):
    p = temp_dir / 'one.txt.xz'
    with lzma.open(p, mode='wb') as f:
        f.write(b'hello\nworld\n')
    with p.open(mode='rb') as f:
        array = construct_file_array(f, array_bytesize=16, sample_sizes=[4])
    assert isinstance(array, bytes)
    assert array.hex() == '00002000600000000000000000001000'


@fixture
def db(temp_dir):
    return open_database(temp_dir / 'db')


def test_index_files(temp_dir, db):
    p1 = temp_dir / 'one.txt'
    p1.write_bytes(b'Lorem ipsum dolor sit amet.\nThis is second line.\n')
    p2 = temp_dir / 'two.txt.gz'
    p2.write_bytes(gzip.compress(b'This is a compressed file.\n'))
    index_file(db, p1, array_bytesize=16)
    index_file(db, p2, array_bytesize=16)
    cur = db._connect().cursor()
    cur.execute('SELECT key, array FROM bloom_files_v2')
    row1, row2 = sorted(cur.fetchall())
    assert row1[0] == f"{p1}:{p1.stat().st_size}:{p1.stat().st_mtime}:fnv1a_64:4,5,6"
    assert zlib.decompress(row1[1]).hex() == '97e126c173ff9373a75d1967f97219ec'
    assert row2[0] == f"{p2}:{p2.stat().st_size}:{p2.stat().st_mtime}:fnv1a_64:4,5,6"
    assert zlib.decompress(row2[1]).hex() == '12e3c6f14a0792e8836c194a4e8f00e0'


def test_filter_files(temp_dir, db):
    p1 = temp_dir / 'one.txt'
    p1.write_bytes(b'Lorem ipsum dolor sit amet.\nThis is second line.\n')
    p2 = temp_dir / 'two.txt.gz'
    p2.write_bytes(gzip.compress(b'This is a compressed file.\n'))
    filter_files = lambda db, paths, q: [p for p in paths if match_file(db, q, p)]
    assert list(filter_files(db, [p1, p2], ['This'])) == [p1, p2]
    assert list(filter_files(db, [p1, p2], ['compressed'])) == [p2]
    assert list(filter_files(db, [p1, p2], ['nothing'])) == []
