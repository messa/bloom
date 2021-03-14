import gzip
import lzma
from pytest import fixture

from bloom.main import construct_match_array, array_is_subset, construct_file_array, index_files, filter_files, open_database


def test_construct_match_array():
    array = construct_match_array(16, ['hello', 'world'], sample_size=4)
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
        array = construct_file_array(f, array_bytesize=16, sample_size=4)
    assert isinstance(array, bytes)
    assert array.hex() == '00002000600000000000000000001000'


def test_construct_file_array_gzip(temp_dir):
    p = temp_dir / 'one.txt.gz'
    with gzip.open(p, mode='wb') as f:
        f.write(b'hello\nworld\n')
    with p.open(mode='rb') as f:
        array = construct_file_array(f, array_bytesize=16, sample_size=4)
    assert isinstance(array, bytes)
    assert array.hex() == '00002000600000000000000000001000'


def test_construct_file_array_xz(temp_dir):
    p = temp_dir / 'one.txt.xz'
    with lzma.open(p, mode='wb') as f:
        f.write(b'hello\nworld\n')
    with p.open(mode='rb') as f:
        array = construct_file_array(f, array_bytesize=16, sample_size=4)
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
    index_files(db, [p1, p2], array_bytesize=16)
    cur = db._conn.cursor()
    cur.execute('SELECT key, array FROM bloom_files_v1')
    row1, row2 = sorted(cur.fetchall())
    assert row1[0] == f"{p1}:{p1.stat().st_size}:{p1.stat().st_mtime}:fnv1a_64:4"
    assert row1[1].hex() == '8420048122080262a4041907386011a4'
    assert row2[0] == f"{p2}:{p2.stat().st_size}:{p2.stat().st_mtime}:fnv1a_64:4"
    assert row2[1].hex() == '108284100a0402c0004c004246080000'
