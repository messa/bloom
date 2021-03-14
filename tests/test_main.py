import gzip
import lzma
from bloom.main import construct_match_array, array_is_subset, construct_file_array


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
