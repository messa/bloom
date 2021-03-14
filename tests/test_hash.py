import os
from time import monotonic as monotime


def test_fnv1a_32_python():
    from bloom._hashpy import fnv1a_32
    _test_fnv1a_32(fnv1a_32)
    _performance_hash(fnv1a_32)


def test_fnv1a_32_c():
    from bloom._hashc import fnv1a_32
    _test_fnv1a_32(fnv1a_32)
    _performance_hash(fnv1a_32)


def test_fnv1a_32_default():
    from bloom.hash import fnv1a_32
    _test_fnv1a_32(fnv1a_32)
    _performance_hash(fnv1a_32)


def _test_fnv1a_32(fnv1a_32):
    assert fnv1a_32(b'') == 2166136261
    assert fnv1a_32(b'hello') == 1335831723
    # test some known collisions
    assert fnv1a_32(b'costarring') == fnv1a_32(b'liquid')
    assert fnv1a_32(b'declinate') == fnv1a_32(b'macallums')


def _performance_hash(func):
    t0 = monotime()
    total_bytes = 0
    for i in range(50):
        sample = os.urandom(2**14)
        total_bytes += len(sample)
        func(sample)
    td = monotime() - t0
    mb_per_s = total_bytes / td / 2**20
    print(f"{func} performance: {mb_per_s:.2f} MB/s")


def test_fnv1a_64_python():
    from bloom._hashpy import fnv1a_64
    _test_fnv1a_64(fnv1a_64)
    _performance_hash(fnv1a_64)


def test_fnv1a_64_c():
    from bloom._hashc import fnv1a_64
    _test_fnv1a_64(fnv1a_64)
    _performance_hash(fnv1a_64)


def test_fnv1a_64_default():
    from bloom.hash import fnv1a_64
    _test_fnv1a_64(fnv1a_64)
    _performance_hash(fnv1a_64)


def _test_fnv1a_64(fnv1a_64):
    assert fnv1a_64(b'') == 14695981039346656037
    assert fnv1a_64(b'hello') == 11831194018420276491


def test_insert_bloom_fnv1a_32_python():
    from bloom._hashpy import insert_bloom_fnv1a_32
    _test_insert_bloom_fnv1a_32(insert_bloom_fnv1a_32)
    _performance_bloom(insert_bloom_fnv1a_32)


def test_insert_bloom_fnv1a_32_c():
    from bloom._hashc import insert_bloom_fnv1a_32
    _test_insert_bloom_fnv1a_32(insert_bloom_fnv1a_32)
    _performance_bloom(insert_bloom_fnv1a_32)


def test_insert_bloom_fnv1a_32_default():
    from bloom.hash import insert_bloom_fnv1a_32
    _test_insert_bloom_fnv1a_32(insert_bloom_fnv1a_32)
    _performance_bloom(insert_bloom_fnv1a_32)


def _test_insert_bloom_fnv1a_32(insert_bloom_fnv1a_32):
    array = bytearray(8)
    assert array.hex() == '0000000000000000'
    insert_bloom_fnv1a_32(array, b'ello', 4)
    assert array.hex() == '0000000000000000'
    insert_bloom_fnv1a_32(array, b'hello', 4)
    assert array.hex() == '0000000040000000'
    insert_bloom_fnv1a_32(array, b'hello', 4)
    assert array.hex() == '0000000040000000'
    insert_bloom_fnv1a_32(array, b'Lorem ipsum dolor sit amet', 4)
    assert array.hex() == 'a028180168600786'


def _performance_bloom(func):
    t0 = monotime()
    total_bytes = 0
    array = bytearray(2**16)
    for i in range(50):
        data = os.urandom(2**12)
        total_bytes += len(data)
        func(array, data, 4)
    td = monotime() - t0
    mb_per_s = total_bytes / td / 2**20
    print(f"{func} performance: {mb_per_s:.2f} MB/s")


def test_insert_bloom_fnv1a_64_python():
    from bloom._hashpy import insert_bloom_fnv1a_64
    _test_insert_bloom_fnv1a_64(insert_bloom_fnv1a_64)
    _performance_bloom(insert_bloom_fnv1a_64)


def test_insert_bloom_fnv1a_64_c():
    from bloom._hashc import insert_bloom_fnv1a_64
    _test_insert_bloom_fnv1a_64(insert_bloom_fnv1a_64)
    _performance_bloom(insert_bloom_fnv1a_64)


def test_insert_bloom_fnv1a_64_default():
    from bloom.hash import insert_bloom_fnv1a_64
    _test_insert_bloom_fnv1a_64(insert_bloom_fnv1a_64)
    _performance_bloom(insert_bloom_fnv1a_64)


def _test_insert_bloom_fnv1a_64(insert_bloom_fnv1a_64):
    array = bytearray(8)
    assert array.hex() == '0000000000000000'
    insert_bloom_fnv1a_64(array, b'ello', 4)
    assert array.hex() == '0000000000000000'
    insert_bloom_fnv1a_64(array, b'hello', 4)
    assert array.hex() == '0000000040000000'
    insert_bloom_fnv1a_64(array, b'hello', 4)
    assert array.hex() == '0000000040000000'
    insert_bloom_fnv1a_64(array, b'Lorem ipsum dolor sit amet', 4)
    assert array.hex() == '80200d8768681286'
