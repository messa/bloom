'''
Pure-Python implementation of the hash algorithms from hashcmodule.c.
'''

from functools import partial


def fnv1a_32(sample):
    # Source: https://softwareengineering.stackexchange.com/a/145633/22518
    assert isinstance(sample, bytes)
    hval = 2166136261
    prime = 16777619
    uint32_max = 2 ** 32
    for c in sample:
        hval = hval ^ c
        hval = (hval * prime) % uint32_max
    return hval


def fnv1a_64(sample):
    # Source: https://softwareengineering.stackexchange.com/a/145633/22518
    assert isinstance(sample, bytes)
    hval = 14695981039346656037
    prime = 1099511628211
    uint64_max = 2 ** 64
    for c in sample:
        hval = hval ^ c
        hval = (hval * prime) % uint64_max
    return hval


def insert_bloom(hash_func, array, data, sample_size):
    bitsize = len(array) * 8
    for offset in range(len(data) - sample_size + 1):
        sample = data[offset:offset + sample_size]
        assert len(sample) == sample_size
        h = hash_func(sample) % bitsize
        array[h // 8] |= 1 << (h % 8)


insert_bloom_fnv1a_32 = partial(insert_bloom, fnv1a_32)
insert_bloom_fnv1a_64 = partial(insert_bloom, fnv1a_64)
