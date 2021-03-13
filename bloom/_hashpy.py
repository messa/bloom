'''
Pure-Python implementation of the hash algorithms from hashcmodule.c.
'''


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
