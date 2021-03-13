def test_import():
    import bloom

def test_hashc():
    from bloom.hashc import hello
    assert hello("hi") == 2
