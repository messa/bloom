from bloom.main import construct_match_array


def test_construct_match_array():
    array = construct_match_array(16, ['hello', 'world'])
    assert isinstance(array, bytes)
    assert array.hex() == '00000000600000000000000000000000'
