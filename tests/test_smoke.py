from os import environ
from pathlib import Path
from subprocess import check_call
from sys import executable


def test_import():
    pass


def test_hashc():
    from bloom._hashc import hello

    assert hello('hi') == 2


def test_run_help():
    environ['PATH'] = str(Path(executable).parent) + ':' + environ['PATH']
    check_call(['bloom', '--help'])
