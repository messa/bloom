from pathlib import Path
import os
from subprocess import check_call
import sys


def test_import():
    import bloom


def test_hashc():
    from bloom._hashc import hello
    assert hello("hi") == 2


def test_run_help():
    os.environ['PATH'] = str(Path(sys.executable).parent) + ':' + os.environ['PATH']
    check_call(['bloom', '--help'])
