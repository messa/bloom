import os
import sys
from pathlib import Path
from subprocess import check_call


def test_import():
    pass


def test_hashc():
    from bloom._hashc import hello

    assert hello('hi') == 2


def test_run_help():
    os.environ['PATH'] = str(Path(sys.executable).parent) + ':' + os.environ['PATH']
    check_call(['bloom', '--help'])
