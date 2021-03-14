from pathlib import Path
from pytest import fixture


@fixture
def temp_dir(tmpdir):
    return Path(tmpdir)
