from argparse import ArgumentParser
from logging import getLogger


logger = getLogger(__name__)

log_format = '%(asctime)s %(name)s %(levelname)5s: %(message)s'


def bloom_main():
    p = ArgumentParser()
    args = p.parse_args()
    setup_logging()


def setup_logging():
    from logging import DEBUG, basicConfig
    basicConfig(format=log_format, level=DEBUG)
