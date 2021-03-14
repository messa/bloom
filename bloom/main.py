from argparse import ArgumentParser
from logging import getLogger
import os
from pathlib import Path
import sys

from .database import open_database
from .hash import insert_bloom_fnv1a_64


logger = getLogger(__name__)

log_format = '%(asctime)s %(name)s %(levelname)5s: %(message)s'

hash_func_name = 'fnv1a_64'
bloom_index_func = insert_bloom_fnv1a_64
array_bytesize = 2**16
sample_size = 4


def bloom_main():
    p = ArgumentParser()
    p.add_argument('--db', help='path to database file')
    p.add_argument('--file', '-f', action='append', help='file to process')
    p.add_argument('--index', help='index only, do not perform search')
    p.add_argument('expression', nargs='*')
    args = p.parse_args()
    setup_logging()
    if not args.index and not args.expression:
        sys.exit('ERROR: No expression provided')
    if args.db:
        db_path = Path(args.db)
    elif os.environ.get('BLOOM_DB'):
        db_path = Path(os.environ['BLOOM_DB'])
    else:
        db_path = Path('~/.cache/bloom/db').expanduser()
    logger.debug('DB path: %s', db_path)
    db = open_database(db_path)
    try:
        if args.file:
            # file paths were given on cmd line
            file_paths = [Path(f) for f in args.file]
        else:
            # file paths are expected on stdin, one per line
            file_paths = (Path(line.rstrip('\r\n')) for line in sys.stdin)
        if args.index:
            index_files(db, file_paths)
        else:
            for matching_path in filter_files(db, file_paths, args.expression):
                print(matching_path, flush=True)
    finally:
        db.close()


def index_files(db, paths):
    for path in paths:
        path_resolved = path.resolve()
        with path.open(mode='rb') as f:
            f_stat = os.fstat(f.fileno())
            file_array = db.get_file_array(path_resolved, f_stat.st_size, f_stat.st_mtime, hash_func_name)
            if file_array:
                logger.debug('File is up-to-date: %s', path)
            else:
                logger.info('Indexing file: %s', path)
                file_array = compute_file_bloom_array(f)
                db.set_file_array(path_resolved, f_stat.st_size, f_stat.st_mtime, hash_func_name, file_array)


def filter_files(db, paths, expressions):
    for path in paths:
        path_resolved = path.resolve()
        with path.open(mode='rb') as f:
            f_stat = os.fstat(f.fileno())
            file_array = db.get_file_array(path_resolved, f_stat.st_size, f_stat.st_mtime, hash_func_name)
            if not file_array:
                logger.debug('Indexing file: %s', path)
                file_array = compute_file_bloom_array(f)
                db.set_file_array(path_resolved, f_stat.st_size, f_stat.st_mtime, hash_func_name, file_array)
            match_array = construct_match_array(len(file_array), expressions)
            assert len(match_array) == len(file_array)
            have_match = all((cf & cm) == cm for cm, cf in zip(match_array, file_array))
            if have_match:
                yield path


def construct_match_array(bytesize, expressions):
    match_array = bytearray(bytesize)
    for expr in expressions:
        assert isinstance(expr, str)
        expr_bytes = expr.lower().encode('utf-8')
        bloom_index_func(match_array, expr_bytes, sample_size)
    return bytes(match_array)


def compute_file_bloom_array(stream):
    raise Exception('NIY')


def setup_logging():
    from logging import DEBUG, basicConfig
    basicConfig(format=log_format, level=DEBUG)
