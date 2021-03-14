from argparse import ArgumentParser
import gzip
from logging import getLogger
import lzma
import os
from pathlib import Path
import sys

from .database import open_database
from .hash import insert_bloom_fnv1a_64


logger = getLogger(__name__)

log_format = '%(asctime)s %(name)s %(levelname)5s: %(message)s'

hash_func_name = 'fnv1a_64'
bloom_index_func = insert_bloom_fnv1a_64
default_array_bytesize = 2**16
sample_size = 4


def bloom_main():
    p = ArgumentParser()
    p.add_argument('--verbose', '-v', action='store_true')
    p.add_argument('--db', help='path to database file')
    p.add_argument('--file', '-f', action='append', help='file to process')
    p.add_argument('--index', help='index only, do not perform search')
    p.add_argument('expression', nargs='*')
    args = p.parse_args()
    setup_logging(args.verbose)
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


def index_files(db, paths, array_bytesize=default_array_bytesize):
    for path in paths:
        path_resolved = path.resolve()
        with path.open(mode='rb') as f:
            f_stat = os.fstat(f.fileno())
            file_array = db.get_file_array(path_resolved, f_stat.st_size, f_stat.st_mtime, hash_func_name, sample_size)
            if file_array:
                logger.debug('File is up-to-date: %s', path)
            else:
                logger.info('Indexing file: %s', path)
                file_array = construct_file_array(f, array_bytesize=array_bytesize, sample_size=sample_size)
                db.set_file_array(path_resolved, f_stat.st_size, f_stat.st_mtime, hash_func_name, sample_size, file_array)


def filter_files(db, paths, expressions, array_bytesize=default_array_bytesize):
    for path in paths:
        path_resolved = path.resolve()
        with path.open(mode='rb') as f:
            f_stat = os.fstat(f.fileno())
            file_array = db.get_file_array(path_resolved, f_stat.st_size, f_stat.st_mtime, hash_func_name, sample_size)
            if not file_array:
                logger.debug('Indexing file: %s', path)
                file_array = construct_file_array(f, array_bytesize=array_bytesize, sample_size=sample_size)
                logger.debug('Bloom array stats: %.1f %% filled', 100 * count_ones(file_array) / (len(file_array) * 8))
                db.set_file_array(path_resolved, f_stat.st_size, f_stat.st_mtime, hash_func_name, sample_size, file_array)
            match_array = construct_match_array(len(file_array), expressions, sample_size=sample_size)
            if array_is_subset(match_array, file_array):
                logger.debug('File possibly matching: %s', path)
                yield path
            else:
                logger.debug('File does not match: %s', path)


def count_ones(array):
    ones = 0
    for c in array:
        for x in range(8):
            ones += (c >> x) & 1
    return ones


def construct_match_array(array_bytesize, expressions, sample_size):
    match_array = bytearray(array_bytesize)
    for expr in expressions:
        assert isinstance(expr, str)
        expr_bytes = expr.lower().encode('utf-8')
        bloom_index_func(match_array, expr_bytes, sample_size)
    return bytes(match_array)


def array_is_subset(match_array, file_array):
    assert len(match_array) == len(file_array)
    return all((cf & cm) == cm for cm, cf in zip(match_array, file_array))


def construct_file_array(raw_stream, array_bytesize, sample_size):
    assert raw_stream.tell() == 0
    header = raw_stream.read(20)
    raw_stream.seek(0)
    assert raw_stream.tell() == 0
    if header.startswith(bytes.fromhex('1f8b')):
        logger.debug('gzip compression detected')
        stream = gzip.open(raw_stream, mode='rb')
    elif header.startswith(bytes.fromhex('28b52ffd')):
        logger.debug('zstd compression detected')
        raise Exception('zstd decompression not implemented')
    elif header.startswith(bytes.fromhex('FD377A585A00')):
        logger.debug('xz compression detected')
        stream = lzma.open(raw_stream, mode='rb')
    else:
        try:
            header.decode('utf-8')
        except UnicodeDecodeError:
            logger.warning('No compression detected, but file header is not utf-8? %r', header)
        else:
            logger.debug('No compression detected')
        stream = raw_stream
    file_array = bytearray(array_bytesize)
    for line in stream:
        assert isinstance(line, bytes)
        line = line.rstrip()
        line = line.decode('utf-8').lower().encode('utf-8')
        bloom_index_func(file_array, line, sample_size)
    return bytes(file_array)


def setup_logging(verbose):
    from logging import DEBUG, INFO, basicConfig
    basicConfig(format=log_format, level=DEBUG if verbose else INFO)
