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
bloom_insert_func = insert_bloom_fnv1a_64
array_bytesize = 2**16


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
    if args.file:
        # file paths were given on cmd line
        file_paths = [Path(f) for f in args.file]
    else:
        # file paths are expected on stdin, one per line
        file_paths = (Path(line.rstrip('\r\n')) for line in sys.stdin)
    for path in file_paths:
        path_resolved = path.resolve()
        array = db.get_file_array(path_resolved, hash_func_name)
        if not array:
            array = compute_file_bloom_array(path, bloom_insert_func)
            db.set_file_array(path_resolved, hash_func_name, array)




    db.close()



def setup_logging():
    from logging import DEBUG, basicConfig
    basicConfig(format=log_format, level=DEBUG)
