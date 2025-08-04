from logging import getLogger
from sqlite3 import connect as sqlite3_connect
from time import time
from zlib import compress as zlib_compress
from zlib import decompress as zlib_decompress

logger = getLogger(__name__)


def open_database(db_path):
    if not db_path.parent.is_dir():
        if not db_path.parent.parent.is_dir():
            logger.info('Creating directory %s', db_path.parent.parent)
            db_path.parent.parent.mkdir()
        logger.info('Creating directory %s', db_path.parent)
        db_path.parent.mkdir()
    db = Database(db_path)
    db.init()
    db.close()
    return db


class Database:
    def __init__(self, db_path):
        self._db_path = db_path
        self._connection = None

    def _connect(self):
        if not self._connection:
            conn = sqlite3_connect(self._db_path, timeout=30)
            self._connection = conn
        return self._connection

    def close(self):
        if self._connection:
            self._connection.close()
            self._connection = None

    def init(self):
        cur = self._connect().cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bloom_files_v3 (
                path TEXT,
                key TEXT,
                part INTEGER,
                created INTEGER,
                array BLOB
            )
        """)
        cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx1 ON bloom_files_v3 ( path, key, part )')
        self._connection.commit()

    def get_file_arrays(self, path, size, mtime, version, sample_sizes):
        sample_sizes_str = ','.join(str(i) for i in sample_sizes)
        key = f'{size}:{mtime}:{version}:{sample_sizes_str}'
        cur = self._connect().cursor()
        cur.execute('SELECT part, array FROM bloom_files_v3 WHERE path=? AND key=?', (str(path), key))
        rows = sorted(cur.fetchall())
        self._connection.rollback()
        return [zlib_decompress(row[1]) for row in rows]

    def set_file_arrays(self, path, size, mtime, version, sample_sizes, arrays):
        assert all(isinstance(a, bytes) for a in arrays)
        sample_sizes_str = ','.join(str(i) for i in sample_sizes)
        key = f'{size}:{mtime}:{version}:{sample_sizes_str}'
        now = int(time())
        compressed_arrays = [zlib_compress(a) for a in arrays]
        logger.debug('Array compression: %.2f kB -> %.2f kB', sum(len(a) for a in arrays) / 1024, sum(len(a) for a in compressed_arrays) / 1024)
        cur = self._connect().cursor()
        # The upsert syntax works in sqlite since 3.24.0, but it seems some Python installations have older version
        # cur.execute('''
        #     INSERT INTO bloom_files_v1 (key, created, array) VALUES (?, ?, ?)
        #     ON CONFLICT (key) DO UPDATE SET created=?, array=?
        # ''', (key, now, array, now, array))
        # So let's do DELETE + INSERT instead :)
        cur.execute('DELETE FROM bloom_files_v3 WHERE path=?', (str(path),))
        for n, a in enumerate(compressed_arrays):
            cur.execute('INSERT INTO bloom_files_v3 (path, key, part, created, array) VALUES (?, ?, ?, ?, ?)', (str(path), key, n, now, a))
        self._connection.commit()
