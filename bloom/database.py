from logging import getLogger
import sqlite3
from time import time
import zlib


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
            conn = sqlite3.connect(self._db_path, timeout=30)
            conn.execute('pragma journal_mode=wal')
            self._connection = conn
        return self._connection

    def close(self):
        if self._connection:
            self._connection.close()
            self._connection = None

    def init(self):
        cur = self._connect().cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS bloom_files_v2 (
                key TEXT PRIMARY KEY,
                path TEXT,
                created INTEGER,
                array BLOB
            )
        ''')
        self._connection.commit()

    def get_file_array(self, path, size, mtime, version, sample_sizes):
        sample_sizes_str = ','.join(str(i) for i in sample_sizes)
        key = f"{path}:{size}:{mtime}:{version}:{sample_sizes_str}"
        cur = self._connect().cursor()
        cur.execute('SELECT array FROM bloom_files_v2 WHERE key=?', (key, ))
        row = cur.fetchone()
        self._connection.rollback()
        return zlib.decompress(row[0]) if row else None

    def set_file_array(self, path, size, mtime, version, sample_sizes, array):
        assert isinstance(array, bytes)
        sample_sizes_str = ','.join(str(i) for i in sample_sizes)
        key = f"{path}:{size}:{mtime}:{version}:{sample_sizes_str}"
        now = int(time())
        cur = self._connect().cursor()
        # The upsert syntax works in sqlite since 3.24.0, but it seems some Python installations have older version
        #cur.execute('''
        #    INSERT INTO bloom_files_v1 (key, created, array) VALUES (?, ?, ?)
        #    ON CONFLICT (key) DO UPDATE SET created=?, array=?
        #''', (key, now, array, now, array))
        # So let's do DELETE + INSERT instead :)
        cur.execute('DELETE FROM bloom_files_v2 WHERE path=?', (str(path), ))
        cur.execute('''
            INSERT INTO bloom_files_v2 (key, path, created, array) VALUES (?, ?, ?, ?)
        ''', (key, str(path), now, zlib.compress(array)))
        self._connection.commit()
