from logging import getLogger
import sqlite3
from time import time


logger = getLogger(__name__)


def open_database(db_path):
    conn = sqlite3.connect(db_path)
    db = Database(conn)
    db.init()
    return db


class Database:

    def __init__(self, conn):
        self._conn = conn

    def close(self):
        self._conn.close()
        self._conn = None

    def init(self):
        cur = self._conn.cursor()
        cur.execute('''
            CREATE TABLE bloom_files_v1 (
                key text PRIMARY KEY,
                created integer,
                last_accessed integer,
                array blob
            )
        ''')
        self._conn.commit()

    def get_file_array(self, path, size, mtime, version, sample_size):
        key = f"{path}:{size}:{mtime}:{version}:{sample_size}"
        cur = self._conn.cursor()
        cur.execute('SELECT array FROM bloom_files_v1 WHERE key=?', (key, ))
        row = cur.fetchone()
        if row:
            cur.execute('UPDATE array SET last_accessed=? WHERE key=?', (int(time()), key))
        self._conn.commit()
        return row[0] if row else None

    def set_file_array(self, path, size, mtime, version, sample_size, array):
        assert isinstance(array, bytes)
        key = f"{path}:{size}:{mtime}:{version}:{sample_size}"
        now = int(time())
        cur = self._conn.cursor()
        cur.execute('''
            INSERT INTO bloom_files_v1 (key, created, last_accessed, array) VALUES (?, ?, ?, ?)
            ON CONFLICT (key) DO UPDATE SET created=?, last_accessed=?, array=?
        ''', (key, now, now, array, now, now, array))
        self._conn.commit()
