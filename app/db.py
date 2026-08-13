import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DATA_DIR = os.getenv("DATA_DIR", os.path.join(os.getcwd(), "data"))
os.makedirs(DATA_DIR, exist_ok=True)
SQLITE_PATH = os.path.join(DATA_DIR, "app.db")


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def _is_postgres():
    return DATABASE_URL.startswith("postgres://") or DATABASE_URL.startswith("postgresql://")


@contextmanager
def conn():
    if _is_postgres():
        import psycopg
        url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        c = psycopg.connect(url, autocommit=True)
        try:
            yield c
        finally:
            c.close()
    else:
        c = sqlite3.connect(SQLITE_PATH, check_same_thread=False)
        c.row_factory = sqlite3.Row
        try:
            yield c
            c.commit()
        finally:
            c.close()


def qmark(sql):
    return sql.replace("?", "%s") if _is_postgres() else sql


def init_db():
    serial = "BIGSERIAL PRIMARY KEY" if _is_postgres() else "INTEGER PRIMARY KEY AUTOINCREMENT"
    booltype = "BOOLEAN" if _is_postgres() else "INTEGER"
    with conn() as c:
        cur = c.cursor()
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )""")
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS followers (
            pk TEXT PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            first_seen TEXT NOT NULL,
            welcomed {booltype} NOT NULL DEFAULT 0,
            welcomed_at TEXT,
            last_error TEXT
        )""")
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS dm_log (
            id {serial},
            follower_pk TEXT,
            username TEXT,
            status TEXT NOT NULL,
            message TEXT,
            error TEXT,
            created_at TEXT NOT NULL
        )""")
        cur.close()


def get_setting(key, default=None):
    with conn() as c:
        cur = c.cursor()
        cur.execute(qmark("SELECT value FROM settings WHERE key=?"), (key,))
        row = cur.fetchone()
        cur.close()
        if not row:
            return default
        return row[0] if not hasattr(row, "keys") else row["value"]


def set_setting(key, value):
    with conn() as c:
        cur = c.cursor()
        if _is_postgres():
            cur.execute("INSERT INTO settings(key,value) VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value", (key, str(value)))
        else:
            cur.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))
        cur.close()


def rows(sql, params=()):
    with conn() as c:
        cur = c.cursor()
        cur.execute(qmark(sql), params)
        result = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        cur.close()
        out=[]
        for r in result:
            if hasattr(r, "keys"):
                out.append(dict(r))
            else:
                out.append(dict(zip(cols, r)))
        return out


def execute(sql, params=()):
    with conn() as c:
        cur = c.cursor()
        cur.execute(qmark(sql), params)
        cur.close()
