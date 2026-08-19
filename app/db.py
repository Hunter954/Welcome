import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

DATABASE_URL = os.getenv('DATABASE_URL','').strip()
DATA_DIR = os.getenv('DATA_DIR', os.path.join(os.getcwd(),'data'))
os.makedirs(DATA_DIR, exist_ok=True)
SQLITE_PATH = os.path.join(DATA_DIR,'app.db')

def utcnow(): return datetime.now(timezone.utc).isoformat()
def _is_postgres(): return DATABASE_URL.startswith('postgres://') or DATABASE_URL.startswith('postgresql://')

@contextmanager
def conn():
    if _is_postgres():
        import psycopg
        c=psycopg.connect(DATABASE_URL.replace('postgres://','postgresql://',1), autocommit=True)
        try: yield c
        finally: c.close()
    else:
        c=sqlite3.connect(SQLITE_PATH, check_same_thread=False); c.row_factory=sqlite3.Row
        try: yield c; c.commit()
        finally: c.close()

def qmark(sql): return sql.replace('?', '%s') if _is_postgres() else sql

def init_db():
    serial='BIGSERIAL PRIMARY KEY' if _is_postgres() else 'INTEGER PRIMARY KEY AUTOINCREMENT'
    booltype='BOOLEAN' if _is_postgres() else 'INTEGER'; false='FALSE' if _is_postgres() else '0'
    schemas=[
      '''CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY,value TEXT NOT NULL)''',
      f'''CREATE TABLE IF NOT EXISTS followers (pk TEXT PRIMARY KEY,username TEXT,full_name TEXT,first_seen TEXT NOT NULL,welcomed {booltype} NOT NULL DEFAULT {false},welcomed_at TEXT,last_error TEXT)''',
      f'''CREATE TABLE IF NOT EXISTS dm_log (id {serial},follower_pk TEXT,username TEXT,status TEXT NOT NULL,message TEXT,error TEXT,created_at TEXT NOT NULL)''',
      f'''CREATE TABLE IF NOT EXISTS app_log (id {serial},level TEXT NOT NULL,event TEXT NOT NULL,message TEXT,details TEXT,created_at TEXT NOT NULL)''',
      '''CREATE TABLE IF NOT EXISTS contacts (pk TEXT PRIMARY KEY,username TEXT,full_name TEXT,avatar_url TEXT,status TEXT DEFAULT 'lead',score INTEGER DEFAULT 0,tags TEXT DEFAULT '',notes TEXT DEFAULT '',phone TEXT DEFAULT '',email TEXT DEFAULT '',company TEXT DEFAULT '',city TEXT DEFAULT '',source TEXT DEFAULT '',first_contact TEXT,last_interaction TEXT,assigned_to TEXT DEFAULT '')''',
      '''CREATE TABLE IF NOT EXISTS inbox_threads (thread_id TEXT PRIMARY KEY,title TEXT,username TEXT,user_pk TEXT,avatar_url TEXT,last_message TEXT,last_message_at TEXT,unread INTEGER DEFAULT 0,raw_json TEXT DEFAULT '')''',
      f'''CREATE TABLE IF NOT EXISTS inbox_messages (id {serial},thread_id TEXT,item_id TEXT UNIQUE,user_pk TEXT,username TEXT,direction TEXT,message_type TEXT,text TEXT,created_at TEXT,raw_json TEXT DEFAULT '')''',
      f'''CREATE TABLE IF NOT EXISTS automations (id {serial},name TEXT NOT NULL,trigger_type TEXT NOT NULL,keyword TEXT DEFAULT '',match_mode TEXT DEFAULT 'contains',scope TEXT DEFAULT 'all',media_id TEXT DEFAULT '',reply_text TEXT DEFAULT '',dm_text TEXT DEFAULT '',tag TEXT DEFAULT '',enabled {booltype} NOT NULL DEFAULT {false},executions INTEGER DEFAULT 0,failures INTEGER DEFAULT 0,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)''',
      f'''CREATE TABLE IF NOT EXISTS quick_replies (id {serial},shortcut TEXT UNIQUE NOT NULL,title TEXT DEFAULT '',message TEXT NOT NULL,created_at TEXT NOT NULL)''',
      '''CREATE TABLE IF NOT EXISTS media_cache (pk TEXT PRIMARY KEY,media_type TEXT,product_type TEXT,caption TEXT,thumbnail_url TEXT,media_url TEXT,taken_at TEXT,like_count INTEGER DEFAULT 0,comment_count INTEGER DEFAULT 0,raw_json TEXT DEFAULT '')''',
      '''CREATE TABLE IF NOT EXISTS comment_cache (pk TEXT PRIMARY KEY,media_pk TEXT,user_pk TEXT,username TEXT,text TEXT,created_at TEXT,replied INTEGER DEFAULT 0,raw_json TEXT DEFAULT '')''',
      f'''CREATE TABLE IF NOT EXISTS scheduled_posts (id {serial},kind TEXT DEFAULT 'photo',file_path TEXT,caption TEXT,status TEXT DEFAULT 'draft',scheduled_at TEXT,published_media_pk TEXT,error TEXT,created_at TEXT NOT NULL)''',
    ]
    with conn() as c:
      cur=c.cursor(); lock=684731920114
      if _is_postgres(): cur.execute('SELECT pg_advisory_lock(%s)',(lock,))
      try:
        for sql in schemas: cur.execute(sql)
        defaults=[('/preco','Preço','Olá! Posso te passar os valores e entender o que você precisa 👇'),('/whatsapp','WhatsApp','Claro! Me passa seu WhatsApp com DDD e continuamos por lá.'),('/portfolio','Portfólio','Separei nosso portfólio para você. Quer que eu te indique os cases mais parecidos com o seu projeto?')]
        for shortcut,title,msg in defaults:
          try:
            cur.execute(qmark('INSERT INTO quick_replies(shortcut,title,message,created_at) VALUES(?,?,?,?)'),(shortcut,title,msg,utcnow()))
          except Exception: pass
      finally:
        if _is_postgres(): cur.execute('SELECT pg_advisory_unlock(%s)',(lock,))
        cur.close()

def get_setting(key,default=None):
  with conn() as c:
    cur=c.cursor(); cur.execute(qmark('SELECT value FROM settings WHERE key=?'),(key,)); r=cur.fetchone(); cur.close()
    return default if not r else (r['value'] if hasattr(r,'keys') else r[0])

def set_setting(key,value):
  with conn() as c:
    cur=c.cursor()
    if _is_postgres(): cur.execute('INSERT INTO settings(key,value) VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value',(key,str(value)))
    else: cur.execute('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(key,str(value)))
    cur.close()

def rows(sql,params=()):
  with conn() as c:
    cur=c.cursor(); cur.execute(qmark(sql),params); result=cur.fetchall(); cols=[d[0] for d in cur.description] if cur.description else []; cur.close()
    return [dict(r) if hasattr(r,'keys') else dict(zip(cols,r)) for r in result]

def execute(sql,params=()):
  with conn() as c:
    cur=c.cursor(); cur.execute(qmark(sql),params); cur.close()
