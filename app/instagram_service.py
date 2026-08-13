import json
import os
import random
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from instagrapi import Client
from instagrapi.exceptions import ChallengeRequired, LoginRequired, BadPassword, TwoFactorRequired

from .crypto import decrypt, encrypt
from .db import execute, get_setting, rows, set_setting, utcnow

DATA_DIR = os.getenv("DATA_DIR", os.path.join(os.getcwd(), "data"))
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
SESSION_FILE = os.path.join(DATA_DIR, "instagram_session.json")
LOCK = threading.RLock()
client = None
worker_started = False


def _bool(v, default=False):
    if v is None:
        return default
    return str(v).lower() in {"1", "true", "yes", "on"}


def status():
    return {
        "connected": _bool(get_setting("ig_connected", "false")),
        "username": get_setting("ig_username", ""),
        "last_poll": get_setting("last_poll", "Nunca"),
        "last_error": get_setting("last_error", ""),
        "welcome_enabled": _bool(get_setting("welcome_enabled", os.getenv("WELCOME_ENABLED", "false"))),
        "poll_seconds": int(get_setting("poll_seconds", os.getenv("POLL_SECONDS", "90"))),
        "max_dms_per_hour": int(get_setting("max_dms_per_hour", os.getenv("MAX_DMS_PER_HOUR", "12"))),
        "min_dm_delay_seconds": int(get_setting("min_dm_delay_seconds", os.getenv("MIN_DM_DELAY_SECONDS", "25"))),
        "welcome_message": get_setting("welcome_message", "Olá, {first_name}! 👋 Obrigado por seguir @{account}. Seja muito bem-vindo(a)!"),
    }


def save_config(message, enabled, poll_seconds, max_dms_per_hour, min_delay):
    set_setting("welcome_message", message.strip())
    set_setting("welcome_enabled", str(bool(enabled)).lower())
    set_setting("poll_seconds", max(60, int(poll_seconds)))
    set_setting("max_dms_per_hour", max(1, min(50, int(max_dms_per_hour))))
    set_setting("min_dm_delay_seconds", max(10, int(min_delay)))


def _load_client():
    global client
    with LOCK:
        if client:
            return client
        cl = Client()
        cl.delay_range = [1, 3]
        if os.path.exists(SESSION_FILE):
            try:
                cl.load_settings(SESSION_FILE)
            except Exception:
                pass
        username = get_setting("ig_username")
        enc = get_setting("ig_password_enc")
        if username and enc:
            password = decrypt(enc)
            try:
                cl.login(username, password, relogin=False)
                cl.dump_settings(SESSION_FILE)
                set_setting("ig_connected", "true")
                set_setting("last_error", "")
                client = cl
                return client
            except Exception as e:
                set_setting("ig_connected", "false")
                set_setting("last_error", f"Falha ao restaurar sessão: {type(e).__name__}: {e}")
        return None


def login(username, password, verification_code=None):
    global client
    with LOCK:
        cl = Client()
        cl.delay_range = [1, 3]
        if os.path.exists(SESSION_FILE):
            try:
                cl.load_settings(SESSION_FILE)
            except Exception:
                pass
        try:
            if verification_code:
                cl.login(username, password, verification_code=verification_code)
            else:
                cl.login(username, password)
            info = cl.account_info()
            cl.dump_settings(SESSION_FILE)
            set_setting("ig_username", username)
            set_setting("ig_password_enc", encrypt(password))
            set_setting("ig_connected", "true")
            set_setting("last_error", "")
            client = cl
            return True, f"Conectado como @{info.username}"
        except TwoFactorRequired:
            return False, "2FA_REQUIRED"
        except ChallengeRequired as e:
            set_setting("last_error", f"ChallengeRequired: {e}")
            return False, "O Instagram pediu uma verificação/challenge. Abra o Instagram oficial, confirme que foi você e tente novamente."
        except BadPassword:
            return False, "Usuário ou senha inválidos."
        except Exception as e:
            set_setting("last_error", f"{type(e).__name__}: {e}")
            return False, f"Falha no login: {type(e).__name__}: {e}"


def logout():
    global client
    with LOCK:
        client = None
        set_setting("ig_connected", "false")
        set_setting("ig_password_enc", "")
        try:
            os.remove(SESSION_FILE)
        except FileNotFoundError:
            pass


def _dm_count_last_hour():
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    r = rows("SELECT COUNT(*) AS n FROM dm_log WHERE status='sent' AND created_at >= ?", (cutoff,))
    return int(r[0]["n"]) if r else 0


def _render_message(template, user, account):
    first = (user.full_name or user.username or "").strip().split(" ")[0]
    return template.replace("{first_name}", first).replace("{username}", user.username or "").replace("{account}", account)


def sync_once(send_messages=True):
    cl = _load_client()
    if not cl:
        return {"ok": False, "message": "Instagram não conectado."}
    cfg = status()
    try:
        me = cl.account_info()
        followers = cl.user_followers(me.pk, amount=0)
        current_ids = {str(pk) for pk in followers.keys()}
        known = {r["pk"] for r in rows("SELECT pk FROM followers")}
        new_ids = current_ids - known
        baseline = len(known) == 0

        for pk, user in followers.items():
            pk = str(pk)
            if pk not in known:
                execute("INSERT INTO followers(pk,username,full_name,first_seen,welcomed,last_error) VALUES(?,?,?,?,?,?)", (
                    pk, user.username or "", user.full_name or "", utcnow(), 1 if baseline else 0, "baseline" if baseline else None
                ))

        sent = 0
        errors = 0
        if send_messages and cfg["welcome_enabled"] and not baseline:
            pending = rows("SELECT pk,username,full_name FROM followers WHERE welcomed=0 ORDER BY first_seen ASC LIMIT 25")
            for row in pending:
                if _dm_count_last_hour() >= cfg["max_dms_per_hour"]:
                    break
                user = followers.get(int(row["pk"])) or followers.get(row["pk"])
                if not user:
                    try:
                        user = cl.user_info(int(row["pk"]))
                    except Exception as e:
                        execute("UPDATE followers SET last_error=? WHERE pk=?", (str(e), row["pk"]))
                        errors += 1
                        continue
                msg = _render_message(cfg["welcome_message"], user, me.username)
                try:
                    cl.direct_send(msg, user_ids=[int(row["pk"])])
                    execute("UPDATE followers SET welcomed=1, welcomed_at=?, last_error=NULL WHERE pk=?", (utcnow(), row["pk"]))
                    execute("INSERT INTO dm_log(follower_pk,username,status,message,error,created_at) VALUES(?,?,?,?,?,?)", (row["pk"], row["username"], "sent", msg, None, utcnow()))
                    sent += 1
                    time.sleep(cfg["min_dm_delay_seconds"] + random.randint(0, 12))
                except Exception as e:
                    execute("UPDATE followers SET last_error=? WHERE pk=?", (f"{type(e).__name__}: {e}", row["pk"]))
                    execute("INSERT INTO dm_log(follower_pk,username,status,message,error,created_at) VALUES(?,?,?,?,?,?)", (row["pk"], row["username"], "error", msg, f"{type(e).__name__}: {e}", utcnow()))
                    errors += 1
                    if isinstance(e, LoginRequired):
                        set_setting("ig_connected", "false")
                        break
        set_setting("last_poll", utcnow())
        set_setting("last_error", "")
        return {"ok": True, "new": 0 if baseline else len(new_ids), "sent": sent, "errors": errors, "baseline": baseline, "total": len(followers)}
    except Exception as e:
        set_setting("last_poll", utcnow())
        set_setting("last_error", f"{type(e).__name__}: {e}")
        if isinstance(e, LoginRequired):
            set_setting("ig_connected", "false")
        return {"ok": False, "message": f"{type(e).__name__}: {e}"}


def worker_loop():
    while True:
        try:
            if status()["connected"]:
                sync_once(send_messages=True)
        except Exception as e:
            set_setting("last_error", f"Worker: {type(e).__name__}: {e}")
        time.sleep(max(60, status()["poll_seconds"]))


def start_worker():
    global worker_started
    if worker_started:
        return
    worker_started = True
    t = threading.Thread(target=worker_loop, daemon=True, name="instagram-worker")
    t.start()
