import asyncio
import os
import random
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

from aiograpi import Client
from aiograpi.exceptions import BadPassword, ChallengeRequired, LoginRequired, TwoFactorRequired

from .crypto import decrypt, encrypt
from .db import _is_postgres, execute, get_setting, rows, set_setting, utcnow

DATA_DIR = os.getenv("DATA_DIR", os.path.join(os.getcwd(), "data"))
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
SESSION_FILE = os.path.join(DATA_DIR, "instagram_session.json")

# aiograpi is fully async. Flask/Gunicorn in this project remains synchronous,
# so all Instagram I/O is kept on one dedicated asyncio event loop.
AIO_LOOP = asyncio.new_event_loop()
AIO_THREAD = None
AIO_THREAD_LOCK = threading.Lock()
CLIENT_LOCK = threading.RLock()
client = None
worker_started = False


def _aio_loop_worker():
    asyncio.set_event_loop(AIO_LOOP)
    AIO_LOOP.run_forever()


def _ensure_aio_loop():
    global AIO_THREAD
    if AIO_THREAD and AIO_THREAD.is_alive():
        return
    with AIO_THREAD_LOCK:
        if AIO_THREAD and AIO_THREAD.is_alive():
            return
        AIO_THREAD = threading.Thread(
            target=_aio_loop_worker,
            daemon=True,
            name="aiograpi-event-loop",
        )
        AIO_THREAD.start()


def _run_async(coro, timeout=300):
    _ensure_aio_loop()
    future = asyncio.run_coroutine_threadsafe(coro, AIO_LOOP)
    return future.result(timeout=timeout)


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
        "welcome_message": get_setting(
            "welcome_message",
            "Olá, {first_name}! 👋 Obrigado por seguir @{account}. Seja muito bem-vindo(a)!",
        ),
    }


def save_config(message, enabled, poll_seconds, max_dms_per_hour, min_delay):
    set_setting("welcome_message", message.strip())
    set_setting("welcome_enabled", str(bool(enabled)).lower())
    set_setting("poll_seconds", max(60, int(poll_seconds)))
    set_setting("max_dms_per_hour", max(1, min(50, int(max_dms_per_hour))))
    set_setting("min_dm_delay_seconds", max(10, int(min_delay)))


async def _new_client(load_saved_session=True):
    cl = Client()
    cl.delay_range = [1, 3]
    if load_saved_session and os.path.exists(SESSION_FILE):
        try:
            cl.load_settings(SESSION_FILE)
        except Exception:
            pass
    return cl


async def _restore_client_async():
    global client
    if client is not None:
        return client

    username = get_setting("ig_username")
    enc = get_setting("ig_password_enc")
    if not username or not enc:
        return None

    cl = await _new_client(load_saved_session=True)
    password = decrypt(enc)

    try:
        # aiograpi reuses a valid loaded session automatically and refreshes it
        # when needed. Do not force a fresh login on every Railway restart.
        await cl.login(username, password)
        cl.dump_settings(SESSION_FILE)
        set_setting("ig_connected", "true")
        set_setting("last_error", "")
        client = cl
        return client
    except Exception as e:
        set_setting("ig_connected", "false")
        set_setting("last_error", f"Falha ao restaurar sessão: {type(e).__name__}: {e}")
        return None


def _load_client():
    with CLIENT_LOCK:
        return _run_async(_restore_client_async())


async def _login_async(username, password, verification_code=None):
    global client
    cl = await _new_client(load_saved_session=True)

    try:
        if verification_code:
            await cl.login(username, password, verification_code=verification_code)
        else:
            await cl.login(username, password)

        info = await cl.account_info()
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
        return False, (
            "O Instagram pediu uma verificação/challenge. Abra o Instagram oficial, "
            "confirme que foi você e tente novamente."
        )
    except BadPassword:
        return False, "Usuário ou senha inválidos."
    except Exception as e:
        set_setting("last_error", f"{type(e).__name__}: {e}")
        return False, f"Falha no login: {type(e).__name__}: {e}"


def login(username, password, verification_code=None):
    with CLIENT_LOCK:
        return _run_async(_login_async(username, password, verification_code))


def logout():
    global client
    with CLIENT_LOCK:
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
    return (
        template.replace("{first_name}", first)
        .replace("{username}", user.username or "")
        .replace("{account}", account)
    )


async def _sync_once_async(send_messages=True):
    cl = await _restore_client_async()
    if not cl:
        return {"ok": False, "message": "Instagram não conectado."}

    cfg = status()
    try:
        me = await cl.account_info()
        followers = await cl.user_followers(str(me.pk), amount=0)
        followers_by_id = {str(pk): user for pk, user in followers.items()}
        current_ids = set(followers_by_id.keys())
        known = {str(r["pk"]) for r in rows("SELECT pk FROM followers")}
        new_ids = current_ids - known
        baseline = len(known) == 0

        for pk, user in followers_by_id.items():
            if pk not in known:
                execute(
                    "INSERT INTO followers(pk,username,full_name,first_seen,welcomed,last_error) VALUES(?,?,?,?,?,?)",
                    (
                        pk,
                        user.username or "",
                        user.full_name or "",
                        utcnow(),
                        bool(baseline) if _is_postgres() else (1 if baseline else 0),
                        "baseline" if baseline else None,
                    ),
                )

        sent = 0
        errors = 0
        if send_messages and cfg["welcome_enabled"] and not baseline:
            pending_sql = (
                "SELECT pk,username,full_name FROM followers WHERE welcomed=FALSE ORDER BY first_seen ASC LIMIT 25"
                if _is_postgres()
                else "SELECT pk,username,full_name FROM followers WHERE welcomed=0 ORDER BY first_seen ASC LIMIT 25"
            )
            pending = rows(pending_sql)

            for row in pending:
                if _dm_count_last_hour() >= cfg["max_dms_per_hour"]:
                    break

                follower_pk = str(row["pk"])
                user = followers_by_id.get(follower_pk)
                if not user:
                    try:
                        user = await cl.user_info(follower_pk)
                    except Exception as e:
                        execute("UPDATE followers SET last_error=? WHERE pk=?", (str(e), follower_pk))
                        errors += 1
                        continue

                msg = _render_message(cfg["welcome_message"], user, me.username)
                try:
                    await cl.direct_send(msg, user_ids=[int(follower_pk)])
                    execute(
                        "UPDATE followers SET welcomed=?, welcomed_at=?, last_error=NULL WHERE pk=?",
                        ((True if _is_postgres() else 1), utcnow(), follower_pk),
                    )
                    execute(
                        "INSERT INTO dm_log(follower_pk,username,status,message,error,created_at) VALUES(?,?,?,?,?,?)",
                        (follower_pk, row["username"], "sent", msg, None, utcnow()),
                    )
                    sent += 1
                    await asyncio.sleep(cfg["min_dm_delay_seconds"] + random.randint(0, 12))
                except Exception as e:
                    error_text = f"{type(e).__name__}: {e}"
                    execute("UPDATE followers SET last_error=? WHERE pk=?", (error_text, follower_pk))
                    execute(
                        "INSERT INTO dm_log(follower_pk,username,status,message,error,created_at) VALUES(?,?,?,?,?,?)",
                        (follower_pk, row["username"], "error", msg, error_text, utcnow()),
                    )
                    errors += 1
                    if isinstance(e, LoginRequired):
                        set_setting("ig_connected", "false")
                        break

        set_setting("last_poll", utcnow())
        set_setting("last_error", "")
        return {
            "ok": True,
            "new": 0 if baseline else len(new_ids),
            "sent": sent,
            "errors": errors,
            "baseline": baseline,
            "total": len(followers_by_id),
        }
    except Exception as e:
        set_setting("last_poll", utcnow())
        set_setting("last_error", f"{type(e).__name__}: {e}")
        if isinstance(e, LoginRequired):
            set_setting("ig_connected", "false")
        return {"ok": False, "message": f"{type(e).__name__}: {e}"}


def sync_once(send_messages=True):
    with CLIENT_LOCK:
        return _run_async(_sync_once_async(send_messages=send_messages), timeout=900)


def worker_loop():
    while True:
        try:
            if status()["connected"]:
                sync_once(send_messages=True)
        except Exception as e:
            set_setting("last_error", f"Worker: {type(e).__name__}: {e}")
        # Worker itself is synchronous; Instagram I/O stays async in AIO_LOOP.
        threading.Event().wait(max(60, status()["poll_seconds"]))


def start_worker():
    global worker_started
    if worker_started:
        return
    worker_started = True
    _ensure_aio_loop()
    t = threading.Thread(target=worker_loop, daemon=True, name="instagram-worker")
    t.start()
