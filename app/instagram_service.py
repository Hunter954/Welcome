import asyncio
import json
import logging
import os
import random
import sys
import threading
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from aiograpi import Client
from aiograpi.exceptions import (
    BadPassword,
    ChallengeRequired,
    CheckpointRequired,
    ClientConnectionError,
    ClientLoginRequired,
    ClientThrottledError,
    ConsentRequired,
    LoginRequired,
    PleaseWaitFewMinutes,
    SentryBlock,
    TwoFactorRequired,
)

from .crypto import decrypt, encrypt
from .db import _is_postgres, execute, get_setting, rows, set_setting, utcnow

DATA_DIR = os.getenv("DATA_DIR", os.path.join(os.getcwd(), "data"))
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
SESSION_FILE = os.path.join(DATA_DIR, "instagram_session.json")

LOGGER = logging.getLogger("instagram_automation")
if not LOGGER.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOGGER.addHandler(handler)
LOGGER.setLevel(logging.INFO)
LOGGER.propagate = False

AIO_LOOP = asyncio.new_event_loop()
AIO_THREAD = None
AIO_THREAD_LOCK = threading.Lock()
CLIENT_LOCK = threading.RLock()
client = None
worker_started = False

SENSITIVE_WORDS = (
    "password", "passwd", "authorization", "cookie", "sessionid", "csrftoken",
    "token", "secret", "fernet", "phone_number", "email", "challenge_context",
)


def _redact(value, key=""):
    if any(word in str(key).lower() for word in SENSITIVE_WORDS):
        return "***REDACTED***"
    if isinstance(value, dict):
        return {str(k): _redact(v, str(k)) for k, v in list(value.items())[:80]}
    if isinstance(value, (list, tuple)):
        return [_redact(v) for v in list(value)[:40]]
    if isinstance(value, (str, int, float, bool)) or value is None:
        text = str(value) if not isinstance(value, (bool, int, float)) and value is not None else value
        if isinstance(text, str) and len(text) > 1200:
            return text[:1200] + "…"
        return text
    return str(value)[:1200]


def _safe_json(value):
    try:
        return json.dumps(_redact(value), ensure_ascii=False, default=str)[:12000]
    except Exception:
        return json.dumps({"unserializable": type(value).__name__})


def log_event(level, event, message="", details=None):
    level = (level or "INFO").upper()
    payload = _safe_json(details or {})
    line = f"IG_EVENT={event} MESSAGE={message} DETAILS={payload}"
    getattr(LOGGER, level.lower(), LOGGER.info)(line)
    try:
        execute(
            "INSERT INTO app_log(level,event,message,details,created_at) VALUES(?,?,?,?,?)",
            (level, event, str(message)[:2000], payload, utcnow()),
        )
    except Exception as db_error:
        LOGGER.error("IG_LOG_DB_ERROR=%s", type(db_error).__name__)


def _exception_details(exc, cl=None, attempt_id=None, username=None, session_loaded=None):
    details = {
        "attempt_id": attempt_id,
        "username": username,
        "exception_type": type(exc).__name__,
        "exception_args": list(getattr(exc, "args", ()) or ()),
        "session_file_exists": os.path.exists(SESSION_FILE),
        "session_loaded": session_loaded,
        "data_dir": DATA_DIR,
    }
    if cl is not None:
        for attr in ("last_json", "last_response", "challenge", "user_id", "device_id", "uuid"):
            try:
                val = getattr(cl, attr, None)
                if attr == "last_response" and val is not None:
                    val = {
                        "status_code": getattr(val, "status_code", None),
                        "url": str(getattr(val, "url", "")),
                        "text": getattr(val, "text", "")[:1200],
                    }
                if val not in (None, "", {}, []):
                    details[attr] = val
            except Exception:
                pass
    return _redact(details)


def _aio_loop_worker():
    asyncio.set_event_loop(AIO_LOOP)
    log_event("INFO", "async_loop_started", "Loop assíncrono do aiograpi iniciado")
    AIO_LOOP.run_forever()


def _ensure_aio_loop():
    global AIO_THREAD
    if AIO_THREAD and AIO_THREAD.is_alive():
        return
    with AIO_THREAD_LOCK:
        if AIO_THREAD and AIO_THREAD.is_alive():
            return
        AIO_THREAD = threading.Thread(target=_aio_loop_worker, daemon=True, name="aiograpi-event-loop")
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
        "welcome_message": get_setting("welcome_message", "Olá, {first_name}! 👋 Obrigado por seguir @{account}. Seja muito bem-vindo(a)!"),
    }


def save_config(message, enabled, poll_seconds, max_dms_per_hour, min_delay):
    set_setting("welcome_message", message.strip())
    set_setting("welcome_enabled", str(bool(enabled)).lower())
    set_setting("poll_seconds", max(60, int(poll_seconds)))
    set_setting("max_dms_per_hour", max(1, min(50, int(max_dms_per_hour))))
    set_setting("min_dm_delay_seconds", max(10, int(min_delay)))
    log_event("INFO", "config_saved", "Configurações da automação salvas")


async def _new_client(load_saved_session=True):
    cl = Client()
    cl.delay_range = [1, 3]
    session_loaded = False
    session_error = None
    if load_saved_session and os.path.exists(SESSION_FILE):
        try:
            cl.load_settings(SESSION_FILE)
            session_loaded = True
        except Exception as e:
            session_error = f"{type(e).__name__}: {e}"
    setattr(cl, "_automation_session_loaded", session_loaded)
    if session_error:
        log_event("WARNING", "session_load_failed", "Falha ao carregar sessão salva", {"error": session_error})
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
    attempt_id = uuid.uuid4().hex[:10]
    session_loaded = getattr(cl, "_automation_session_loaded", False)
    log_event("INFO", "session_restore_started", "Tentando restaurar sessão do Instagram", {
        "attempt_id": attempt_id, "username": username, "session_loaded": session_loaded,
    })
    try:
        await cl.login(username, password)
        info = await cl.account_info()
        cl.dump_settings(SESSION_FILE)
        set_setting("ig_connected", "true")
        set_setting("last_error", "")
        client = cl
        log_event("INFO", "session_restore_success", f"Sessão restaurada para @{info.username}", {"attempt_id": attempt_id})
        return client
    except Exception as e:
        err = f"Falha ao restaurar sessão: {type(e).__name__}: {e}"
        set_setting("ig_connected", "false")
        set_setting("last_error", err)
        log_event("ERROR", "session_restore_failed", err, _exception_details(e, cl, attempt_id, username, session_loaded))
        return None


def _load_client():
    with CLIENT_LOCK:
        return _run_async(_restore_client_async())


async def _login_async(username, password, verification_code=None):
    global client
    attempt_id = uuid.uuid4().hex[:10]
    cl = await _new_client(load_saved_session=True)
    session_loaded = getattr(cl, "_automation_session_loaded", False)
    log_event("INFO", "login_started", "Tentativa de login iniciada", {
        "attempt_id": attempt_id,
        "username": username,
        "has_2fa_code": bool(verification_code),
        "session_file_exists": os.path.exists(SESSION_FILE),
        "session_loaded": session_loaded,
        "library": "aiograpi",
    })
    try:
        if verification_code:
            await cl.login(username, password, verification_code=verification_code)
        else:
            await cl.login(username, password)
        log_event("INFO", "login_api_accepted", "Instagram aceitou a autenticação; validando conta", {"attempt_id": attempt_id})
        info = await cl.account_info()
        cl.dump_settings(SESSION_FILE)
        set_setting("ig_username", username)
        set_setting("ig_password_enc", encrypt(password))
        set_setting("ig_connected", "true")
        set_setting("last_error", "")
        client = cl
        log_event("INFO", "login_success", f"Conectado como @{info.username}", {
            "attempt_id": attempt_id, "instagram_user_id": str(getattr(info, "pk", "")),
        })
        return True, f"Conectado como @{info.username}"
    except TwoFactorRequired as e:
        log_event("WARNING", "login_2fa_required", "Instagram solicitou autenticação em dois fatores", _exception_details(e, cl, attempt_id, username, session_loaded))
        return False, "2FA_REQUIRED"
    except BadPassword as e:
        msg = "O Instagram respondeu 'bad_password'. Confira usuário/senha no app oficial. Veja o Diagnóstico no painel para o retorno técnico."
        set_setting("last_error", f"BadPassword: {e}")
        log_event("ERROR", "login_bad_password", msg, _exception_details(e, cl, attempt_id, username, session_loaded))
        return False, msg
    except ChallengeRequired as e:
        msg = "O Instagram pediu challenge/verificação. Confirme o acesso no app oficial e tente novamente."
        set_setting("last_error", f"ChallengeRequired: {e}")
        log_event("WARNING", "login_challenge_required", msg, _exception_details(e, cl, attempt_id, username, session_loaded))
        return False, msg
    except CheckpointRequired as e:
        msg = "O Instagram bloqueou o login em um checkpoint. Abra o app oficial e aprove o novo acesso."
        set_setting("last_error", f"CheckpointRequired: {e}")
        log_event("WARNING", "login_checkpoint_required", msg, _exception_details(e, cl, attempt_id, username, session_loaded))
        return False, msg
    except ConsentRequired as e:
        msg = "O Instagram exige uma confirmação/consentimento na conta. Abra o app oficial e conclua essa etapa."
        set_setting("last_error", f"ConsentRequired: {e}")
        log_event("WARNING", "login_consent_required", msg, _exception_details(e, cl, attempt_id, username, session_loaded))
        return False, msg
    except SentryBlock as e:
        msg = "O Instagram bloqueou o IP/origem desta tentativa (SentryBlock). O IP do Railway pode estar sendo recusado."
        set_setting("last_error", f"SentryBlock: {e}")
        log_event("ERROR", "login_ip_blocked", msg, _exception_details(e, cl, attempt_id, username, session_loaded))
        return False, msg
    except PleaseWaitFewMinutes as e:
        msg = "O Instagram pediu para aguardar antes de tentar novamente. Evite novas tentativas por enquanto."
        set_setting("last_error", f"PleaseWaitFewMinutes: {e}")
        log_event("WARNING", "login_wait_required", msg, _exception_details(e, cl, attempt_id, username, session_loaded))
        return False, msg
    except ClientThrottledError as e:
        msg = "O Instagram limitou temporariamente as requisições (HTTP 429)."
        set_setting("last_error", f"ClientThrottledError: {e}")
        log_event("WARNING", "login_throttled", msg, _exception_details(e, cl, attempt_id, username, session_loaded))
        return False, msg
    except (ClientConnectionError, ClientLoginRequired, LoginRequired) as e:
        msg = f"Falha de sessão/conexão no login: {type(e).__name__}. Veja o Diagnóstico no painel."
        set_setting("last_error", f"{type(e).__name__}: {e}")
        log_event("ERROR", "login_session_or_connection_error", msg, _exception_details(e, cl, attempt_id, username, session_loaded))
        return False, msg
    except Exception as e:
        msg = f"Falha no login: {type(e).__name__}: {e}"
        set_setting("last_error", msg)
        details = _exception_details(e, cl, attempt_id, username, session_loaded)
        details["traceback"] = traceback.format_exc(limit=8)
        log_event("ERROR", "login_unknown_error", msg, details)
        return False, msg


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
            removed = True
        except FileNotFoundError:
            removed = False
        log_event("INFO", "instagram_logout", "Conta desconectada do painel", {"session_file_removed": removed})


def clear_saved_session():
    global client
    with CLIENT_LOCK:
        client = None
        set_setting("ig_connected", "false")
        try:
            os.remove(SESSION_FILE)
            removed = True
        except FileNotFoundError:
            removed = False
        log_event("WARNING", "saved_session_cleared", "Sessão local do Instagram foi limpa manualmente", {"session_file_removed": removed})
        return removed


def _dm_count_last_hour():
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    r = rows("SELECT COUNT(*) AS n FROM dm_log WHERE status='sent' AND created_at >= ?", (cutoff,))
    return int(r[0]["n"]) if r else 0


def _render_message(template, user, account):
    first = (user.full_name or user.username or "").strip().split(" ")[0]
    return template.replace("{first_name}", first).replace("{username}", user.username or "").replace("{account}", account)


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
                    (pk, user.username or "", user.full_name or "", utcnow(), bool(baseline) if _is_postgres() else (1 if baseline else 0), "baseline" if baseline else None),
                )
        sent = 0
        errors = 0
        if send_messages and cfg["welcome_enabled"] and not baseline:
            pending_sql = "SELECT pk,username,full_name FROM followers WHERE welcomed=FALSE ORDER BY first_seen ASC LIMIT 25" if _is_postgres() else "SELECT pk,username,full_name FROM followers WHERE welcomed=0 ORDER BY first_seen ASC LIMIT 25"
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
                    execute("UPDATE followers SET welcomed=?, welcomed_at=?, last_error=NULL WHERE pk=?", ((True if _is_postgres() else 1), utcnow(), follower_pk))
                    execute("INSERT INTO dm_log(follower_pk,username,status,message,error,created_at) VALUES(?,?,?,?,?,?)", (follower_pk, row["username"], "sent", msg, None, utcnow()))
                    sent += 1
                    await asyncio.sleep(cfg["min_dm_delay_seconds"] + random.randint(0, 12))
                except Exception as e:
                    error_text = f"{type(e).__name__}: {e}"
                    execute("UPDATE followers SET last_error=? WHERE pk=?", (error_text, follower_pk))
                    execute("INSERT INTO dm_log(follower_pk,username,status,message,error,created_at) VALUES(?,?,?,?,?,?)", (follower_pk, row["username"], "error", msg, error_text, utcnow()))
                    log_event("ERROR", "dm_send_failed", error_text, {"username": row["username"], "exception_type": type(e).__name__})
                    errors += 1
                    if isinstance(e, LoginRequired):
                        set_setting("ig_connected", "false")
                        break
        set_setting("last_poll", utcnow())
        set_setting("last_error", "")
        log_event("INFO", "sync_completed", "Sincronização concluída", {"new": 0 if baseline else len(new_ids), "sent": sent, "errors": errors, "baseline": baseline, "total": len(followers_by_id)})
        return {"ok": True, "new": 0 if baseline else len(new_ids), "sent": sent, "errors": errors, "baseline": baseline, "total": len(followers_by_id)}
    except Exception as e:
        set_setting("last_poll", utcnow())
        set_setting("last_error", f"{type(e).__name__}: {e}")
        if isinstance(e, LoginRequired):
            set_setting("ig_connected", "false")
        log_event("ERROR", "sync_failed", f"{type(e).__name__}: {e}", _exception_details(e, cl))
        return {"ok": False, "message": f"{type(e).__name__}: {e}"}


def sync_once(send_messages=True):
    with CLIENT_LOCK:
        return _run_async(_sync_once_async(send_messages=send_messages), timeout=900)


def worker_loop():
    log_event("INFO", "worker_started", "Worker de sincronização iniciado")
    while True:
        try:
            if status()["connected"]:
                sync_once(send_messages=True)
        except Exception as e:
            set_setting("last_error", f"Worker: {type(e).__name__}: {e}")
            log_event("ERROR", "worker_error", f"{type(e).__name__}: {e}")
        import time
        time.sleep(max(60, status()["poll_seconds"]))


def start_worker():
    global worker_started
    if worker_started:
        return
    worker_started = True
    t = threading.Thread(target=worker_loop, daemon=True, name="instagram-worker")
    t.start()
