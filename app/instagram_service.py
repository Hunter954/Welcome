import json
import logging
import os
import re
import shutil
import sys
import threading
import time
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from instagrapi import Client
from instagrapi.exceptions import (
    BadPassword,
    ChallengeRequired,
    ClientConnectionError,
    ClientLoginRequired,
    ClientThrottledError,
    LoginRequired,
    PleaseWaitFewMinutes,
    SentryBlock,
    TwoFactorRequired,
)

from .crypto import encrypt, decrypt
from .db import _is_postgres, execute, get_setting, rows, set_setting, utcnow

DATA_DIR = os.getenv("DATA_DIR", os.path.join(os.getcwd(), "data"))
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
SESSION_FILE = os.path.join(DATA_DIR, "instagram_session.json")
SESSION_BACKUP_FILE = os.path.join(DATA_DIR, "instagram_session.backup.json")

# Intervalo deliberadamente conservador para o detector. O disparo, quando há
# alguém na fila, continua imediato. Pode ser alterado no futuro depois do teste
# de isolamento, mas não fica exposto no painel para evitar polling agressivo.
DETECTOR_POLL_SECONDS = 60
LATEST_FOLLOWERS_AMOUNT = 25
APP_VERSION = "2026.08.14-instagrapi-clean-2"
BOOT_ID = uuid.uuid4().hex[:8]

LOGGER = logging.getLogger("instagram_automation")
if not LOGGER.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOGGER.addHandler(handler)
LOGGER.setLevel(logging.INFO)
LOGGER.propagate = False

CLIENT_LOCK = threading.RLock()
CLIENT = None
WORKER_STARTED = False
COOLDOWN_UNTIL = None
RATE_LIMIT_HITS = 0
STATE_LOCK = threading.RLock()

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
        if isinstance(value, str) and len(value) > 1200:
            return value[:1200] + "…"
        return value
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


def _exception_details(exc, cl=None, operation=None):
    details = {
        "operation": operation,
        "exception_type": type(exc).__name__,
        "exception_args": list(getattr(exc, "args", ()) or ()),
        "session_file_exists": os.path.exists(SESSION_FILE),
        "auth_mode": get_setting("ig_auth_mode", ""),
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
        "detector_enabled": _bool(get_setting("detector_enabled", "true"), True),
        "sender_enabled": _bool(get_setting("sender_enabled", "true"), True),
        "poll_seconds": DETECTOR_POLL_SECONDS,
        "welcome_message": get_setting("welcome_message", "Olá, {first_name}! 👋 Obrigado por seguir @{account}. Seja muito bem-vindo(a)!"),
        "excluded_usernames": get_setting("excluded_usernames", ""),
        "session_saved": os.path.exists(SESSION_FILE),
        "auth_mode": get_setting("ig_auth_mode", "") or "sessionid",
        "library": "instagrapi",
        "app_version": APP_VERSION,
        "boot_id": BOOT_ID,
        "cooldown_until": get_setting("detector_cooldown_until", ""),
        "last_operation": get_setting("last_ig_operation", ""),
    }


def save_config(message, enabled, excluded_usernames="", detector_enabled=True, sender_enabled=True):
    set_setting("welcome_message", (message or "").strip())
    set_setting("welcome_enabled", str(bool(enabled)).lower())
    set_setting("excluded_usernames", (excluded_usernames or "").strip())
    set_setting("detector_enabled", str(bool(detector_enabled)).lower())
    set_setting("sender_enabled", str(bool(sender_enabled)).lower())
    log_event("INFO", "config_saved", "Configurações salvas", {
        "welcome_enabled": bool(enabled),
        "detector_enabled": bool(detector_enabled),
        "sender_enabled": bool(sender_enabled),
        "poll_seconds": DETECTOR_POLL_SECONDS,
        "library": "instagrapi",
    })


def _safe_dump_settings(cl):
    tmp = SESSION_FILE + ".tmp"
    try:
        cl.dump_settings(tmp)
        if os.path.exists(SESSION_FILE):
            try:
                shutil.copy2(SESSION_FILE, SESSION_BACKUP_FILE)
            except Exception:
                pass
        os.replace(tmp, SESSION_FILE)
        log_event("INFO", "session_saved", "Sessão do instagrapi persistida", {"path": "instagram_session.json"})
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def _new_client(load_saved=True):
    cl = Client()
    # A biblioteca pode aplicar pequenos delays internos em algumas rotas; não
    # usamos delay artificial antes da primeira DM.
    cl.delay_range = [1, 2]
    if load_saved and os.path.exists(SESSION_FILE):
        try:
            cl.load_settings(SESSION_FILE)
            log_event("INFO", "session_loaded", "Sessão persistente carregada pelo instagrapi")
        except Exception as e:
            log_event("WARNING", "session_load_failed", f"{type(e).__name__}: {e}")
            if os.path.exists(SESSION_BACKUP_FILE):
                try:
                    cl.load_settings(SESSION_BACKUP_FILE)
                    log_event("WARNING", "session_backup_loaded", "Backup da sessão carregado")
                except Exception as backup_error:
                    log_event("ERROR", "session_backup_failed", f"{type(backup_error).__name__}: {backup_error}")
    return cl


def _get_client():
    global CLIENT
    with CLIENT_LOCK:
        if CLIENT is not None:
            return CLIENT
        if not _bool(get_setting("ig_connected", "false")):
            return None
        if not os.path.exists(SESSION_FILE):
            set_setting("ig_connected", "false")
            set_setting("last_error", "Arquivo de sessão não encontrado. Importe novamente o sessionid.")
            return None
        cl = _new_client(load_saved=True)
        try:
            # Não chama login() aqui. Apenas valida os settings já persistidos.
            info = cl.account_info()
            set_setting("ig_username", getattr(info, "username", "") or get_setting("ig_username", ""))
            set_setting("ig_user_id", str(getattr(info, "pk", "")))
            set_setting("last_error", "")
            CLIENT = cl
            log_event("INFO", "session_restore_success", f"Sessão restaurada para @{get_setting('ig_username', '')}")
            return CLIENT
        except Exception as e:
            set_setting("ig_connected", "false")
            set_setting("last_error", f"Sessão inválida: {type(e).__name__}: {e}")
            log_event("ERROR", "session_restore_failed", f"{type(e).__name__}: {e}", _exception_details(e, cl, "restore"))
            return None


def _extract_sessionid(raw):
    raw = (raw or "").strip()
    if not raw:
        return ""

    # JSON de extensões/exportadores de cookies.
    try:
        data = json.loads(raw)
        items = data if isinstance(data, list) else data.get("cookies", data) if isinstance(data, dict) else []
        if isinstance(items, dict):
            if items.get("sessionid"):
                return str(items["sessionid"]).strip()
            items = [items]
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict) and str(item.get("name", "")).lower() == "sessionid":
                    return str(item.get("value", "")).strip()
    except Exception:
        pass

    # Cookie header: sessionid=...; csrftoken=...
    match = re.search(r"(?:^|[;\s])sessionid\s*=\s*([^;\s]+)", raw, flags=re.I)
    if match:
        return match.group(1).strip()

    # Se colou somente o valor, aceita diretamente.
    if "\n" not in raw and len(raw) >= 10 and " " not in raw:
        return raw
    return ""


def import_browser_session(raw):
    global CLIENT
    sessionid = _extract_sessionid(raw)
    if not sessionid:
        return False, "Não encontrei um sessionid válido no conteúdo informado."

    attempt_id = uuid.uuid4().hex[:10]
    cl = _new_client(load_saved=False)
    log_event("INFO", "sessionid_import_started", "Importação de sessionid iniciada", {
        "attempt_id": attempt_id,
        "library": "instagrapi",
    })
    try:
        cl.login_by_sessionid(sessionid)
        info = cl.account_info()
        _safe_dump_settings(cl)
        set_setting("ig_username", getattr(info, "username", ""))
        set_setting("ig_user_id", str(getattr(info, "pk", "")))
        set_setting("ig_sessionid_enc", encrypt(sessionid))
        set_setting("ig_password_enc", "")
        set_setting("ig_auth_mode", "sessionid")
        set_setting("ig_connected", "true")
        # Modo diagnóstico: uma sessão recém-importada não inicia leitura nem
        # disparo automaticamente. Isso permite verificar se o simples bootstrap
        # pelo sessionid já invalida a sessão antes de culpar detector/DM.
        set_setting("detector_enabled", "false")
        set_setting("sender_enabled", "false")
        set_setting("welcome_enabled", "false")
        set_setting("last_error", "")
        set_setting("last_ig_operation", "sessionid_import")
        CLIENT = cl
        log_event("INFO", "sessionid_import_success", f"Sessão importada para @{info.username}", {
            "attempt_id": attempt_id,
            "instagram_user_id": str(getattr(info, "pk", "")),
        })
        return True, f"Conectado como @{info.username} usando sessionid."
    except Exception as e:
        set_setting("ig_connected", "false")
        msg = f"Falha ao importar sessionid: {type(e).__name__}: {e}"
        set_setting("last_error", msg)
        details = _exception_details(e, cl, "sessionid_import")
        details["traceback"] = traceback.format_exc(limit=6)
        log_event("ERROR", "sessionid_import_failed", msg, details)
        return False, msg


def login(username, password, verification_code=None):
    """Login por senha mantido como alternativa; o fluxo principal é sessionid."""
    global CLIENT
    cl = _new_client(load_saved=True)
    attempt_id = uuid.uuid4().hex[:10]
    log_event("INFO", "password_login_started", "Login por senha iniciado", {"attempt_id": attempt_id, "username": username})
    try:
        cl.login(username, password, verification_code=verification_code)
        info = cl.account_info()
        _safe_dump_settings(cl)
        set_setting("ig_username", username)
        set_setting("ig_user_id", str(getattr(info, "pk", "")))
        set_setting("ig_password_enc", encrypt(password))
        set_setting("ig_auth_mode", "password")
        set_setting("ig_connected", "true")
        set_setting("last_error", "")
        CLIENT = cl
        return True, f"Conectado como @{info.username}"
    except TwoFactorRequired:
        return False, "2FA_REQUIRED"
    except (BadPassword, ChallengeRequired, SentryBlock,
            PleaseWaitFewMinutes, ClientThrottledError, ClientConnectionError, ClientLoginRequired, LoginRequired) as e:
        msg = f"{type(e).__name__}: {e}"
        set_setting("last_error", msg)
        log_event("ERROR", "password_login_failed", msg, _exception_details(e, cl, "password_login"))
        return False, msg
    except Exception as e:
        msg = f"Falha no login: {type(e).__name__}: {e}"
        set_setting("last_error", msg)
        log_event("ERROR", "password_login_failed", msg, _exception_details(e, cl, "password_login"))
        return False, msg


def logout():
    global CLIENT
    with CLIENT_LOCK:
        CLIENT = None
        set_setting("ig_connected", "false")
        set_setting("ig_password_enc", "")
        set_setting("ig_sessionid_enc", "")
        set_setting("ig_auth_mode", "")
        removed = False
        for path in (SESSION_FILE, SESSION_BACKUP_FILE):
            try:
                os.remove(path)
                removed = True
            except FileNotFoundError:
                pass
        log_event("INFO", "instagram_logout", "Conta desconectada", {"session_file_removed": removed})


def clear_saved_session():
    global CLIENT
    with CLIENT_LOCK:
        CLIENT = None
        set_setting("ig_connected", "false")
        removed = False
        for path in (SESSION_FILE, SESSION_BACKUP_FILE):
            try:
                os.remove(path)
                removed = True
            except FileNotFoundError:
                pass
        log_event("WARNING", "saved_session_cleared", "Sessão local apagada manualmente", {"removed": removed})
        return removed


def _excluded_set(raw):
    return {p.strip().lstrip("@").lower() for p in re.split(r"[,;\n\r\t ]+", raw or "") if p.strip()}


def _render_message(template, user, account):
    first = (getattr(user, "full_name", "") or getattr(user, "username", "") or "").strip().split(" ")[0]
    return (template or "").replace("{first_name}", first).replace("{username}", getattr(user, "username", "") or "").replace("{account}", account or "")


def _upsert_detected_followers(followers, baseline=False):
    known = {str(r["pk"]) for r in rows("SELECT pk FROM followers")}
    new_count = 0
    for pk, user in followers.items():
        pk = str(pk)
        if pk in known:
            continue
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
        if not baseline:
            new_count += 1
            log_event("INFO", "new_follower_detected", f"Novo seguidor detectado: @{user.username}", {"pk": pk})
    return new_count


def _set_operation(name):
    set_setting("last_ig_operation", name)
    set_setting("last_ig_operation_at", utcnow())


def detect_followers(force_full=False):
    global COOLDOWN_UNTIL, RATE_LIMIT_HITS, CLIENT
    cfg = status()
    if not cfg["detector_enabled"]:
        return {"ok": True, "new": 0, "reason": "detector_disabled"}

    with STATE_LOCK:
        if COOLDOWN_UNTIL and datetime.now(timezone.utc) < COOLDOWN_UNTIL:
            remaining = int((COOLDOWN_UNTIL - datetime.now(timezone.utc)).total_seconds())
            return {"ok": True, "new": 0, "reason": "cooldown", "remaining": max(1, remaining)}

    cl = _get_client()
    if cl is None:
        return {"ok": False, "message": "Sessão do Instagram não está conectada."}

    try:
        me_id = get_setting("ig_user_id", "")
        if not me_id:
            info = cl.account_info()
            me_id = str(info.pk)
            set_setting("ig_user_id", me_id)

        known_count = int(rows("SELECT COUNT(*) AS n FROM followers")[0]["n"])
        baseline = known_count == 0
        # Nunca baixa a lista inteira automaticamente. A primeira execução usa
        # apenas os mais recentes como baseline; isso evita uma requisição grande
        # logo depois de importar a sessão.
        amount = LATEST_FOLLOWERS_AMOUNT
        if force_full:
            amount = 0

        op_id = uuid.uuid4().hex[:8]
        _set_operation("detector")
        log_event("INFO", "detector_request_start", "Consulta de seguidores iniciada", {
            "op_id": op_id,
            "amount": amount,
            "baseline": baseline,
            "poll_seconds": DETECTOR_POLL_SECONDS,
        })
        started = time.monotonic()
        # Serializamos requests privadas no mesmo Client. Isso evita detector e
        # sender fazendo chamadas simultâneas com a mesma sessão.
        with CLIENT_LOCK:
            followers = cl.user_followers(
                str(me_id),
                amount=amount,
                order="date_followed_latest",
                use_cache=False,
            )
        elapsed = time.monotonic() - started
        new_count = _upsert_detected_followers(followers, baseline=baseline)
        set_setting("last_poll", utcnow())
        set_setting("last_error", "")
        with STATE_LOCK:
            RATE_LIMIT_HITS = 0
            COOLDOWN_UNTIL = None
        set_setting("detector_cooldown_until", "")
        log_event("INFO", "detector_request_success", "Consulta de seguidores concluída", {
            "op_id": op_id,
            "returned": len(followers),
            "new": 0 if baseline else new_count,
            "seconds": round(elapsed, 3),
        })
        return {"ok": True, "new": 0 if baseline else new_count, "baseline": baseline, "total": len(followers)}
    except (PleaseWaitFewMinutes, ClientThrottledError) as e:
        with STATE_LOCK:
            RATE_LIMIT_HITS += 1
            minutes = min(60, 10 * (2 ** max(0, RATE_LIMIT_HITS - 1)))
            COOLDOWN_UNTIL = datetime.now(timezone.utc) + timedelta(minutes=minutes)
        set_setting("detector_cooldown_until", COOLDOWN_UNTIL.isoformat())
        set_setting("last_error", f"Detector em cooldown por {minutes} min: {type(e).__name__}")
        log_event("WARNING", "detector_rate_limited", "Detector recebeu rate limit", {
            "cooldown_minutes": minutes,
            **_exception_details(e, cl, "detector"),
        })
        return {"ok": False, "message": f"{type(e).__name__}: cooldown {minutes} min"}
    except (ClientLoginRequired, LoginRequired) as e:
        CLIENT = None
        set_setting("ig_connected", "false")
        set_setting("last_error", f"Sessão invalidada durante DETECTOR: {type(e).__name__}: {e}")
        log_event("ERROR", "session_invalidated_by_detector", "A sessão parou de funcionar durante a consulta de seguidores", _exception_details(e, cl, "detector"))
        return {"ok": False, "message": f"{type(e).__name__}: {e}", "reauth_required": True, "culprit": "detector"}
    except Exception as e:
        set_setting("last_error", f"Detector: {type(e).__name__}: {e}")
        log_event("ERROR", "detector_request_failed", f"{type(e).__name__}: {e}", _exception_details(e, cl, "detector"))
        return {"ok": False, "message": f"{type(e).__name__}: {e}"}


def send_one_pending():
    global CLIENT
    cfg = status()
    if not cfg["welcome_enabled"] or not cfg["sender_enabled"]:
        return {"ok": True, "sent": 0, "reason": "sender_disabled"}

    pending_sql = "SELECT pk,username,full_name FROM followers WHERE welcomed=FALSE ORDER BY first_seen ASC LIMIT 1" if _is_postgres() else "SELECT pk,username,full_name FROM followers WHERE welcomed=0 ORDER BY first_seen ASC LIMIT 1"
    pending = rows(pending_sql)
    if not pending:
        return {"ok": True, "sent": 0, "reason": "empty"}
    row = pending[0]
    username_norm = (row.get("username") or "").lower().lstrip("@")
    if username_norm in _excluded_set(cfg.get("excluded_usernames", "")):
        execute("UPDATE followers SET welcomed=?, welcomed_at=?, last_error=? WHERE pk=?", ((True if _is_postgres() else 1), utcnow(), "excluded_by_rule", str(row["pk"])))
        return {"ok": True, "sent": 0, "reason": "excluded"}

    cl = _get_client()
    if cl is None:
        return {"ok": False, "sent": 0, "message": "Sessão do Instagram não está conectada."}

    from types import SimpleNamespace
    user = SimpleNamespace(username=row.get("username") or "", full_name=row.get("full_name") or "")
    msg = _render_message(cfg["welcome_message"], user, get_setting("ig_username", ""))
    follower_pk = str(row["pk"])
    op_id = uuid.uuid4().hex[:8]
    _set_operation("sender")
    log_event("INFO", "dm_request_start", f"Disparo iniciado para @{row['username']}", {"op_id": op_id})
    started = time.monotonic()
    try:
        with CLIENT_LOCK:
            cl.direct_send(msg, user_ids=[int(follower_pk)])
        elapsed = time.monotonic() - started
        execute("UPDATE followers SET welcomed=?, welcomed_at=?, last_error=NULL WHERE pk=?", ((True if _is_postgres() else 1), utcnow(), follower_pk))
        execute("INSERT INTO dm_log(follower_pk,username,status,message,error,created_at) VALUES(?,?,?,?,?,?)", (follower_pk, row["username"], "sent", msg, None, utcnow()))
        log_event("INFO", "dm_request_success", f"Boas-vindas enviada para @{row['username']}", {"op_id": op_id, "seconds": round(elapsed, 3)})
        return {"ok": True, "sent": 1, "seconds": elapsed}
    except (ClientLoginRequired, LoginRequired) as e:
        CLIENT = None
        set_setting("ig_connected", "false")
        error_text = f"Sessão invalidada durante DISPARO: {type(e).__name__}: {e}"
        set_setting("last_error", error_text)
        execute("UPDATE followers SET last_error=? WHERE pk=?", (error_text, follower_pk))
        execute("INSERT INTO dm_log(follower_pk,username,status,message,error,created_at) VALUES(?,?,?,?,?,?)", (follower_pk, row["username"], "error", msg, error_text, utcnow()))
        log_event("ERROR", "session_invalidated_by_sender", "A sessão parou de funcionar durante o envio da DM", _exception_details(e, cl, "sender"))
        return {"ok": False, "sent": 0, "message": error_text, "culprit": "sender"}
    except (PleaseWaitFewMinutes, ClientThrottledError) as e:
        error_text = f"Disparo limitado: {type(e).__name__}: {e}"
        execute("UPDATE followers SET last_error=? WHERE pk=?", (error_text, follower_pk))
        execute("INSERT INTO dm_log(follower_pk,username,status,message,error,created_at) VALUES(?,?,?,?,?,?)", (follower_pk, row["username"], "error", msg, error_text, utcnow()))
        log_event("WARNING", "sender_rate_limited", error_text, _exception_details(e, cl, "sender"))
        return {"ok": False, "sent": 0, "message": error_text}
    except Exception as e:
        error_text = f"{type(e).__name__}: {e}"
        execute("UPDATE followers SET last_error=? WHERE pk=?", (error_text, follower_pk))
        execute("INSERT INTO dm_log(follower_pk,username,status,message,error,created_at) VALUES(?,?,?,?,?,?)", (follower_pk, row["username"], "error", msg, error_text, utcnow()))
        log_event("ERROR", "dm_request_failed", error_text, _exception_details(e, cl, "sender"))
        return {"ok": False, "sent": 0, "message": error_text}


def sync_once(send_messages=True):
    detected = detect_followers()
    if not detected.get("ok"):
        return detected
    sent = 0
    errors = 0
    if send_messages and not detected.get("baseline") and status()["sender_enabled"]:
        for _ in range(5):
            result = send_one_pending()
            if not result.get("ok"):
                errors += 1
                break
            if not result.get("sent"):
                break
            sent += 1
            time.sleep(1)
    return {**detected, "sent": sent, "errors": errors}


def _test_dm_background(username, custom_message=None):
    global CLIENT
    username = (username or "").strip().lstrip("@")
    try:
        cl = _get_client()
        if cl is None:
            raise LoginRequired("Sessão não conectada")
        _set_operation("test_dm")
        log_event("INFO", "test_dm_request_start", f"Teste iniciado para @{username}")
        with CLIENT_LOCK:
            pk = cl.user_id_from_username(username)
            user = cl.user_info(pk)
            msg = _render_message((custom_message or status()["welcome_message"]).strip(), user, get_setting("ig_username", ""))
            started = time.monotonic()
            cl.direct_send(msg, user_ids=[int(pk)])
        elapsed = time.monotonic() - started
        execute("INSERT INTO dm_log(follower_pk,username,status,message,error,created_at) VALUES(?,?,?,?,?,?)", (str(pk), username, "test", msg, None, utcnow()))
        log_event("INFO", "test_dm_request_success", f"DM de teste enviada para @{username}", {"seconds": round(elapsed, 3)})
    except (ClientLoginRequired, LoginRequired) as e:
        CLIENT = None
        set_setting("ig_connected", "false")
        set_setting("last_error", f"Sessão invalidada durante TESTE DE DM: {type(e).__name__}: {e}")
        log_event("ERROR", "session_invalidated_by_test_dm", "A sessão parou de funcionar durante o teste de DM", _exception_details(e, None, "test_dm"))
    except Exception as e:
        log_event("ERROR", "test_dm_request_failed", f"{type(e).__name__}: {e}", {"username": username})


def send_test_dm(username, custom_message=None):
    username = (username or "").strip().lstrip("@")
    if not username:
        return False, "Informe um @usuário para o teste."
    threading.Thread(target=_test_dm_background, args=(username, custom_message), daemon=True, name="instagram-test-dm").start()
    return True, f"Teste para @{username} colocado na fila."


def mark_pending_as_baseline():
    pending_sql = "SELECT COUNT(*) AS n FROM followers WHERE welcomed=FALSE" if _is_postgres() else "SELECT COUNT(*) AS n FROM followers WHERE welcomed=0"
    count = int(rows(pending_sql)[0]["n"])
    if _is_postgres():
        execute("UPDATE followers SET welcomed=TRUE, welcomed_at=?, last_error=? WHERE welcomed=FALSE", (utcnow(), "manual_baseline"))
    else:
        execute("UPDATE followers SET welcomed=1, welcomed_at=?, last_error=? WHERE welcomed=0", (utcnow(), "manual_baseline"))
    log_event("WARNING", "pending_marked_baseline", f"{count} pendentes foram marcados como base")
    return count


def detector_loop():
    log_event("INFO", "detector_started", "Detector iniciado", {
        "library": "instagrapi",
        "app_version": APP_VERSION,
        "boot_id": BOOT_ID,
        "poll_seconds": DETECTOR_POLL_SECONDS,
        "latest_amount": LATEST_FOLLOWERS_AMOUNT,
    })
    while True:
        try:
            cfg = status()
            if cfg["connected"] and cfg["detector_enabled"]:
                detect_followers()
        except Exception as e:
            log_event("ERROR", "detector_loop_error", f"{type(e).__name__}: {e}")
        time.sleep(DETECTOR_POLL_SECONDS)


def sender_loop():
    log_event("INFO", "sender_started", "Remetente iniciado", {"library": "instagrapi", "app_version": APP_VERSION, "boot_id": BOOT_ID})
    while True:
        try:
            cfg = status()
            if cfg["connected"] and cfg["welcome_enabled"] and cfg["sender_enabled"]:
                result = send_one_pending()
                if result.get("sent"):
                    time.sleep(1)
                    continue
        except Exception as e:
            log_event("ERROR", "sender_loop_error", f"{type(e).__name__}: {e}")
        time.sleep(1)


def start_worker():
    global WORKER_STARTED
    if WORKER_STARTED:
        return
    WORKER_STARTED = True
    threading.Thread(target=detector_loop, daemon=True, name="instagram-detector").start()
    threading.Thread(target=sender_loop, daemon=True, name="instagram-sender").start()
