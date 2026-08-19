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
APP_VERSION = "2026.08.19-command-center-mvp"
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

# --- Command Center extensions -------------------------------------------------
def _model(obj):
    if obj is None: return {}
    if isinstance(obj, dict): return obj
    if hasattr(obj, 'model_dump'):
        try: return obj.model_dump(mode='json')
        except Exception: pass
    if hasattr(obj, 'dict'):
        try: return obj.dict()
        except Exception: pass
    try: return vars(obj)
    except Exception: return {'value': str(obj)}

def sync_inbox(amount=30):
    cl=_get_client()
    if cl is None: return False, 'Conecte o Instagram primeiro.'
    try:
        _set_operation('inbox_sync')
        with CLIENT_LOCK: threads=cl.direct_threads(amount=amount)
        total=0
        for t in threads or []:
            d=_model(t); users=d.get('users') or []
            other=users[0] if users else {}; other=_model(other)
            items=d.get('messages') or d.get('items') or []
            latest=items[0] if items else None; lm=_model(latest)
            thread_id=str(d.get('id') or d.get('thread_id') or '')
            if not thread_id: continue
            username=other.get('username') or d.get('thread_title') or 'Instagram'
            title=d.get('thread_title') or other.get('full_name') or username
            text=lm.get('text') or lm.get('link_text') or lm.get('item_type') or ''
            at=str(lm.get('timestamp') or lm.get('created_at') or '')
            unread=1 if d.get('read_state') in (0,'0',False) else 0
            raw=_safe_json(d)
            if _is_postgres():
                execute('INSERT INTO inbox_threads(thread_id,title,username,user_pk,avatar_url,last_message,last_message_at,unread,raw_json) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(thread_id) DO UPDATE SET title=EXCLUDED.title,username=EXCLUDED.username,user_pk=EXCLUDED.user_pk,avatar_url=EXCLUDED.avatar_url,last_message=EXCLUDED.last_message,last_message_at=EXCLUDED.last_message_at,unread=EXCLUDED.unread,raw_json=EXCLUDED.raw_json',(thread_id,title,username,str(other.get('pk') or ''),other.get('profile_pic_url') or '',text,at,unread,raw))
            else:
                execute('INSERT INTO inbox_threads(thread_id,title,username,user_pk,avatar_url,last_message,last_message_at,unread,raw_json) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(thread_id) DO UPDATE SET title=excluded.title,username=excluded.username,user_pk=excluded.user_pk,avatar_url=excluded.avatar_url,last_message=excluded.last_message,last_message_at=excluded.last_message_at,unread=excluded.unread,raw_json=excluded.raw_json',(thread_id,title,username,str(other.get('pk') or ''),other.get('profile_pic_url') or '',text,at,unread,raw))
            if other.get('pk'):
                if _is_postgres(): execute("INSERT INTO contacts(pk,username,full_name,avatar_url,source,first_contact,last_interaction) VALUES(?,?,?,?,?,?,?) ON CONFLICT(pk) DO UPDATE SET username=EXCLUDED.username,full_name=EXCLUDED.full_name,avatar_url=EXCLUDED.avatar_url,last_interaction=EXCLUDED.last_interaction",(str(other.get('pk')),username,other.get('full_name') or '',other.get('profile_pic_url') or '','Instagram Direct',utcnow(),utcnow()))
                else: execute("INSERT INTO contacts(pk,username,full_name,avatar_url,source,first_contact,last_interaction) VALUES(?,?,?,?,?,?,?) ON CONFLICT(pk) DO UPDATE SET username=excluded.username,full_name=excluded.full_name,avatar_url=excluded.avatar_url,last_interaction=excluded.last_interaction",(str(other.get('pk')),username,other.get('full_name') or '',other.get('profile_pic_url') or '','Instagram Direct',utcnow(),utcnow()))
            total+=1
        log_event('INFO','inbox_sync_success',f'{total} conversas sincronizadas')
        return True, f'{total} conversas sincronizadas.'
    except Exception as e:
        log_event('ERROR','inbox_sync_failed',f'{type(e).__name__}: {e}',_exception_details(e,cl,'inbox_sync')); return False, f'{type(e).__name__}: {e}'

def sync_thread(thread_id):
    cl=_get_client()
    if cl is None: return False,'Conecte o Instagram primeiro.'
    try:
        with CLIENT_LOCK: t=cl.direct_thread(int(thread_id), amount=100)
        d=_model(t); users=[_model(u) for u in (d.get('users') or [])]; lookup={str(u.get('pk')):u for u in users}
        for item in (d.get('messages') or d.get('items') or []):
            m=_model(item); iid=str(m.get('id') or m.get('item_id') or '')
            if not iid: continue
            upk=str(m.get('user_id') or '')
            own=str(get_setting('ig_user_id',''))==upk
            user=lookup.get(upk,{})
            vals=(thread_id,iid,upk,user.get('username') or ('Você' if own else ''),'out' if own else 'in',m.get('item_type') or 'text',m.get('text') or m.get('link_text') or '',str(m.get('timestamp') or m.get('created_at') or ''),_safe_json(m))
            inserted=False
            try:
                execute('INSERT INTO inbox_messages(thread_id,item_id,user_pk,username,direction,message_type,text,created_at,raw_json) VALUES(?,?,?,?,?,?,?,?,?)',vals); inserted=True
            except Exception: pass
            if inserted and not own and (m.get('text') or ''):
                run_dm_automations(upk, user.get('username') or '', m.get('text') or '', thread_id)
        execute('UPDATE inbox_threads SET unread=0 WHERE thread_id=?',(thread_id,)); return True,'Conversa atualizada.'
    except Exception as e: return False,f'{type(e).__name__}: {e}'

def send_thread_message(thread_id,text):
    cl=_get_client(); text=(text or '').strip()
    if not cl or not text: return False,'Mensagem inválida ou Instagram desconectado.'
    try:
        with CLIENT_LOCK: cl.direct_send(text, thread_ids=[int(thread_id)])
        execute('INSERT INTO inbox_messages(thread_id,item_id,user_pk,username,direction,message_type,text,created_at,raw_json) VALUES(?,?,?,?,?,?,?,?,?)',(thread_id,'local-'+uuid.uuid4().hex,get_setting('ig_user_id',''),get_setting('ig_username',''),'out','text',text,utcnow(),'{}'))
        execute('UPDATE inbox_threads SET last_message=?,last_message_at=? WHERE thread_id=?',(text,utcnow(),thread_id)); return True,'Mensagem enviada.'
    except Exception as e: return False,f'{type(e).__name__}: {e}'

def sync_media(amount=24):
    cl=_get_client()
    if not cl: return False,'Conecte o Instagram primeiro.'
    try:
        user_id=int(get_setting('ig_user_id','0')); _set_operation('media_sync')
        with CLIENT_LOCK: medias=cl.user_medias(user_id, amount=amount)
        for media in medias or []:
            d=_model(media); pk=str(d.get('pk') or d.get('id') or '')
            if not pk: continue
            vals=(pk,str(d.get('media_type') or ''),d.get('product_type') or '',d.get('caption_text') or d.get('caption') or '',str(d.get('thumbnail_url') or ''),str(d.get('video_url') or d.get('media_url') or ''),str(d.get('taken_at') or ''),int(d.get('like_count') or 0),int(d.get('comment_count') or 0),_safe_json(d))
            sql='INSERT INTO media_cache(pk,media_type,product_type,caption,thumbnail_url,media_url,taken_at,like_count,comment_count,raw_json) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(pk) DO UPDATE SET caption=excluded.caption,thumbnail_url=excluded.thumbnail_url,media_url=excluded.media_url,taken_at=excluded.taken_at,like_count=excluded.like_count,comment_count=excluded.comment_count,raw_json=excluded.raw_json'
            if _is_postgres(): sql=sql.replace('excluded.','EXCLUDED.')
            execute(sql,vals)
        return True,f'{len(medias or [])} publicações sincronizadas.'
    except Exception as e: return False,f'{type(e).__name__}: {e}'

def sync_comments(media_pk, amount=50):
    cl=_get_client()
    if not cl: return False,'Conecte o Instagram primeiro.'
    try:
        with CLIENT_LOCK: comments=cl.media_comments(int(media_pk), amount=amount)
        for c in comments or []:
            d=_model(c); user=_model(d.get('user')); pk=str(d.get('pk') or d.get('id') or '')
            if not pk: continue
            vals=(pk,str(media_pk),str(user.get('pk') or d.get('user_id') or ''),user.get('username') or '',d.get('text') or '',str(d.get('created_at_utc') or d.get('created_at') or ''),0,_safe_json(d))
            inserted=False
            try:
                execute('INSERT INTO comment_cache(pk,media_pk,user_pk,username,text,created_at,replied,raw_json) VALUES(?,?,?,?,?,?,?,?)',vals); inserted=True
            except Exception: pass
            if inserted:
                run_comment_automations(media_pk, pk, vals[2], vals[3], vals[4])
        return True,f'{len(comments or [])} comentários sincronizados.'
    except Exception as e: return False,f'{type(e).__name__}: {e}'

def reply_comment(comment_pk,text):
    cl=_get_client(); text=(text or '').strip()
    if not cl or not text: return False,'Resposta inválida.'
    try:
        comment=rows('SELECT media_pk FROM comment_cache WHERE pk=?',(str(comment_pk),))
        if not comment: return False,'Comentário não encontrado no cache.'
        with CLIENT_LOCK: cl.media_comment(int(comment[0]['media_pk']), text, replied_to_comment_id=int(comment_pk))
        execute('UPDATE comment_cache SET replied=1 WHERE pk=?',(comment_pk,)); return True,'Comentário respondido.'
    except Exception as e: return False,f'{type(e).__name__}: {e}'

def publish_photo(file_path,caption=''):
    cl=_get_client()
    if not cl: return False,'Conecte o Instagram primeiro.',None
    try:
        with CLIENT_LOCK: media=cl.photo_upload(file_path, caption or '')
        d=_model(media); return True,'Publicação enviada.',str(d.get('pk') or '')
    except Exception as e: return False,f'{type(e).__name__}: {e}',None

def _keyword_match(text, keyword, mode='contains'):
    import unicodedata
    def norm(v):
        v=unicodedata.normalize('NFKD',(v or '').lower()); return ''.join(ch for ch in v if not unicodedata.combining(ch)).strip()
    text=norm(text); keyword=norm(keyword)
    if not keyword: return False
    return text==keyword if mode=='equals' else keyword in text

def _apply_tag_to_contact(user_pk, tag):
    tag=(tag or '').strip()
    if not user_pk or not tag: return
    found=rows('SELECT tags FROM contacts WHERE pk=?',(str(user_pk),))
    if not found: return
    tags=[x.strip() for x in (found[0].get('tags') or '').split(',') if x.strip()]
    if tag.lower() not in [x.lower() for x in tags]: tags.append(tag)
    execute('UPDATE contacts SET tags=?,score=score+5,last_interaction=? WHERE pk=?',(', '.join(tags),utcnow(),str(user_pk)))

def run_dm_automations(user_pk, username, text, thread_id=None):
    cl=_get_client()
    if not cl: return
    truth='TRUE' if _is_postgres() else '1'
    autos=rows(f"SELECT * FROM automations WHERE enabled={truth} AND trigger_type='dm_keyword' ORDER BY id ASC")
    for a in autos:
        if not _keyword_match(text,a.get('keyword'),a.get('match_mode')): continue
        try:
            if a.get('dm_text'):
                
                if thread_id:
                    with CLIENT_LOCK: cl.direct_send(a['dm_text'], thread_ids=[int(thread_id)])
                else:
                    with CLIENT_LOCK: cl.direct_send(a['dm_text'], user_ids=[int(user_pk)])
            _apply_tag_to_contact(user_pk,a.get('tag'))
            execute('UPDATE automations SET executions=executions+1,updated_at=? WHERE id=?',(utcnow(),a['id']))
            log_event('INFO','automation_executed',f"Automação {a['name']} executada para @{username}",{'automation_id':a['id'],'trigger':'dm_keyword'})
        except Exception as e:
            execute('UPDATE automations SET failures=failures+1,updated_at=? WHERE id=?',(utcnow(),a['id']))
            log_event('ERROR','automation_failed',f"{a['name']}: {type(e).__name__}: {e}")

def run_comment_automations(media_pk, comment_pk, user_pk, username, text):
    cl=_get_client()
    if not cl: return
    truth='TRUE' if _is_postgres() else '1'
    autos=rows(f"SELECT * FROM automations WHERE enabled={truth} AND trigger_type='comment_keyword' ORDER BY id ASC")
    for a in autos:
        if a.get('scope')=='media' and str(a.get('media_id'))!=str(media_pk): continue
        if not _keyword_match(text,a.get('keyword'),a.get('match_mode')): continue
        try:
            if a.get('reply_text'):
                with CLIENT_LOCK: cl.media_comment(int(media_pk), a['reply_text'], replied_to_comment_id=int(comment_pk))
            if a.get('dm_text') and user_pk:
                with CLIENT_LOCK: cl.direct_send(a['dm_text'],user_ids=[int(user_pk)])
            if user_pk:
                found=rows('SELECT pk FROM contacts WHERE pk=?',(str(user_pk),))
                if not found:
                    execute('INSERT INTO contacts(pk,username,full_name,status,score,tags,source,first_contact,last_interaction) VALUES(?,?,?,?,?,?,?,?,?)',(str(user_pk),username,'','lead',5,a.get('tag') or '','Comentário',utcnow(),utcnow()))
                else: _apply_tag_to_contact(user_pk,a.get('tag'))
            execute('UPDATE automations SET executions=executions+1,updated_at=? WHERE id=?',(utcnow(),a['id']))
            execute('UPDATE comment_cache SET replied=1 WHERE pk=?',(str(comment_pk),))
            log_event('INFO','automation_executed',f"Automação {a['name']} executada para @{username}",{'automation_id':a['id'],'trigger':'comment_keyword','media_pk':str(media_pk)})
        except Exception as e:
            execute('UPDATE automations SET failures=failures+1,updated_at=? WHERE id=?',(utcnow(),a['id']))
            log_event('ERROR','automation_failed',f"{a['name']}: {type(e).__name__}: {e}")
