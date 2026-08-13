import asyncio
import json
import logging
import hashlib
import shutil
import os
import random
import sys
import threading
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
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
SESSION_BACKUP_FILE = os.path.join(DATA_DIR, "instagram_session.backup.json")
IG_PROXY_URL = os.getenv("IG_PROXY_URL", "").strip()

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


def _int_setting(key, default, minimum=None, maximum=None):
    try:
        value = int(get_setting(key, default))
    except (TypeError, ValueError):
        value = int(default)
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def status():
    return {
        "connected": _bool(get_setting("ig_connected", "false")),
        "username": get_setting("ig_username", ""),
        "last_poll": get_setting("last_poll", "Nunca"),
        "last_error": get_setting("last_error", ""),
        "welcome_enabled": _bool(get_setting("welcome_enabled", os.getenv("WELCOME_ENABLED", "false"))),
        "poll_seconds": _int_setting("poll_seconds", os.getenv("POLL_SECONDS", "1"), 1, 3600),
        "max_dms_per_hour": _int_setting("max_dms_per_hour", os.getenv("MAX_DMS_PER_HOUR", "12"), 1, 50),
        "max_dms_per_day": _int_setting("max_dms_per_day", "80", 1, 500),
        "min_dm_delay_seconds": _int_setting("min_dm_delay_seconds", os.getenv("MIN_DM_DELAY_SECONDS", "1"), 0, 600),
        "max_dm_delay_seconds": _int_setting("max_dm_delay_seconds", "2", 0, 900),
        "max_retries": _int_setting("max_retries", "3", 0, 10),
        "welcome_message": get_setting("welcome_message", "Olá, {first_name}! 👋 Obrigado por seguir @{account}. Seja muito bem-vindo(a)!"),
        "alternate_enabled": _bool(get_setting("alternate_enabled", "false")),
        "welcome_message_alt": get_setting("welcome_message_alt", "Oi, {first_name}! 😊 Que bom ter você por aqui. Obrigado por seguir @{account}!"),
        "schedule_enabled": _bool(get_setting("schedule_enabled", "false")),
        "schedule_start": get_setting("schedule_start", "09:00"),
        "schedule_end": get_setting("schedule_end", "21:00"),
        "schedule_days": get_setting("schedule_days", "0,1,2,3,4,5,6"),
        "timezone": get_setting("timezone", "America/Sao_Paulo"),
        "excluded_usernames": get_setting("excluded_usernames", ""),
        "session_saved": os.path.exists(SESSION_FILE),
        "proxy_configured": bool(IG_PROXY_URL),
        "auth_mode": get_setting("ig_auth_mode", "password"),
    }


def save_config(message, enabled, poll_seconds, max_dms_per_hour, min_delay,
                max_dms_per_day=80, max_delay=45, max_retries=3, alternate_enabled=False,
                alternate_message="", schedule_enabled=False, schedule_start="09:00",
                schedule_end="21:00", schedule_days="0,1,2,3,4,5,6", excluded_usernames=""):
    min_delay_i = max(0, int(min_delay))
    max_delay_i = max(min_delay_i, int(max_delay))
    set_setting("welcome_message", message.strip())
    set_setting("welcome_enabled", str(bool(enabled)).lower())
    set_setting("poll_seconds", max(1, min(3600, int(poll_seconds))))
    set_setting("max_dms_per_hour", max(1, min(50, int(max_dms_per_hour))))
    set_setting("max_dms_per_day", max(1, min(500, int(max_dms_per_day))))
    set_setting("min_dm_delay_seconds", min_delay_i)
    set_setting("max_dm_delay_seconds", min(900, max_delay_i))
    set_setting("max_retries", max(0, min(10, int(max_retries))))
    set_setting("alternate_enabled", str(bool(alternate_enabled)).lower())
    set_setting("welcome_message_alt", alternate_message.strip())
    set_setting("schedule_enabled", str(bool(schedule_enabled)).lower())
    set_setting("schedule_start", schedule_start or "09:00")
    set_setting("schedule_end", schedule_end or "21:00")
    set_setting("schedule_days", schedule_days or "0,1,2,3,4,5,6")
    set_setting("timezone", "America/Sao_Paulo")
    set_setting("excluded_usernames", excluded_usernames.strip())
    log_event("INFO", "config_saved", "Configurações profissionais da automação salvas", {
        "enabled": bool(enabled), "hourly_limit": max_dms_per_hour, "daily_limit": max_dms_per_day,
        "schedule_enabled": bool(schedule_enabled), "alternate_enabled": bool(alternate_enabled),
    })


def _session_fingerprint(path=SESSION_FILE):
    try:
        data = Path(path).read_bytes()
        return hashlib.sha256(data).hexdigest()[:12]
    except Exception:
        return None


def _safe_dump_settings(cl):
    """Grava a sessão de forma atômica e mantém uma cópia de segurança."""
    tmp = SESSION_FILE + ".tmp"
    try:
        cl.dump_settings(tmp)
        if os.path.exists(SESSION_FILE):
            try:
                shutil.copy2(SESSION_FILE, SESSION_BACKUP_FILE)
            except Exception:
                pass
        os.replace(tmp, SESSION_FILE)
        log_event("INFO", "session_saved", "Sessão do aiograpi persistida", {
            "fingerprint": _session_fingerprint(),
            "backup_exists": os.path.exists(SESSION_BACKUP_FILE),
        })
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


async def _new_client(load_saved_session=True):
    cl = Client()
    cl.delay_range = [1, 3]
    if IG_PROXY_URL:
        try:
            cl.set_proxy(IG_PROXY_URL)
            log_event("INFO", "proxy_configured", "Proxy estável configurado para o Instagram", {"configured": True})
        except Exception as e:
            log_event("ERROR", "proxy_config_failed", f"Falha ao configurar proxy: {type(e).__name__}: {e}")
    session_loaded = False
    session_error = None
    if load_saved_session and os.path.exists(SESSION_FILE):
        try:
            cl.load_settings(SESSION_FILE)
            session_loaded = True
            log_event("INFO", "session_loaded", "Sessão persistida carregada no aiograpi", {
                "fingerprint": _session_fingerprint(),
                "proxy_configured": bool(IG_PROXY_URL),
            })
        except Exception as e:
            session_error = f"{type(e).__name__}: {e}"
            # Se a sessão principal estiver corrompida, tenta a cópia de segurança.
            if os.path.exists(SESSION_BACKUP_FILE):
                try:
                    cl.load_settings(SESSION_BACKUP_FILE)
                    session_loaded = True
                    session_error = None
                    log_event("WARNING", "session_backup_loaded", "Sessão principal falhou; backup carregado", {
                        "fingerprint": _session_fingerprint(SESSION_BACKUP_FILE),
                    })
                except Exception as backup_error:
                    session_error += f" | backup: {type(backup_error).__name__}: {backup_error}"
    setattr(cl, "_automation_session_loaded", session_loaded)
    if session_error:
        log_event("WARNING", "session_load_failed", "Falha ao carregar sessão salva", {"error": session_error})
    return cl


async def _restore_client_async():
    global client
    if client is not None:
        return client

    username = get_setting("ig_username")
    enc_password = get_setting("ig_password_enc")
    enc_sessionid = get_setting("ig_sessionid_enc")
    if not username:
        return None

    cl = await _new_client(load_saved_session=True)
    attempt_id = uuid.uuid4().hex[:10]
    session_loaded = getattr(cl, "_automation_session_loaded", False)
    log_event("INFO", "session_restore_started", "Tentando restaurar sessão do Instagram", {
        "attempt_id": attempt_id,
        "username": username,
        "session_loaded": session_loaded,
        "auth_mode": get_setting("ig_auth_mode", "password"),
        "has_password_fallback": bool(enc_password),
        "has_sessionid_fallback": bool(enc_sessionid),
    })

    # Primeiro tenta usar diretamente os settings persistidos. Isso evita um novo
    # login e preserva a identidade/dispositivo já aceitos pelo Instagram.
    if session_loaded:
        try:
            info = await cl.account_info()
            set_setting("ig_connected", "true")
            set_setting("last_error", "")
            client = cl
            log_event("INFO", "session_restore_success", f"Sessão persistida validada para @{info.username}", {
                "attempt_id": attempt_id, "method": "saved_settings",
            })
            return client
        except Exception as saved_error:
            log_event("WARNING", "saved_session_validation_failed", "Settings salvos não foram aceitos; tentando fallback de autenticação", _exception_details(saved_error, cl, attempt_id, username, session_loaded))

    try:
        if enc_sessionid:
            sessionid = decrypt(enc_sessionid)
            await cl.login_by_sessionid(sessionid)
            method = "sessionid"
        elif enc_password:
            password = decrypt(enc_password)
            await cl.login(username, password)
            method = "password"
        else:
            raise LoginRequired("Nenhum método de autenticação de fallback disponível")

        info = await cl.account_info()
        _safe_dump_settings(cl)
        set_setting("ig_connected", "true")
        set_setting("last_error", "")
        client = cl
        log_event("INFO", "session_restore_success", f"Sessão restaurada para @{info.username}", {
            "attempt_id": attempt_id, "method": method,
        })
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
        _safe_dump_settings(cl)
        set_setting("ig_username", username)
        set_setting("ig_password_enc", encrypt(password))
        set_setting("ig_sessionid_enc", "")
        set_setting("ig_auth_mode", "password")
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


def _extract_sessionid(raw):
    """Aceita sessionid puro, Cookie header ou JSON exportado pelo navegador."""
    raw = (raw or "").strip()
    if not raw:
        return None

    # JSON: {"sessionid": "..."}, {"cookies": [...]}, ou lista de cookies.
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            direct = data.get("sessionid")
            if direct:
                return str(direct).strip()
            data = data.get("cookies", data.get("data", data))
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and str(item.get("name", "")).lower() == "sessionid":
                    value = item.get("value")
                    if value:
                        return str(value).strip()
    except Exception:
        pass

    # Cookie header / export textual: sessionid=VALUE; csrftoken=...
    import re
    match = re.search(r'(?:^|[;\s])sessionid\s*=\s*([^;\s]+)', raw, flags=re.I)
    if match:
        return match.group(1).strip().strip('"\'')

    # Netscape cookie export: domínio, flags, path, secure, expires, name, value.
    for line in raw.splitlines():
        parts = line.strip().split('\t')
        if len(parts) >= 7 and parts[-2].lower() == "sessionid" and parts[-1]:
            return parts[-1].strip()

    # Se não parece estrutura de cookie, assume que o usuário colou apenas o valor.
    if "=" not in raw and "\n" not in raw and "\r" not in raw and len(raw) >= 10:
        return raw
    return None


async def _import_browser_session_async(raw):
    global client
    attempt_id = uuid.uuid4().hex[:10]
    sessionid = _extract_sessionid(raw)
    log_event("INFO", "browser_session_import_started", "Importação manual de sessão do navegador iniciada", {
        "attempt_id": attempt_id,
        "payload_format_detected": "sessionid" if sessionid else "unknown",
        "payload_length": len(raw or ""),
    })
    if not sessionid:
        msg = "Não encontrei o cookie sessionid. Cole o valor do sessionid, o Cookie header ou um JSON exportado do instagram.com."
        log_event("WARNING", "browser_session_missing_sessionid", msg, {"attempt_id": attempt_id})
        return False, msg

    cl = await _new_client(load_saved_session=False)
    try:
        await cl.login_by_sessionid(sessionid)
        info = await cl.account_info()
        _safe_dump_settings(cl)
        set_setting("ig_username", info.username or "")
        set_setting("ig_password_enc", "")
        set_setting("ig_sessionid_enc", encrypt(sessionid))
        set_setting("ig_auth_mode", "browser_session")
        set_setting("ig_connected", "true")
        set_setting("last_error", "")
        client = cl
        log_event("INFO", "browser_session_import_success", f"Sessão do navegador validada para @{info.username}", {
            "attempt_id": attempt_id,
            "instagram_user_id": str(getattr(info, "pk", "")),
            "session_fingerprint": _session_fingerprint(),
        })
        return True, f"Sessão importada e validada. Conectado como @{info.username}"
    except Exception as e:
        msg = f"O Instagram não aceitou esse sessionid para a API privada: {type(e).__name__}: {e}"
        set_setting("last_error", msg)
        details = _exception_details(e, cl, attempt_id, None, False)
        details["import_method"] = "login_by_sessionid"
        log_event("ERROR", "browser_session_import_failed", msg, details)
        return False, msg


def import_browser_session(raw):
    with CLIENT_LOCK:
        return _run_async(_import_browser_session_async(raw))


def logout():
    global client
    with CLIENT_LOCK:
        client = None
        set_setting("ig_connected", "false")
        set_setting("ig_password_enc", "")
        set_setting("ig_sessionid_enc", "")
        set_setting("ig_auth_mode", "password")
        removed = False
        for path in (SESSION_FILE, SESSION_BACKUP_FILE):
            try:
                os.remove(path)
                removed = True
            except FileNotFoundError:
                pass
        log_event("INFO", "instagram_logout", "Conta desconectada do painel", {"session_file_removed": removed})


def clear_saved_session():
    global client
    with CLIENT_LOCK:
        client = None
        set_setting("ig_connected", "false")
        set_setting("ig_sessionid_enc", "")
        if get_setting("ig_auth_mode", "password") == "browser_session":
            set_setting("ig_auth_mode", "password")
        removed = False
        for path in (SESSION_FILE, SESSION_BACKUP_FILE):
            try:
                os.remove(path)
                removed = True
            except FileNotFoundError:
                pass
        log_event("WARNING", "saved_session_cleared", "Sessão local do Instagram foi limpa manualmente", {"session_file_removed": removed})
        return removed


def _dm_count_last_hour():
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    r = rows("SELECT COUNT(*) AS n FROM dm_log WHERE status='sent' AND created_at >= ?", (cutoff,))
    return int(r[0]["n"]) if r else 0


def _dm_count_today(tz_name="America/Sao_Paulo"):
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("America/Sao_Paulo")
    now_local = datetime.now(tz)
    start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff = start_local.astimezone(timezone.utc).isoformat()
    r = rows("SELECT COUNT(*) AS n FROM dm_log WHERE status='sent' AND created_at >= ?", (cutoff,))
    return int(r[0]["n"]) if r else 0


def _excluded_set(raw):
    import re
    return {p.strip().lstrip("@").lower() for p in re.split(r"[,;\n\r\t ]+", raw or "") if p.strip()}


def _schedule_allows(cfg):
    if not cfg.get("schedule_enabled"):
        return True, "24h"
    try:
        tz = ZoneInfo(cfg.get("timezone") or "America/Sao_Paulo")
    except Exception:
        tz = ZoneInfo("America/Sao_Paulo")
    now = datetime.now(tz)
    allowed_days = {int(x) for x in str(cfg.get("schedule_days", "0,1,2,3,4,5,6")).split(",") if str(x).strip().isdigit()}
    if now.weekday() not in allowed_days:
        return False, "dia_fora_da_agenda"
    try:
        sh, sm = [int(x) for x in cfg.get("schedule_start", "09:00").split(":")[:2]]
        eh, em = [int(x) for x in cfg.get("schedule_end", "21:00").split(":")[:2]]
        start = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
        end = now.replace(hour=eh, minute=em, second=0, microsecond=0)
    except Exception:
        return True, "agenda_invalida_ignorada"
    if start == end:
        return True, "24h"
    if start < end:
        ok = start <= now <= end
    else:
        ok = now >= start or now <= end
    return ok, "dentro_da_agenda" if ok else "fora_do_horario"


def _render_message(template, user, account):
    first = (user.full_name or user.username or "").strip().split(" ")[0]
    return template.replace("{first_name}", first).replace("{username}", user.username or "").replace("{account}", account)


def _choose_template(cfg):
    if cfg.get("alternate_enabled") and (cfg.get("welcome_message_alt") or "").strip() and random.random() < 0.5:
        return cfg["welcome_message_alt"], "B"
    return cfg["welcome_message"], "A"


def _error_attempts_for(follower_pk):
    r = rows("SELECT COUNT(*) AS n FROM dm_log WHERE follower_pk=? AND status='error'", (str(follower_pk),))
    return int(r[0]["n"]) if r else 0


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
        schedule_ok, schedule_reason = _schedule_allows(cfg)
        if send_messages and cfg["welcome_enabled"] and not baseline and schedule_ok:
            pending_sql = "SELECT pk,username,full_name FROM followers WHERE welcomed=FALSE ORDER BY first_seen ASC LIMIT 50" if _is_postgres() else "SELECT pk,username,full_name FROM followers WHERE welcomed=0 ORDER BY first_seen ASC LIMIT 50"
            pending = rows(pending_sql)
            excluded = _excluded_set(cfg.get("excluded_usernames", ""))
            for row in pending:
                username_norm = (row.get("username") or "").lower().lstrip("@")
                if username_norm in excluded:
                    execute("UPDATE followers SET welcomed=?, welcomed_at=?, last_error=? WHERE pk=?", ((True if _is_postgres() else 1), utcnow(), "excluded_by_rule", str(row["pk"])))
                    log_event("INFO", "follower_excluded", f"@{username_norm} ignorado pela lista de exclusão")
                    continue
                if _error_attempts_for(row["pk"]) >= cfg["max_retries"] and cfg["max_retries"] > 0:
                    continue
                if _dm_count_last_hour() >= cfg["max_dms_per_hour"]:
                    log_event("INFO", "hourly_limit_reached", "Limite de DMs por hora atingido", {"limit": cfg["max_dms_per_hour"]})
                    break
                if _dm_count_today(cfg.get("timezone")) >= cfg["max_dms_per_day"]:
                    log_event("INFO", "daily_limit_reached", "Limite diário de DMs atingido", {"limit": cfg["max_dms_per_day"]})
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
                template, variant = _choose_template(cfg)
                msg = _render_message(template, user, me.username)
                try:
                    await cl.direct_send(msg, user_ids=[int(follower_pk)])
                    execute("UPDATE followers SET welcomed=?, welcomed_at=?, last_error=NULL WHERE pk=?", ((True if _is_postgres() else 1), utcnow(), follower_pk))
                    execute("INSERT INTO dm_log(follower_pk,username,status,message,error,created_at) VALUES(?,?,?,?,?,?)", (follower_pk, row["username"], "sent", msg, None, utcnow()))
                    sent += 1
                    log_event("INFO", "dm_sent", f"Boas-vindas enviada para @{row['username']}", {"variant": variant})
                    delay = random.randint(cfg["min_dm_delay_seconds"], cfg["max_dm_delay_seconds"])
                    if delay > 0:
                        await asyncio.sleep(delay)
                except Exception as e:
                    error_text = f"{type(e).__name__}: {e}"
                    execute("UPDATE followers SET last_error=? WHERE pk=?", (error_text, follower_pk))
                    execute("INSERT INTO dm_log(follower_pk,username,status,message,error,created_at) VALUES(?,?,?,?,?,?)", (follower_pk, row["username"], "error", msg, error_text, utcnow()))
                    log_event("ERROR", "dm_send_failed", error_text, {"username": row["username"], "exception_type": type(e).__name__})
                    errors += 1
                    if isinstance(e, LoginRequired):
                        set_setting("ig_connected", "false")
                        break
        if send_messages and cfg["welcome_enabled"] and not baseline and not schedule_ok:
            log_event("INFO", "schedule_paused", "Envios pausados pela agenda; novos seguidores continuam entrando na fila", {"reason": schedule_reason})
        set_setting("last_poll", utcnow())
        set_setting("last_error", "")
        log_event("INFO", "sync_completed", "Sincronização concluída", {"new": 0 if baseline else len(new_ids), "sent": sent, "errors": errors, "baseline": baseline, "total": len(followers_by_id), "schedule": schedule_reason})
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


async def _send_test_dm_async(username, custom_message=None):
    cl = await _restore_client_async()
    if not cl:
        return False, "Instagram não conectado."
    username = (username or "").strip().lstrip("@")
    if not username:
        return False, "Informe um @usuário para o teste."
    try:
        pk = await cl.user_id_from_username(username)
        user = await cl.user_info(pk)
        me = await cl.account_info()
        template = (custom_message or status()["welcome_message"]).strip()
        msg = _render_message(template, user, me.username)
        await cl.direct_send(msg, user_ids=[int(pk)])
        execute("INSERT INTO dm_log(follower_pk,username,status,message,error,created_at) VALUES(?,?,?,?,?,?)", (str(pk), username, "test", msg, None, utcnow()))
        log_event("INFO", "test_dm_sent", f"DM de teste enviada para @{username}")
        return True, f"DM de teste enviada para @{username}."
    except Exception as e:
        msg = f"Falha no teste: {type(e).__name__}: {e}"
        log_event("ERROR", "test_dm_failed", msg, _exception_details(e, cl, username=username))
        return False, msg


def send_test_dm(username, custom_message=None):
    with CLIENT_LOCK:
        return _run_async(_send_test_dm_async(username, custom_message), timeout=180)


def mark_pending_as_baseline():
    pending_sql = "SELECT COUNT(*) AS n FROM followers WHERE welcomed=FALSE" if _is_postgres() else "SELECT COUNT(*) AS n FROM followers WHERE welcomed=0"
    count = int(rows(pending_sql)[0]["n"])
    if _is_postgres():
        execute("UPDATE followers SET welcomed=TRUE, welcomed_at=?, last_error=? WHERE welcomed=FALSE", (utcnow(), "manual_baseline"))
    else:
        execute("UPDATE followers SET welcomed=1, welcomed_at=?, last_error=? WHERE welcomed=0", (utcnow(), "manual_baseline"))
    log_event("WARNING", "pending_marked_baseline", f"{count} pendentes foram marcados como base manualmente")
    return count


def worker_loop():
    log_event("INFO", "worker_started", "Worker de sincronização iniciado", {"mode": "near_realtime", "poll_seconds": status()["poll_seconds"]})
    while True:
        try:
            if status()["connected"]:
                sync_once(send_messages=True)
        except Exception as e:
            set_setting("last_error", f"Worker: {type(e).__name__}: {e}")
            log_event("ERROR", "worker_error", f"{type(e).__name__}: {e}")
        import time
        time.sleep(max(1, status()["poll_seconds"]))


def start_worker():
    global worker_started
    if worker_started:
        return
    worker_started = True
    t = threading.Thread(target=worker_loop, daemon=True, name="instagram-worker")
    t.start()
