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
ROLE_CLIENTS = {}
ROLE_CLIENTS_LOCK = threading.RLock()
DETECTOR_COOLDOWN_UNTIL = None
DETECTOR_RATE_LIMIT_HITS = 0
DETECTOR_STATE_LOCK = threading.RLock()


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
    auth_mode = get_setting("ig_auth_mode", "")
    return {
        "connected": _bool(get_setting("ig_connected", "false")) and auth_mode == "dedicated",
        "username": get_setting("ig_username", ""),
        "last_poll": get_setting("last_poll", "Nunca"),
        "last_error": get_setting("last_error", ""),
        "welcome_enabled": _bool(get_setting("welcome_enabled", os.getenv("WELCOME_ENABLED", "false"))),
        # Polling conservador: consulta novos seguidores a cada 15s.
        # Quando detectado, o envio da DM continua imediato.
        "poll_seconds": 15,
        "max_dms_per_hour": 50,
        "max_dms_per_day": 300,
        "min_dm_delay_seconds": 0,
        "max_dm_delay_seconds": 1,
        "max_retries": 3,
        "welcome_message": get_setting("welcome_message", "Olá, {first_name}! 👋 Obrigado por seguir @{account}. Seja muito bem-vindo(a)!"),
        "alternate_enabled": False,
        "welcome_message_alt": "",
        "schedule_enabled": False,
        "schedule_start": "00:00",
        "schedule_end": "23:59",
        "schedule_days": "0,1,2,3,4,5,6",
        "timezone": get_setting("timezone", "America/Sao_Paulo"),
        "excluded_usernames": get_setting("excluded_usernames", ""),
        "session_saved": os.path.exists(SESSION_FILE),
        "proxy_configured": bool(IG_PROXY_URL),
        "auth_mode": auth_mode or "dedicated",
        "dedicated_session": auth_mode == "dedicated",
    }


def save_config(message, enabled, excluded_usernames=""):
    """Salva apenas o que o operador precisa configurar.

    Polling, delays, agenda, A/B e limites operacionais ficam internos para
    evitar configurações conflitantes. O modo padrão é quase em tempo real.
    """
    set_setting("welcome_message", message.strip())
    set_setting("welcome_enabled", str(bool(enabled)).lower())
    set_setting("excluded_usernames", excluded_usernames.strip())

    # Neutraliza valores antigos que possam ter ficado persistidos no banco.
    set_setting("poll_seconds", 15)
    set_setting("min_dm_delay_seconds", 0)
    set_setting("max_dm_delay_seconds", 1)
    set_setting("alternate_enabled", "false")
    set_setting("welcome_message_alt", "")
    set_setting("schedule_enabled", "false")

    log_event("INFO", "config_saved", "Configurações da automação salvas", {
        "enabled": bool(enabled),
        "mode": "safe_realtime",
        "poll_seconds": 15,
        "dm_delay_seconds": "0-1",
    })


def _session_fingerprint(path=SESSION_FILE):
    try:
        data = Path(path).read_bytes()
        return hashlib.sha256(data).hexdigest()[:12]
    except Exception:
        return None


def _purge_session_files():
    removed = False
    for path in (SESSION_FILE, SESSION_BACKUP_FILE, SESSION_FILE + ".tmp"):
        try:
            os.remove(path)
            removed = True
        except FileNotFoundError:
            pass
        except Exception:
            pass
    return removed


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
    cl.delay_range = [1, 2]
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
    """Restaura somente a sessão dedicada do bot.

    Sessões importadas do navegador não são reutilizadas para evitar que o
    Instagram associe o mesmo sessionid web a um cliente mobile/datacenter.
    """
    global client
    if client is not None:
        return client

    username = get_setting("ig_username")
    enc_password = get_setting("ig_password_enc")
    auth_mode = get_setting("ig_auth_mode", "")
    if not username or auth_mode != "dedicated":
        if auth_mode == "browser_session":
            set_setting("ig_connected", "false")
            set_setting("last_error", "Sessão antiga do Chrome desativada. Faça um login para criar a sessão dedicada do bot.")
        return None

    cl = await _new_client(load_saved_session=True)
    attempt_id = uuid.uuid4().hex[:10]
    session_loaded = getattr(cl, "_automation_session_loaded", False)
    log_event("INFO", "dedicated_session_restore_started", "Restaurando sessão dedicada do bot", {
        "attempt_id": attempt_id,
        "username": username,
        "session_loaded": session_loaded,
        "proxy_configured": bool(IG_PROXY_URL),
        "has_password_fallback": bool(enc_password),
    })

    try:
        if session_loaded:
            # A documentação do aiograpi recomenda carregar os settings e usar
            # login() com as mesmas credenciais; ele valida/reutiliza a sessão
            # e só reloga se o Instagram responder login_required.
            if enc_password:
                await cl.login(username, decrypt(enc_password))
            info = await cl.account_info()
            _safe_dump_settings(cl)
            method = "saved_dedicated_session"
        elif enc_password:
            await cl.login(username, decrypt(enc_password))
            info = await cl.account_info()
            _safe_dump_settings(cl)
            method = "password_fallback"
        else:
            raise LoginRequired("Sessão dedicada ausente e nenhuma credencial de fallback disponível")

        set_setting("ig_connected", "true")
        set_setting("last_error", "")
        client = cl
        log_event("INFO", "dedicated_session_restore_success", f"Sessão dedicada validada para @{info.username}", {
            "attempt_id": attempt_id,
            "method": method,
            "proxy_configured": bool(IG_PROXY_URL),
            "session_fingerprint": _session_fingerprint(),
        })
        return client
    except (ClientLoginRequired, LoginRequired) as e:
        set_setting("ig_connected", "false")
        set_setting("last_error", "Sessão dedicada expirada. Reconecte a conta no painel.")
        log_event("ERROR", "dedicated_session_login_required", "Sessão dedicada expirou e a automação foi pausada", _exception_details(e, cl, attempt_id, username, session_loaded))
        return None
    except Exception as e:
        err = f"Falha ao restaurar sessão dedicada: {type(e).__name__}: {e}"
        set_setting("ig_connected", "false")
        set_setting("last_error", err)
        log_event("ERROR", "dedicated_session_restore_failed", err, _exception_details(e, cl, attempt_id, username, session_loaded))
        return None

def _load_client():
    with CLIENT_LOCK:
        return _run_async(_restore_client_async())


async def _login_async(username, password, verification_code=None):
    global client
    attempt_id = uuid.uuid4().hex[:10]
    if not IG_PROXY_URL:
        msg = "Configure IG_PROXY_URL no Railway com um proxy residencial/mobile fixo antes de criar a sessão dedicada."
        set_setting("last_error", msg)
        log_event("WARNING", "dedicated_login_blocked_no_proxy", msg, {"attempt_id": attempt_id})
        return False, msg
    use_saved = get_setting("ig_auth_mode", "") == "dedicated" and os.path.exists(SESSION_FILE)
    if not use_saved:
        removed_old = _purge_session_files()
        set_setting("ig_sessionid_enc", "")
        if removed_old:
            log_event("INFO", "legacy_session_removed", "Sessão anterior removida antes de criar a sessão dedicada")
    cl = await _new_client(load_saved_session=use_saved)
    session_loaded = getattr(cl, "_automation_session_loaded", False)
    log_event("INFO", "login_started", "Tentativa de login iniciada", {
        "attempt_id": attempt_id,
        "username": username,
        "has_2fa_code": bool(verification_code),
        "session_file_exists": os.path.exists(SESSION_FILE),
        "session_loaded": session_loaded,
        "library": "aiograpi",
        "session_mode": "dedicated",
        "proxy_configured": bool(IG_PROXY_URL),
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
        set_setting("ig_user_id", str(getattr(info, "pk", "")))
        set_setting("ig_password_enc", encrypt(password))
        set_setting("ig_sessionid_enc", "")
        set_setting("ig_auth_mode", "dedicated")
        set_setting("ig_connected", "true")
        set_setting("last_error", "")
        client = cl
        _invalidate_role_clients()
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


def import_browser_session(raw):
    """Desativado: a automação usa somente sessão dedicada do bot."""
    log_event("WARNING", "browser_session_import_blocked", "Importação de sessionid do navegador está desativada no modo dedicado")
    return False, "Importação de sessão do Chrome foi desativada. Use o login dedicado do bot."


def logout():
    global client
    with CLIENT_LOCK:
        client = None
        _invalidate_role_clients()
        set_setting("ig_connected", "false")
        set_setting("ig_password_enc", "")
        set_setting("ig_sessionid_enc", "")
        set_setting("ig_auth_mode", "dedicated")
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
        _invalidate_role_clients()
        set_setting("ig_connected", "false")
        set_setting("ig_sessionid_enc", "")
        set_setting("ig_auth_mode", "dedicated")
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


def _invalidate_role_clients():
    with ROLE_CLIENTS_LOCK:
        ROLE_CLIENTS.clear()


async def _role_client_async(role):
    """Cria um Client independente por função para que polling, DMs e testes não se bloqueiem."""
    with ROLE_CLIENTS_LOCK:
        existing = ROLE_CLIENTS.get(role)
    if existing is not None:
        return existing

    cl = await _new_client(load_saved_session=True)
    session_loaded = getattr(cl, "_automation_session_loaded", False)
    try:
        if session_loaded:
            info = await cl.account_info()
        else:
            enc_password = get_setting("ig_password_enc")
            username = get_setting("ig_username")
            if enc_password and username and get_setting("ig_auth_mode", "") == "dedicated":
                await cl.login(username, decrypt(enc_password))
            else:
                raise LoginRequired("Nenhuma sessão dedicada disponível")
            info = await cl.account_info()
        set_setting("ig_user_id", str(getattr(info, "pk", "")))
        set_setting("ig_username", getattr(info, "username", "") or get_setting("ig_username", ""))
        with ROLE_CLIENTS_LOCK:
            ROLE_CLIENTS[role] = cl
        log_event("INFO", "role_client_ready", f"Cliente independente pronto: {role}", {"role": role})
        return cl
    except Exception:
        with ROLE_CLIENTS_LOCK:
            ROLE_CLIENTS.pop(role, None)
        raise


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


async def _detect_followers_async(force_full=False):
    """Polling seguro: após a base, busca somente os 50 seguidores mais recentes, ordenados por data."""
    global DETECTOR_COOLDOWN_UNTIL, DETECTOR_RATE_LIMIT_HITS
    try:
        cl = await _role_client_async("detector")
        me_id = get_setting("ig_user_id", "")
        if not me_id:
            me = await cl.account_info()
            me_id = str(me.pk)
            set_setting("ig_user_id", me_id)

        known_count = int(rows("SELECT COUNT(*) AS n FROM followers")[0]["n"])
        baseline = known_count == 0
        amount = 0 if baseline or force_full else 50
        t0 = datetime.now(timezone.utc)
        followers = await cl.user_followers(
            str(me_id),
            amount=amount,
            order="date_followed_latest",
            use_cache=False,
        )
        elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
        new_count = _upsert_detected_followers(followers, baseline=baseline)
        set_setting("last_poll", utcnow())
        set_setting("last_error", "")
        log_event("INFO", "followers_polled", "Consulta de seguidores concluída", {
            "mode": "baseline_full" if baseline else "latest_50",
            "returned": len(followers),
            "new": 0 if baseline else new_count,
            "seconds": round(elapsed, 3),
        })
        with DETECTOR_STATE_LOCK:
            DETECTOR_RATE_LIMIT_HITS = 0
            DETECTOR_COOLDOWN_UNTIL = None
        return {"ok": True, "new": 0 if baseline else new_count, "baseline": baseline, "total": len(followers)}
    except PleaseWaitFewMinutes as e:
        with DETECTOR_STATE_LOCK:
            DETECTOR_RATE_LIMIT_HITS += 1
            # 5, 10, 20, 30 minutos conforme reincidência; reseta após uma consulta bem-sucedida.
            minutes = min(30, 5 * (2 ** max(0, DETECTOR_RATE_LIMIT_HITS - 1)))
            DETECTOR_COOLDOWN_UNTIL = datetime.now(timezone.utc) + timedelta(minutes=minutes)
        set_setting("last_poll", utcnow())
        set_setting("last_error", f"Detector em cooldown por {minutes} min: PleaseWaitFewMinutes")
        with ROLE_CLIENTS_LOCK:
            ROLE_CLIENTS.pop("detector", None)
        log_event("WARNING", "detector_rate_limited", "Instagram pediu para aguardar; detector entrou em cooldown", {
            "cooldown_minutes": minutes,
            "cooldown_until": DETECTOR_COOLDOWN_UNTIL.isoformat(),
            "error": f"{type(e).__name__}: {e}",
        })
        return {"ok": False, "message": f"PleaseWaitFewMinutes: cooldown {minutes} min", "cooldown": minutes}
    except ClientThrottledError as e:
        with DETECTOR_STATE_LOCK:
            DETECTOR_RATE_LIMIT_HITS += 1
            minutes = min(30, 5 * (2 ** max(0, DETECTOR_RATE_LIMIT_HITS - 1)))
            DETECTOR_COOLDOWN_UNTIL = datetime.now(timezone.utc) + timedelta(minutes=minutes)
        set_setting("last_poll", utcnow())
        set_setting("last_error", f"Detector em cooldown por {minutes} min: rate limit")
        with ROLE_CLIENTS_LOCK:
            ROLE_CLIENTS.pop("detector", None)
        log_event("WARNING", "detector_throttled", "Instagram limitou as consultas; detector entrou em cooldown", {
            "cooldown_minutes": minutes,
            "cooldown_until": DETECTOR_COOLDOWN_UNTIL.isoformat(),
        })
        return {"ok": False, "message": f"Rate limit: cooldown {minutes} min", "cooldown": minutes}
    except (ClientLoginRequired, LoginRequired) as e:
        set_setting("last_poll", utcnow())
        set_setting("last_error", f"Sessão do Instagram precisa ser renovada: {type(e).__name__}: {e}")
        set_setting("ig_connected", "false")
        with ROLE_CLIENTS_LOCK:
            ROLE_CLIENTS.pop("detector", None)
        log_event("ERROR", "detector_login_required", "Sessão dedicada invalidada; automação pausada até reconectar a conta", {
            "error": f"{type(e).__name__}: {e}"
        })
        return {"ok": False, "message": f"{type(e).__name__}: {e}", "reauth_required": True}
    except Exception as e:
        set_setting("last_poll", utcnow())
        set_setting("last_error", f"Detector: {type(e).__name__}: {e}")
        with ROLE_CLIENTS_LOCK:
            ROLE_CLIENTS.pop("detector", None)
        log_event("ERROR", "follower_poll_failed", f"{type(e).__name__}: {e}")
        return {"ok": False, "message": f"{type(e).__name__}: {e}"}


async def _send_one_pending_async():
    cfg = status()
    if not cfg["welcome_enabled"]:
        return {"ok": True, "sent": 0, "reason": "disabled"}
    schedule_ok, schedule_reason = _schedule_allows(cfg)
    if not schedule_ok:
        return {"ok": True, "sent": 0, "reason": schedule_reason}
    if _dm_count_last_hour() >= cfg["max_dms_per_hour"]:
        return {"ok": True, "sent": 0, "reason": "hourly_limit"}
    if _dm_count_today(cfg.get("timezone")) >= cfg["max_dms_per_day"]:
        return {"ok": True, "sent": 0, "reason": "daily_limit"}

    pending_sql = "SELECT pk,username,full_name FROM followers WHERE welcomed=FALSE ORDER BY first_seen ASC LIMIT 1" if _is_postgres() else "SELECT pk,username,full_name FROM followers WHERE welcomed=0 ORDER BY first_seen ASC LIMIT 1"
    pending = rows(pending_sql)
    if not pending:
        return {"ok": True, "sent": 0, "reason": "empty"}
    row = pending[0]
    username_norm = (row.get("username") or "").lower().lstrip("@")
    if username_norm in _excluded_set(cfg.get("excluded_usernames", "")):
        execute("UPDATE followers SET welcomed=?, welcomed_at=?, last_error=? WHERE pk=?", ((True if _is_postgres() else 1), utcnow(), "excluded_by_rule", str(row["pk"])))
        return {"ok": True, "sent": 0, "reason": "excluded"}
    if _error_attempts_for(row["pk"]) >= cfg["max_retries"] and cfg["max_retries"] > 0:
        return {"ok": True, "sent": 0, "reason": "max_retries"}

    cl = await _role_client_async("sender")
    from types import SimpleNamespace
    user = SimpleNamespace(username=row.get("username") or "", full_name=row.get("full_name") or "")
    template, variant = _choose_template(cfg)
    msg = _render_message(template, user, get_setting("ig_username", ""))
    follower_pk = str(row["pk"])
    t0 = datetime.now(timezone.utc)
    try:
        await cl.direct_send(msg, user_ids=[int(follower_pk)])
        elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
        execute("UPDATE followers SET welcomed=?, welcomed_at=?, last_error=NULL WHERE pk=?", ((True if _is_postgres() else 1), utcnow(), follower_pk))
        execute("INSERT INTO dm_log(follower_pk,username,status,message,error,created_at) VALUES(?,?,?,?,?,?)", (follower_pk, row["username"], "sent", msg, None, utcnow()))
        log_event("INFO", "dm_sent", f"Boas-vindas enviada para @{row['username']}", {"variant": variant, "seconds": round(elapsed, 3)})
        return {"ok": True, "sent": 1, "seconds": elapsed}
    except Exception as e:
        error_text = f"{type(e).__name__}: {e}"
        execute("UPDATE followers SET last_error=? WHERE pk=?", (error_text, follower_pk))
        execute("INSERT INTO dm_log(follower_pk,username,status,message,error,created_at) VALUES(?,?,?,?,?,?)", (follower_pk, row["username"], "error", msg, error_text, utcnow()))
        with ROLE_CLIENTS_LOCK:
            ROLE_CLIENTS.pop("sender", None)
        log_event("ERROR", "dm_send_failed", error_text, {"username": row["username"]})
        return {"ok": False, "sent": 0, "message": error_text}


async def _sync_once_async(send_messages=True):
    detected = await _detect_followers_async()
    if not detected.get("ok"):
        return detected
    sent = 0
    errors = 0
    if send_messages and not detected.get("baseline"):
        # Drena algumas pendências sem fazer nova consulta completa de seguidores.
        for _ in range(10):
            result = await _send_one_pending_async()
            if not result.get("ok"):
                errors += 1
                break
            if not result.get("sent"):
                break
            sent += 1
            cfg = status()
            delay = random.randint(cfg["min_dm_delay_seconds"], cfg["max_dm_delay_seconds"])
            if delay > 0:
                await asyncio.sleep(delay)
    return {**detected, "sent": sent, "errors": errors}


def sync_once(send_messages=True):
    return _run_async(_sync_once_async(send_messages=send_messages), timeout=900)


async def _send_test_dm_async(username, custom_message=None):
    username = (username or "").strip().lstrip("@")
    if not username:
        return False, "Informe um @usuário para o teste."
    try:
        cl = await _role_client_async("test")
        pk = await cl.user_id_from_username(username)
        user = await cl.user_info(pk)
        template = (custom_message or status()["welcome_message"]).strip()
        msg = _render_message(template, user, get_setting("ig_username", ""))
        t0 = datetime.now(timezone.utc)
        await cl.direct_send(msg, user_ids=[int(pk)])
        elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
        execute("INSERT INTO dm_log(follower_pk,username,status,message,error,created_at) VALUES(?,?,?,?,?,?)", (str(pk), username, "test", msg, None, utcnow()))
        log_event("INFO", "test_dm_sent", f"DM de teste enviada para @{username}", {"seconds": round(elapsed, 3)})
        return True, f"DM de teste enviada para @{username}."
    except Exception as e:
        with ROLE_CLIENTS_LOCK:
            ROLE_CLIENTS.pop("test", None)
        msg = f"Falha no teste: {type(e).__name__}: {e}"
        log_event("ERROR", "test_dm_failed", msg, {"username": username})
        return False, msg


def _test_dm_background(username, custom_message=None):
    try:
        _run_async(_send_test_dm_async(username, custom_message), timeout=180)
    except Exception as e:
        log_event("ERROR", "test_dm_background_failed", f"{type(e).__name__}: {e}")


def send_test_dm(username, custom_message=None):
    username = (username or "").strip().lstrip("@")
    if not username:
        return False, "Informe um @usuário para o teste."
    threading.Thread(target=_test_dm_background, args=(username, custom_message), daemon=True, name="instagram-test-dm").start()
    return True, f"Teste para @{username} colocado na fila. O painel não ficará travado enquanto envia."


def mark_pending_as_baseline():
    pending_sql = "SELECT COUNT(*) AS n FROM followers WHERE welcomed=FALSE" if _is_postgres() else "SELECT COUNT(*) AS n FROM followers WHERE welcomed=0"
    count = int(rows(pending_sql)[0]["n"])
    if _is_postgres():
        execute("UPDATE followers SET welcomed=TRUE, welcomed_at=?, last_error=? WHERE welcomed=FALSE", (utcnow(), "manual_baseline"))
    else:
        execute("UPDATE followers SET welcomed=1, welcomed_at=?, last_error=? WHERE welcomed=0", (utcnow(), "manual_baseline"))
    log_event("WARNING", "pending_marked_baseline", f"{count} pendentes foram marcados como base manualmente")
    return count


def detector_loop():
    log_event("INFO", "detector_started", "Detector seguro de novos seguidores iniciado", {
        "mode": "latest_50",
        "poll_seconds": 15,
        "automatic_cooldown": True,
    })
    import time
    while True:
        try:
            now = datetime.now(timezone.utc)
            with DETECTOR_STATE_LOCK:
                cooldown_until = DETECTOR_COOLDOWN_UNTIL
            if cooldown_until and now < cooldown_until:
                remaining = max(1, int((cooldown_until - now).total_seconds()))
                time.sleep(min(15, remaining))
                continue
            if status()["connected"]:
                _run_async(_detect_followers_async(), timeout=120)
        except Exception as e:
            log_event("ERROR", "detector_loop_error", f"{type(e).__name__}: {e}")
        time.sleep(15)


def sender_loop():
    log_event("INFO", "sender_started", "Remetente de DMs iniciado separadamente do detector")
    import time
    while True:
        try:
            if status()["connected"] and status()["welcome_enabled"]:
                result = _run_async(_send_one_pending_async(), timeout=180)
                if result.get("sent"):
                    cfg = status()
                    delay = random.randint(cfg["min_dm_delay_seconds"], cfg["max_dm_delay_seconds"])
                    time.sleep(max(0, delay))
                    continue
        except Exception as e:
            log_event("ERROR", "sender_loop_error", f"{type(e).__name__}: {e}")
        time.sleep(0.25)


def start_worker():
    global worker_started
    if worker_started:
        return
    worker_started = True
    threading.Thread(target=detector_loop, daemon=True, name="instagram-detector").start()
    threading.Thread(target=sender_loop, daemon=True, name="instagram-sender").start()

