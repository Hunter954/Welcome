import os
from functools import wraps
from flask import Flask, flash, redirect, render_template, request, session, url_for

from .db import init_db, rows
from .instagram_service import login as ig_login, logout as ig_logout, save_config, start_worker, status, sync_once

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "change-me-now")
init_db()
start_worker()


def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("admin"):
            return redirect(url_for("admin_login"))
        return fn(*args, **kwargs)
    return wrapper


@app.get("/health")
def health():
    return {"ok": True}, 200


@app.route("/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        u = request.form.get("username", "")
        p = request.form.get("password", "")
        if u == os.getenv("ADMIN_USERNAME", "admin") and p == os.getenv("ADMIN_PASSWORD", "admin"):
            session["admin"] = True
            return redirect(url_for("dashboard"))
        flash("Login administrativo inválido.", "danger")
    return render_template("admin_login.html")


@app.get("/logout")
def admin_logout():
    session.clear()
    return redirect(url_for("admin_login"))


@app.get("/")
@admin_required
def dashboard():
    cfg = status()
    logs = rows("SELECT * FROM dm_log ORDER BY id DESC LIMIT 30")
    followers = rows("SELECT * FROM followers ORDER BY first_seen DESC LIMIT 30")
    counts = {
        "followers": rows("SELECT COUNT(*) AS n FROM followers")[0]["n"],
        "sent": rows("SELECT COUNT(*) AS n FROM dm_log WHERE status='sent'")[0]["n"],
        "errors": rows("SELECT COUNT(*) AS n FROM dm_log WHERE status='error'")[0]["n"],
    }
    return render_template("dashboard.html", cfg=cfg, logs=logs, followers=followers, counts=counts)


@app.route("/instagram/login", methods=["POST"])
@admin_required
def instagram_login():
    ok, msg = ig_login(request.form.get("username", "").strip(), request.form.get("password", ""), request.form.get("verification_code") or None)
    if msg == "2FA_REQUIRED":
        session["pending_ig_user"] = request.form.get("username", "").strip()
        session["pending_ig_pass"] = request.form.get("password", "")
        flash("Sua conta usa autenticação em 2 fatores. Digite o código e conecte novamente.", "warning")
        return redirect(url_for("dashboard", twofa="1"))
    flash(msg, "success" if ok else "danger")
    return redirect(url_for("dashboard"))


@app.post("/instagram/2fa")
@admin_required
def instagram_2fa():
    user = session.pop("pending_ig_user", "")
    password = session.pop("pending_ig_pass", "")
    code = request.form.get("verification_code", "").strip()
    ok, msg = ig_login(user, password, code)
    flash(msg, "success" if ok else "danger")
    return redirect(url_for("dashboard"))


@app.post("/instagram/logout")
@admin_required
def instagram_disconnect():
    ig_logout()
    flash("Conta do Instagram desconectada.", "success")
    return redirect(url_for("dashboard"))


@app.post("/config")
@admin_required
def config():
    save_config(
        request.form.get("welcome_message", ""),
        request.form.get("welcome_enabled") == "on",
        request.form.get("poll_seconds", "90"),
        request.form.get("max_dms_per_hour", "12"),
        request.form.get("min_dm_delay_seconds", "25"),
    )
    flash("Configurações salvas.", "success")
    return redirect(url_for("dashboard"))


@app.post("/sync")
@admin_required
def sync():
    result = sync_once(send_messages=True)
    if result.get("ok"):
        if result.get("baseline"):
            flash(f"Base inicial criada com {result['total']} seguidores. Ninguém antigo recebeu DM.", "success")
        else:
            flash(f"Sincronização concluída: {result['new']} novos, {result['sent']} DMs enviadas, {result['errors']} erros.", "success")
    else:
        flash(result.get("message", "Falha na sincronização."), "danger")
    return redirect(url_for("dashboard"))
