import os, uuid, json, secrets
from functools import wraps
from pathlib import Path
from flask import Flask, flash, redirect, render_template, request, session, url_for, jsonify, send_from_directory
from werkzeug.utils import secure_filename

from .db import init_db, rows, execute, utcnow, _is_postgres
from .meta_service import (
 configured as meta_configured, oauth_url as meta_oauth_url, exchange_code as meta_exchange_code,
 list_accounts as meta_list_accounts, get_account as meta_get_account, send_message as meta_send_message,
 webhook_verify as meta_webhook_verify, verify_signature as meta_verify_signature, ingest_webhook as meta_ingest_webhook,
 recent_events as meta_recent_events, disconnect as meta_disconnect,
 sync_all as meta_sync_all, kick_full_sync as meta_kick_full_sync, start_auto_sync_worker as meta_start_auto_sync_worker,
 reply_comment as meta_reply_comment, sync_comments as meta_sync_comments, sync_media as meta_sync_media,
 subscribe_webhooks as meta_subscribe_webhooks, publish_photo as meta_publish_photo,
)
from .instagram_service import (
 clear_saved_session, import_browser_session, login as ig_login, logout as ig_logout,
 mark_pending_as_baseline, save_config, send_test_dm, start_worker, status, sync_once,
 sync_inbox, sync_thread, send_thread_message, sync_media as legacy_sync_media, sync_comments as legacy_sync_comments, reply_comment as legacy_reply_comment,
 publish_photo, APP_VERSION, BOOT_ID,
)

app=Flask(__name__); app.secret_key=os.getenv('SECRET_KEY','change-me-now')
UPLOAD_DIR=os.path.join(os.getenv('DATA_DIR',os.path.join(os.getcwd(),'data')),'uploads'); Path(UPLOAD_DIR).mkdir(parents=True,exist_ok=True)
init_db(); start_worker(); meta_start_auto_sync_worker()

def admin_required(fn):
 @wraps(fn)
 def wrapper(*args,**kwargs):
  if not session.get('admin'): return redirect(url_for('admin_login'))
  return fn(*args,**kwargs)
 return wrapper

def active_meta_account():
 aid=session.get('active_account_id')
 return meta_get_account(aid) if aid else None

def common(active):
 legacy=status(); accounts=meta_list_accounts(); acct=active_meta_account(); cfg=dict(legacy)
 if acct and acct.get('status')=='connected':
  cfg.update({'connected':True,'username':acct.get('username'),'auth_mode':'Meta API oficial','provider':'meta','account_id':acct.get('account_id')})
 else:
  cfg['provider']='instagrapi' if legacy.get('connected') else None
 return {'cfg':cfg,'legacy_cfg':legacy,'accounts':accounts,'active_account':acct,'meta_ready':meta_configured(),'active':active}

def active_account_id():
 a=active_meta_account(); return a.get('account_id') if a else ''

def count(sql,params=()):
 try: return int(rows(sql,params)[0]['n'])
 except Exception: return 0

@app.get('/health')
def health(): return {'ok':True,'app_version':APP_VERSION,'boot_id':BOOT_ID,'library':'instagrapi'},200

@app.get('/uploads/<path:filename>')
def uploaded_file(filename): return send_from_directory(UPLOAD_DIR,filename)

@app.route('/login',methods=['GET','POST'])
def admin_login():
 if request.method=='POST':
  if request.form.get('username','')==os.getenv('ADMIN_USERNAME','admin') and request.form.get('password','')==os.getenv('ADMIN_PASSWORD','admin'):
   session['admin']=True; return redirect(url_for('dashboard'))
  flash('Login administrativo inválido.','danger')
 return render_template('admin_login.html')
@app.get('/logout')
def admin_logout(): session.clear(); return redirect(url_for('admin_login'))

@app.get('/')
@admin_required
def dashboard():
 ctx=common('dashboard'); aid=active_account_id()
 if aid:
  meta_kick_full_sync(aid)
  prefix=aid+':%'
  counts={'followers':0,'sent':count("SELECT COUNT(*) AS n FROM inbox_messages WHERE thread_id LIKE ? AND direction='out'",(prefix,)),'contacts':count('SELECT COUNT(*) AS n FROM contacts WHERE pk LIKE ?',(prefix,)),'unread':count('SELECT COUNT(*) AS n FROM inbox_threads WHERE thread_id LIKE ? AND unread=1',(prefix,)),'automations':count('SELECT COUNT(*) AS n FROM automations WHERE account_id=?',(aid,)),'posts':count('SELECT COUNT(*) AS n FROM media_cache WHERE account_id=?',(aid,))}
  autos=rows('SELECT * FROM automations WHERE account_id=? ORDER BY id DESC LIMIT 5',(aid,))
  threads=rows('SELECT * FROM inbox_threads WHERE thread_id LIKE ? ORDER BY last_message_at DESC LIMIT 5',(prefix,))
  recent=rows("SELECT 'INFO' AS level,event_type AS event,payload AS message,'' AS details,created_at FROM realtime_events WHERE account_id=? ORDER BY id DESC LIMIT 8",(aid,))
 else:
  counts={'followers':count('SELECT COUNT(*) AS n FROM followers'),'sent':count("SELECT COUNT(*) AS n FROM dm_log WHERE status='sent'"),'contacts':count("SELECT COUNT(*) AS n FROM contacts WHERE pk NOT LIKE 'meta:%'"),'unread':count("SELECT COUNT(*) AS n FROM inbox_threads WHERE thread_id NOT LIKE 'meta:%' AND unread=1"),'automations':count("SELECT COUNT(*) AS n FROM automations WHERE account_id='' OR account_id IS NULL"),'posts':count("SELECT COUNT(*) AS n FROM media_cache WHERE account_id='' OR account_id IS NULL")}
  autos=rows("SELECT * FROM automations WHERE account_id='' OR account_id IS NULL ORDER BY id DESC LIMIT 5")
  threads=rows("SELECT * FROM inbox_threads WHERE thread_id NOT LIKE 'meta:%' ORDER BY last_message_at DESC LIMIT 5")
  recent=rows('SELECT * FROM app_log ORDER BY id DESC LIMIT 8')
 ctx.update(counts=counts,recent=recent,automations=autos,threads=threads)
 return render_template('dashboard.html',**ctx)

@app.route('/inbox')
@admin_required
def inbox():
 thread_id=request.args.get('thread'); ctx=common('inbox'); aid=active_account_id()
 if aid:
  meta_kick_full_sync(aid)
  threads=rows('SELECT * FROM inbox_threads WHERE thread_id LIKE ? ORDER BY last_message_at DESC LIMIT 100',(aid+':%',))
 else:
  threads=rows("SELECT * FROM inbox_threads WHERE thread_id NOT LIKE 'meta:%' ORDER BY last_message_at DESC LIMIT 100")
 messages=[]; contact=None
 if thread_id:
  execute('UPDATE inbox_threads SET unread=0 WHERE thread_id=?',(thread_id,))
  messages=rows('SELECT * FROM inbox_messages WHERE thread_id=? ORDER BY created_at ASC',(thread_id,)); t=rows('SELECT * FROM inbox_threads WHERE thread_id=?',(thread_id,))
  if t and t[0].get('user_pk'):
   c=rows('SELECT * FROM contacts WHERE pk=?',(t[0]['user_pk'],)); contact=c[0] if c else None
 ctx.update(threads=threads,messages=messages,thread_id=thread_id,contact=contact,quick=rows('SELECT * FROM quick_replies ORDER BY shortcut'))
 return render_template('inbox.html',**ctx)
@app.post('/inbox/sync')
@admin_required
def inbox_sync_route(): ok,msg=sync_inbox(); flash(msg,'success' if ok else 'danger'); return redirect(url_for('inbox'))
@app.post('/inbox/<thread_id>/sync')
@admin_required
def thread_sync_route(thread_id): ok,msg=sync_thread(thread_id); flash(msg,'success' if ok else 'danger'); return redirect(url_for('inbox',thread=thread_id))
@app.post('/inbox/<thread_id>/send')
@admin_required
def thread_send_route(thread_id):
 text=request.form.get('message','').strip(); aid=active_account_id()
 if aid and thread_id.startswith(aid+':'):
  recipient=thread_id.split(':')[-1]; ok,msg=meta_send_message(aid,recipient,text)
  if ok:
   execute('INSERT INTO inbox_messages(thread_id,item_id,user_pk,username,direction,message_type,text,created_at,raw_json) VALUES(?,?,?,?,?,?,?,?,?)',(thread_id,'local:'+uuid.uuid4().hex,'','','out','text',text,utcnow(),'{}'))
   execute('UPDATE inbox_threads SET last_message=?,last_message_at=?,unread=0 WHERE thread_id=?',(text,utcnow(),thread_id))
 else:
  ok,msg=send_thread_message(thread_id,text)
 flash(msg,'success' if ok else 'danger'); return redirect(url_for('inbox',thread=thread_id))

@app.get('/contacts')
@admin_required
def contacts():
 aid=active_account_id()
 if aid: meta_kick_full_sync(aid)
 data=rows('SELECT * FROM contacts WHERE pk LIKE ? ORDER BY last_interaction DESC LIMIT 300',(aid+':%',)) if aid else rows("SELECT * FROM contacts WHERE pk NOT LIKE 'meta:%' ORDER BY last_interaction DESC LIMIT 300")
 return render_template('contacts.html',**common('contacts'),contacts=data)
@app.post('/contacts/<pk>')
@admin_required
def contact_update(pk):
 execute('UPDATE contacts SET status=?,tags=?,notes=?,phone=?,email=?,company=?,city=?,assigned_to=?,score=? WHERE pk=?',(request.form.get('status','lead'),request.form.get('tags',''),request.form.get('notes',''),request.form.get('phone',''),request.form.get('email',''),request.form.get('company',''),request.form.get('city',''),request.form.get('assigned_to',''),int(request.form.get('score') or 0),pk)); flash('Contato atualizado.','success'); return redirect(url_for('contacts'))

@app.route('/automations',methods=['GET','POST'])
@admin_required
def automations():
 if request.method=='POST':
  now=utcnow(); execute('INSERT INTO automations(name,trigger_type,keyword,match_mode,scope,media_id,reply_text,dm_text,tag,enabled,executions,failures,created_at,updated_at,account_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(request.form.get('name') or 'Nova automação',request.form.get('trigger_type','comment_keyword'),request.form.get('keyword',''),request.form.get('match_mode','contains'),request.form.get('scope','all'),request.form.get('media_id',''),request.form.get('reply_text',''),request.form.get('dm_text',''),request.form.get('tag',''),True if _is_postgres() else 1,0,0,now,now,active_account_id())); flash('Automação criada.','success'); return redirect(url_for('automations'))
 aid=active_account_id(); autos=rows('SELECT * FROM automations WHERE account_id=? ORDER BY id DESC',(aid,)); return render_template('automations.html',**common('automations'),automations=autos)
@app.post('/automations/<int:aid>/toggle')
@admin_required
def automation_toggle(aid):
 a=rows('SELECT enabled FROM automations WHERE id=?',(aid,));
 if a: execute('UPDATE automations SET enabled=?,updated_at=? WHERE id=?',((not bool(a[0]['enabled'])) if _is_postgres() else (0 if a[0]['enabled'] else 1),utcnow(),aid))
 return redirect(url_for('automations'))
@app.post('/automations/<int:aid>/delete')
@admin_required
def automation_delete(aid): execute('DELETE FROM automations WHERE id=?',(aid,)); flash('Automação removida.','warning'); return redirect(url_for('automations'))

@app.get('/comments')
@admin_required
def comments():
 aid=active_account_id()
 if aid: meta_kick_full_sync(aid)
 if aid:
  cs=rows('SELECT c.*,m.caption,m.thumbnail_url FROM comment_cache c LEFT JOIN media_cache m ON m.pk=c.media_pk WHERE c.account_id=? ORDER BY c.created_at DESC LIMIT 300',(aid,))
  ms=rows('SELECT * FROM media_cache WHERE account_id=? ORDER BY taken_at DESC LIMIT 50',(aid,))
 else:
  cs=rows("SELECT c.*,m.caption,m.thumbnail_url FROM comment_cache c LEFT JOIN media_cache m ON m.pk=c.media_pk WHERE c.account_id='' OR c.account_id IS NULL ORDER BY c.created_at DESC LIMIT 300")
  ms=rows("SELECT * FROM media_cache WHERE account_id='' OR account_id IS NULL ORDER BY taken_at DESC LIMIT 50")
 return render_template('comments.html',**common('comments'),comments=cs,medias=ms)

@app.post('/comments/sync/<media_pk>')
@admin_required
def comments_sync_route(media_pk):
 aid=active_account_id(); ok,msg,_=(meta_sync_comments(aid,media_pk) if aid else (*legacy_sync_comments(media_pk),0)); flash(msg,'success' if ok else 'danger'); return redirect(url_for('comments'))
@app.post('/comments/<comment_pk>/reply')
@admin_required
def comment_reply_route(comment_pk):
 aid=active_account_id(); ok,msg=(meta_reply_comment(aid,comment_pk,request.form.get('text')) if aid else legacy_reply_comment(comment_pk,request.form.get('text'))); flash(msg,'success' if ok else 'danger'); return redirect(url_for('comments'))

@app.get('/content')
@admin_required
def content():
 aid=active_account_id()
 if aid: meta_kick_full_sync(aid)
 if aid:
  ms=rows('SELECT * FROM media_cache WHERE account_id=? ORDER BY taken_at DESC LIMIT 100',(aid,)); sch=rows('SELECT * FROM scheduled_posts WHERE account_id=? ORDER BY id DESC LIMIT 30',(aid,))
 else:
  ms=rows("SELECT * FROM media_cache WHERE account_id='' OR account_id IS NULL ORDER BY taken_at DESC LIMIT 100"); sch=rows("SELECT * FROM scheduled_posts WHERE account_id='' OR account_id IS NULL ORDER BY id DESC LIMIT 30")
 return render_template('content.html',**common('content'),medias=ms,scheduled=sch)

@app.post('/content/sync')
@admin_required
def content_sync_route():
 aid=active_account_id(); ok,msg,_=(meta_sync_media(aid,True) if aid else (*legacy_sync_media(),0)); flash(msg,'success' if ok else 'danger'); return redirect(url_for('content'))
@app.post('/content/publish')
@admin_required
def content_publish_route():
 f=request.files.get('image'); caption=request.form.get('caption','')
 if not f or not f.filename: flash('Selecione uma imagem.','danger'); return redirect(url_for('content'))
 name=f'{uuid.uuid4().hex}_{secure_filename(f.filename)}'; path=os.path.join(UPLOAD_DIR,name); f.save(path)
 aid=active_account_id()
 if aid:
  public_url=request.host_url.rstrip('/')+url_for('uploaded_file',filename=name)
  ok,msg,pk=meta_publish_photo(aid,public_url,caption)
 else:
  ok,msg,pk=publish_photo(path,caption)
 execute('INSERT INTO scheduled_posts(kind,file_path,caption,status,scheduled_at,published_media_pk,error,created_at,account_id) VALUES(?,?,?,?,?,?,?,?,?)',('photo',path,caption,'published' if ok else 'error',None,pk or '',None if ok else msg,utcnow(),aid)); flash(msg,'success' if ok else 'danger'); return redirect(url_for('content'))

@app.get('/logs')
@admin_required
def logs():
 aid=active_account_id()
 if aid:
  data=rows("SELECT created_at,'INFO' AS level,event_type AS event,payload AS message,'' AS details FROM realtime_events WHERE account_id=? ORDER BY id DESC LIMIT 300",(aid,)); dms=[]
 else:
  data=rows('SELECT * FROM app_log ORDER BY id DESC LIMIT 300'); dms=rows('SELECT * FROM dm_log ORDER BY id DESC LIMIT 100')
 return render_template('logs.html',**common('logs'),logs=data,dms=dms)
@app.get('/settings')
@admin_required
def settings(): return render_template('settings.html',**common('settings'))

# Official Meta / Instagram Login multi-account endpoints
@app.get('/meta/connect')
@admin_required
def meta_connect():
 if not meta_configured():
  flash('Configure META_APP_ID, META_APP_SECRET, META_REDIRECT_URI e META_WEBHOOK_VERIFY_TOKEN no Railway.','warning'); return redirect(url_for('settings'))
 state=secrets.token_urlsafe(24); session['meta_oauth_state']=state
 return redirect(meta_oauth_url(state))

@app.get('/meta/callback')
@admin_required
def meta_callback():
 if request.args.get('state')!=session.pop('meta_oauth_state',None):
  flash('Estado OAuth inválido. Tente conectar novamente.','danger'); return redirect(url_for('settings'))
 if request.args.get('error'):
  flash(request.args.get('error_description') or request.args.get('error'),'danger'); return redirect(url_for('settings'))
 ok,msg,aid=meta_exchange_code(request.args.get('code',''))
 if ok and aid:
  session['active_account_id']=aid
  meta_subscribe_webhooks(aid)
  meta_kick_full_sync(aid)
 flash(msg,'success' if ok else 'danger'); return redirect(url_for('settings'))

@app.post('/accounts/switch')
@admin_required
def account_switch():
 aid=request.form.get('account_id','')
 if aid=='legacy': session.pop('active_account_id',None)
 elif meta_get_account(aid):
  session['active_account_id']=aid
  meta_kick_full_sync(aid)
 return redirect(request.referrer or url_for('dashboard'))

@app.post('/meta/disconnect/<path:account_id>')
@admin_required
def meta_disconnect_route(account_id):
 meta_disconnect(account_id)
 if session.get('active_account_id')==account_id: session.pop('active_account_id',None)
 flash('Conta Meta desconectada do Welcome.','success'); return redirect(url_for('settings'))

@app.route('/webhooks/instagram',methods=['GET','POST'])
def instagram_webhook():
 if request.method=='GET':
  challenge=meta_webhook_verify(request.args)
  return (challenge,200) if challenge is not None else ('Forbidden',403)
 raw=request.get_data()
 if os.getenv('META_VERIFY_SIGNATURE','1')=='1' and not meta_verify_signature(raw,request.headers.get('X-Hub-Signature-256','')):
  return 'Invalid signature',403
 try:
  payload=request.get_json(force=True,silent=False) or {}; processed=meta_ingest_webhook(payload)
 except Exception as e:
  return jsonify({'ok':False,'error':str(e)}),400
 return jsonify({'ok':True,'processed':processed}),200

@app.get('/api/live/state')
@admin_required
def live_state():
 aid=active_account_id()
 if not aid: return jsonify({'account_id':'','threads':[],'messages':[],'media':[],'comments':[]})
 thread_id=request.args.get('thread','')
 threads=rows('SELECT thread_id,title,username,user_pk,last_message,last_message_at,unread FROM inbox_threads WHERE thread_id LIKE ? ORDER BY last_message_at DESC LIMIT 100',(aid+':%',))
 messages=rows('SELECT item_id,direction,message_type,text,created_at FROM inbox_messages WHERE thread_id=? ORDER BY created_at ASC LIMIT 300',(thread_id,)) if thread_id and thread_id.startswith(aid+':') else []
 media=rows('SELECT pk,caption,media_type,product_type,thumbnail_url,taken_at,like_count,comment_count FROM media_cache WHERE account_id=? ORDER BY taken_at DESC LIMIT 100',(aid,))
 comments=rows('SELECT c.pk,c.media_pk,c.username,c.text,c.created_at,c.replied,m.caption,m.thumbnail_url FROM comment_cache c LEFT JOIN media_cache m ON m.pk=c.media_pk WHERE c.account_id=? ORDER BY c.created_at DESC LIMIT 300',(aid,))
 return jsonify({'account_id':aid,'threads':threads,'messages':messages,'media':media,'comments':comments})

@app.post('/api/live/refresh')
@admin_required
def live_refresh():
 aid=active_account_id()
 if aid: meta_kick_full_sync(aid)
 return jsonify({'ok':bool(aid)})

@app.get('/api/live/events')
@admin_required
def live_events():
 aid=active_account_id()
 if not aid: return jsonify({'events':[],'last_id':int(request.args.get('after') or 0)})
 ev=meta_recent_events(aid,request.args.get('after') or 0,50)
 for e in ev:
  try: e['payload']=json.loads(e.get('payload') or '{}')
  except Exception: e['payload']={}
 return jsonify({'events':ev,'last_id':ev[-1]['id'] if ev else int(request.args.get('after') or 0)})

# Existing Instagram/session/welcome endpoints
@app.post('/instagram/login')
@admin_required
def instagram_login():
 u=request.form.get('username','').strip(); ok,msg=ig_login(u,request.form.get('password',''),request.form.get('verification_code') or None)
 if msg=='2FA_REQUIRED': session['pending_ig_user']=u; session['pending_ig_pass']=request.form.get('password',''); flash('Digite o código 2FA.','warning')
 else: flash(msg,'success' if ok else 'danger')
 return redirect(url_for('settings'))
@app.post('/instagram/2fa')
@admin_required
def instagram_2fa(): ok,msg=ig_login(session.pop('pending_ig_user',''),session.pop('pending_ig_pass',''),request.form.get('verification_code','').strip()); flash(msg,'success' if ok else 'danger'); return redirect(url_for('settings'))
@app.post('/instagram/import-session')
@admin_required
def instagram_import_session():
 raw=request.form.get('session_data',''); up=request.files.get('session_file')
 if up and up.filename: raw=up.read().decode('utf-8',errors='ignore')
 ok,msg=import_browser_session(raw); flash(msg,'success' if ok else 'danger'); return redirect(url_for('settings'))
@app.post('/instagram/logout')
@admin_required
def instagram_disconnect(): ig_logout(); flash('Instagram desconectado.','success'); return redirect(url_for('settings'))
@app.post('/instagram/clear-session')
@admin_required
def instagram_clear_session(): clear_saved_session(); flash('Sessão persistente removida.','warning'); return redirect(url_for('settings'))
@app.post('/config')
@admin_required
def config(): save_config(request.form.get('welcome_message',''),request.form.get('welcome_enabled')=='on',request.form.get('excluded_usernames',''),request.form.get('detector_enabled')=='on',request.form.get('sender_enabled')=='on'); flash('Configurações salvas.','success'); return redirect(url_for('settings'))
@app.post('/sync')
@admin_required
def sync():
 r=sync_once(send_messages=True); flash(f"Sincronização: {r.get('new',0)} novos, {r.get('sent',0)} DMs." if r.get('ok') else r.get('message','Falha.'),'success' if r.get('ok') else 'danger'); return redirect(request.referrer or url_for('dashboard'))
@app.post('/instagram/test-dm')
@admin_required
def instagram_test_dm(): ok,msg=send_test_dm(request.form.get('test_username',''),request.form.get('test_message','') or None); flash(msg,'success' if ok else 'danger'); return redirect(url_for('settings'))
@app.post('/followers/mark-baseline')
@admin_required
def followers_mark_baseline(): flash(f'{mark_pending_as_baseline()} seguidores marcados como base.','warning'); return redirect(url_for('settings'))
