import os, json, time, hmac, hashlib
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from .crypto import encrypt, decrypt
from .db import rows, execute, utcnow

GRAPH_VERSION=os.getenv('META_GRAPH_VERSION','v26.0').strip()
GRAPH_BASE=os.getenv('META_GRAPH_BASE','https://graph.instagram.com').rstrip('/')
OAUTH_AUTHORIZE=os.getenv('META_OAUTH_AUTHORIZE','https://www.instagram.com/oauth/authorize')
OAUTH_TOKEN=os.getenv('META_OAUTH_TOKEN','https://api.instagram.com/oauth/access_token')
DEFAULT_SCOPES=os.getenv('META_SCOPES','instagram_business_basic,instagram_business_manage_messages,instagram_business_manage_comments,instagram_business_content_publish').strip()

def configured():
    return bool(os.getenv('META_APP_ID','').strip() and os.getenv('META_APP_SECRET','').strip() and os.getenv('META_REDIRECT_URI','').strip())

def oauth_url(state):
    p={'client_id':os.getenv('META_APP_ID','').strip(),'redirect_uri':os.getenv('META_REDIRECT_URI','').strip(),'response_type':'code','scope':DEFAULT_SCOPES,'state':state}
    return OAUTH_AUTHORIZE+'?'+urlencode(p)

def _request(method,url,data=None,token=None,form=False):
    headers={'Accept':'application/json','User-Agent':'WelcomeCommandCenter/2.0'}
    body=None
    if token: headers['Authorization']='Bearer '+token
    if data is not None:
        if form:
            body=urlencode(data).encode(); headers['Content-Type']='application/x-www-form-urlencoded'
        else:
            body=json.dumps(data).encode(); headers['Content-Type']='application/json'
    req=Request(url,data=body,headers=headers,method=method)
    try:
        with urlopen(req,timeout=25) as r:
            raw=r.read().decode('utf-8','ignore')
            return r.status, json.loads(raw) if raw else {}
    except HTTPError as e:
        raw=e.read().decode('utf-8','ignore')
        try: payload=json.loads(raw)
        except Exception: payload={'error':{'message':raw or str(e)}}
        return e.code,payload

def exchange_code(code):
    status,p=_request('POST',OAUTH_TOKEN,{'client_id':os.getenv('META_APP_ID','').strip(),'client_secret':os.getenv('META_APP_SECRET','').strip(),'grant_type':'authorization_code','redirect_uri':os.getenv('META_REDIRECT_URI','').strip(),'code':code},form=True)
    if status>=300 or not p.get('access_token'): return False,_err(p),None
    short=p['access_token']; ig_id=str(p.get('user_id') or '')
    # Exchange for long-lived token when supported.
    u=f"{GRAPH_BASE}/access_token?"+urlencode({'grant_type':'ig_exchange_token','client_secret':os.getenv('META_APP_SECRET','').strip(),'access_token':short})
    s2,p2=_request('GET',u)
    token=p2.get('access_token') if s2<300 and p2.get('access_token') else short
    expires=int(p2.get('expires_in') or p.get('expires_in') or 0)
    profile=get_profile(token,ig_id)
    if profile.get('id'): ig_id=str(profile.get('id'))
    username=profile.get('username') or f'instagram_{ig_id[-6:]}'
    expiry=(datetime.now(timezone.utc)+timedelta(seconds=expires)).isoformat() if expires else None
    account_id='meta:'+ig_id
    existing=rows('SELECT account_id FROM instagram_accounts WHERE account_id=?',(account_id,))
    if existing:
        execute('UPDATE instagram_accounts SET ig_user_id=?,username=?,display_name=?,avatar_url=?,token_enc=?,expires_at=?,provider=?,connected_at=?,status=? WHERE account_id=?',(ig_id,username,profile.get('name',''),profile.get('profile_picture_url',''),encrypt(token),expiry,'meta',utcnow(),'connected',account_id))
    else:
        execute('INSERT INTO instagram_accounts(account_id,ig_user_id,username,display_name,avatar_url,token_enc,expires_at,provider,connected_at,status) VALUES(?,?,?,?,?,?,?,?,?,?)',(account_id,ig_id,username,profile.get('name',''),profile.get('profile_picture_url',''),encrypt(token),expiry,'meta',utcnow(),'connected'))
    log_event(account_id,'account.connected',{'username':username,'provider':'meta'})
    return True,f'@{username} conectada pela API oficial da Meta.',account_id

def get_profile(token,ig_id='me'):
    target=ig_id or 'me'
    fields='id,user_id,username,name,profile_picture_url'
    s,p=_request('GET',f'{GRAPH_BASE}/{GRAPH_VERSION}/{target}?'+urlencode({'fields':fields}),token=token)
    return p if s<300 else {}

def get_account(account_id):
    a=rows('SELECT * FROM instagram_accounts WHERE account_id=?',(account_id,))
    return a[0] if a else None

def list_accounts(): return rows("SELECT account_id,ig_user_id,username,display_name,avatar_url,expires_at,provider,connected_at,last_webhook_at,status FROM instagram_accounts ORDER BY connected_at DESC")

def token_for(account_id):
    a=get_account(account_id)
    if not a or not a.get('token_enc'): return None
    try: return decrypt(a['token_enc'])
    except Exception: return None

def send_message(account_id,recipient_id,text):
    a=get_account(account_id); token=token_for(account_id)
    if not a or not token: return False,'Conta Meta desconectada.'
    s,p=_request('POST',f"{GRAPH_BASE}/{GRAPH_VERSION}/{a['ig_user_id']}/messages",{'recipient':{'id':str(recipient_id)},'message':{'text':text}},token=token)
    if s>=300: return False,_err(p)
    return True,'Mensagem enviada pela API oficial.'

def verify_signature(raw,signature):
    secret=os.getenv('META_APP_SECRET','').strip()
    if not secret or not signature or not signature.startswith('sha256='): return False
    digest=hmac.new(secret.encode(),raw,hashlib.sha256).hexdigest()
    return hmac.compare_digest(signature[7:],digest)

def webhook_verify(args):
    if args.get('hub.mode')=='subscribe' and args.get('hub.verify_token')==os.getenv('META_WEBHOOK_VERIFY_TOKEN','').strip():
        return args.get('hub.challenge','')
    return None

def ingest_webhook(payload):
    processed=0
    for entry in payload.get('entry',[]) or []:
        account_id='meta:'+str(entry.get('id') or '')
        if not get_account(account_id):
            # Some payloads identify the recipient inside messaging; resolve it below.
            account_id=None
        events=[]
        events.extend(entry.get('messaging',[]) or [])
        for change in entry.get('changes',[]) or []:
            events.append({'_change':change})
        for evt in events:
            try:
                aid=account_id
                recipient=(evt.get('recipient') or {}).get('id')
                if recipient and get_account('meta:'+str(recipient)): aid='meta:'+str(recipient)
                if not aid: continue
                if evt.get('message'):
                    _ingest_message(aid,evt); processed+=1
                elif evt.get('_change'):
                    _ingest_change(aid,evt['_change']); processed+=1
                elif evt.get('postback'):
                    log_event(aid,'messaging.postback',evt); processed+=1
            except Exception as e:
                if account_id: log_event(account_id,'webhook.error',{'error':str(e)})
    return processed

def _ingest_message(account_id,evt):
    sender=str((evt.get('sender') or {}).get('id') or '')
    recipient=str((evt.get('recipient') or {}).get('id') or '')
    mid=str((evt.get('message') or {}).get('mid') or '')
    text=(evt.get('message') or {}).get('text') or ''
    ts=evt.get('timestamp')
    created=datetime.fromtimestamp(ts/1000,tz=timezone.utc).isoformat() if ts else utcnow()
    thread_id=f'{account_id}:{sender}'
    contact_pk=f'{account_id}:{sender}'
    a=get_account(account_id)
    display=sender
    existing=rows('SELECT pk FROM contacts WHERE pk=?',(contact_pk,))
    if not existing:
        execute('INSERT INTO contacts(pk,username,full_name,status,score,tags,notes,source,first_contact,last_interaction) VALUES(?,?,?,?,?,?,?,?,?,?)',(contact_pk,display,display,'lead',3,'','','Meta webhook',created,created))
    else: execute('UPDATE contacts SET last_interaction=?,score=score+3 WHERE pk=?',(created,contact_pk))
    th=rows('SELECT thread_id FROM inbox_threads WHERE thread_id=?',(thread_id,))
    if th: execute('UPDATE inbox_threads SET last_message=?,last_message_at=?,unread=1 WHERE thread_id=?',(text or '[mídia]',created,thread_id))
    else: execute('INSERT INTO inbox_threads(thread_id,title,username,user_pk,last_message,last_message_at,unread,raw_json) VALUES(?,?,?,?,?,?,?,?)',(thread_id,display,display,contact_pk,text or '[mídia]',created,1,json.dumps(evt,ensure_ascii=False)))
    try: execute('INSERT INTO inbox_messages(thread_id,item_id,user_pk,username,direction,message_type,text,created_at,raw_json) VALUES(?,?,?,?,?,?,?,?,?)',(thread_id,mid or f'wh:{ts}:{sender}',contact_pk,display,'in','text' if text else 'media',text,created,json.dumps(evt,ensure_ascii=False)))
    except Exception: pass
    execute('UPDATE instagram_accounts SET last_webhook_at=? WHERE account_id=?',(utcnow(),account_id))
    log_event(account_id,'message.received',{'thread_id':thread_id,'sender_id':sender,'text':text,'message_id':mid})
    _run_dm_automations(account_id,sender,text)

def _run_dm_automations(account_id,sender,text):
    if not text: return
    autos=[a for a in rows("SELECT * FROM automations WHERE trigger_type='dm_keyword' AND account_id=? ORDER BY id",(account_id,)) if bool(a.get('enabled'))]
    low=text.lower()
    for a in autos:
        kw=(a.get('keyword') or '').strip().lower()
        if not kw: continue
        match=kw==low if a.get('match_mode')=='equals' else kw in low
        if match and a.get('dm_text'):
            ok,msg=send_message(account_id,sender,a['dm_text'])
            execute('UPDATE automations SET executions=executions+1,failures=failures+?,updated_at=? WHERE id=?',(0 if ok else 1,utcnow(),a['id']))
            log_event(account_id,'automation.executed',{'automation_id':a['id'],'ok':ok,'message':msg})

def _ingest_change(account_id,change):
    field=change.get('field') or 'change'; value=change.get('value') or {}
    log_event(account_id,f'webhook.{field}',value)

def log_event(account_id,event_type,payload):
    execute('INSERT INTO realtime_events(account_id,event_type,payload,created_at) VALUES(?,?,?,?)',(account_id,event_type,json.dumps(payload,ensure_ascii=False),utcnow()))

def recent_events(account_id,after_id=0,limit=50):
    return rows('SELECT * FROM realtime_events WHERE account_id=? AND id>? ORDER BY id ASC LIMIT ?',(account_id,int(after_id or 0),int(limit)))

def disconnect(account_id):
    execute("UPDATE instagram_accounts SET status='disconnected',token_enc='' WHERE account_id=?",(account_id,))

def _err(p):
    e=p.get('error') if isinstance(p,dict) else None
    if isinstance(e,dict): return e.get('message') or json.dumps(e,ensure_ascii=False)
    return str(e or p or 'Erro desconhecido na Meta API')
