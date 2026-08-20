import os, json, time, hmac, hashlib, threading
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from .crypto import encrypt, decrypt
from .db import rows, execute, utcnow

GRAPH_VERSION=os.getenv('META_GRAPH_VERSION','v26.0').strip()
GRAPH_BASE=os.getenv('META_GRAPH_BASE','https://graph.instagram.com').rstrip('/')
OAUTH_AUTHORIZE=os.getenv('META_OAUTH_AUTHORIZE','https://www.instagram.com/oauth/authorize')
OAUTH_TOKEN=os.getenv('META_OAUTH_TOKEN','https://api.instagram.com/oauth/access_token')
DEFAULT_SCOPES=os.getenv('META_SCOPES','instagram_business_basic,instagram_business_manage_messages,instagram_business_manage_comments,instagram_business_content_publish').strip()
AUTO_SYNC_SECONDS=max(20,int(os.getenv('META_AUTO_SYNC_SECONDS','45') or 45))
MAX_SYNC_PAGES=max(1,min(10,int(os.getenv('META_MAX_SYNC_PAGES','4') or 4)))
_sync_locks={}
_worker_started=False
_worker_lock=threading.Lock()

def configured():
    return bool(os.getenv('META_APP_ID','').strip() and os.getenv('META_APP_SECRET','').strip() and os.getenv('META_REDIRECT_URI','').strip())

def oauth_url(state):
    p={'client_id':os.getenv('META_APP_ID','').strip(),'redirect_uri':os.getenv('META_REDIRECT_URI','').strip(),'response_type':'code','scope':DEFAULT_SCOPES,'state':state}
    return OAUTH_AUTHORIZE+'?'+urlencode(p)

def _request(method,url,data=None,token=None,form=False):
    headers={'Accept':'application/json','User-Agent':'WelcomeCommandCenter/3.0'}
    body=None
    if token: headers['Authorization']='Bearer '+token
    if data is not None:
        if form:
            body=urlencode(data).encode(); headers['Content-Type']='application/x-www-form-urlencoded'
        else:
            body=json.dumps(data).encode(); headers['Content-Type']='application/json'
    req=Request(url,data=body,headers=headers,method=method)
    try:
        with urlopen(req,timeout=30) as r:
            raw=r.read().decode('utf-8','ignore')
            return r.status, json.loads(raw) if raw else {}
    except HTTPError as e:
        raw=e.read().decode('utf-8','ignore')
        try: payload=json.loads(raw)
        except Exception: payload={'error':{'message':raw or str(e)}}
        return e.code,payload
    except (URLError,TimeoutError) as e:
        return 599,{'error':{'message':str(e)}}

def _get_all(url,token,max_pages=MAX_SYNC_PAGES):
    data=[]; page=0
    while url and page<max_pages:
        s,p=_request('GET',url,token=token)
        if s>=300: return False,_err(p),data
        data.extend(p.get('data') or [])
        url=((p.get('paging') or {}).get('next'))
        page+=1
    return True,'OK',data

def exchange_code(code):
    status,p=_request('POST',OAUTH_TOKEN,{'client_id':os.getenv('META_APP_ID','').strip(),'client_secret':os.getenv('META_APP_SECRET','').strip(),'grant_type':'authorization_code','redirect_uri':os.getenv('META_REDIRECT_URI','').strip(),'code':code},form=True)
    if status>=300 or not p.get('access_token'): return False,_err(p),None
    short=p['access_token']; ig_id=str(p.get('user_id') or '')
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
    vals=(ig_id,username,profile.get('name',''),profile.get('profile_picture_url',''),encrypt(token),expiry,'meta',utcnow(),'connected')
    if existing:
        execute('UPDATE instagram_accounts SET ig_user_id=?,username=?,display_name=?,avatar_url=?,token_enc=?,expires_at=?,provider=?,connected_at=?,status=? WHERE account_id=?',vals+(account_id,))
    else:
        execute('INSERT INTO instagram_accounts(account_id,ig_user_id,username,display_name,avatar_url,token_enc,expires_at,provider,connected_at,status) VALUES(?,?,?,?,?,?,?,?,?,?)',(account_id,)+vals)
    log_event(account_id,'account.connected',{'username':username,'provider':'meta'})
    subscribe_webhooks(account_id)
    kick_full_sync(account_id)
    return True,f'@{username} conectada pela API oficial da Meta.',account_id

def get_profile(token,ig_id='me'):
    target=ig_id or 'me'; fields='id,user_id,username,name,profile_picture_url'
    s,p=_request('GET',f'{GRAPH_BASE}/{GRAPH_VERSION}/{target}?'+urlencode({'fields':fields}),token=token)
    return p if s<300 else {}

def get_account(account_id):
    a=rows('SELECT * FROM instagram_accounts WHERE account_id=?',(account_id,))
    return a[0] if a else None

def list_accounts():
    return rows("SELECT account_id,ig_user_id,username,display_name,avatar_url,expires_at,provider,connected_at,last_webhook_at,status FROM instagram_accounts ORDER BY connected_at DESC")

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
    log_event(account_id,'message.sent',{'recipient_id':str(recipient_id),'text':text})
    return True,'Mensagem enviada pela API oficial.'

def private_reply_comment(account_id,comment_id,text):
    a=get_account(account_id); token=token_for(account_id)
    if not a or not token: return False,'Conta Meta desconectada.'
    s,p=_request('POST',f"{GRAPH_BASE}/{GRAPH_VERSION}/{a['ig_user_id']}/messages",{'recipient':{'comment_id':str(comment_id)},'message':{'text':text}},token=token)
    return (True,'DM privada enviada pelo comentário.') if s<300 else (False,_err(p))

def publish_photo(account_id,image_url,caption=''):
    a=get_account(account_id); token=token_for(account_id)
    if not a or not token: return False,'Conta Meta desconectada.',None
    create_url=f"{GRAPH_BASE}/{GRAPH_VERSION}/{a['ig_user_id']}/media"
    s,p=_request('POST',create_url,{'image_url':image_url,'caption':caption or ''},token=token,form=True)
    creation_id=str(p.get('id') or '') if isinstance(p,dict) else ''
    if s>=300 or not creation_id: return False,_err(p),None
    # Give Meta a brief moment to fetch/process the image; publishing endpoint will still return a useful error if not ready.
    time.sleep(1)
    s2,p2=_request('POST',f"{GRAPH_BASE}/{GRAPH_VERSION}/{a['ig_user_id']}/media_publish",{'creation_id':creation_id},token=token,form=True)
    if s2>=300: return False,_err(p2),None
    media_id=str(p2.get('id') or '')
    log_event(account_id,'media.published',{'media_id':media_id,'caption':caption or ''})
    kick_full_sync(account_id)
    return True,'Publicação enviada pela API oficial da Meta.',media_id

def reply_comment(account_id,comment_id,text):
    token=token_for(account_id)
    if not token: return False,'Conta Meta desconectada.'
    s,p=_request('POST',f'{GRAPH_BASE}/{GRAPH_VERSION}/{comment_id}/replies',{'message':text},token=token)
    if s>=300: return False,_err(p)
    execute('UPDATE comment_cache SET replied=1 WHERE pk=? AND account_id=?',(str(comment_id),account_id))
    log_event(account_id,'comment.replied',{'comment_id':str(comment_id),'text':text})
    return True,'Comentário respondido.'

def subscribe_webhooks(account_id):
    a=get_account(account_id); token=token_for(account_id)
    if not a or not token: return False,'Conta sem token.'
    fields=os.getenv('META_SUBSCRIBED_FIELDS','messages,comments,messaging_postbacks').strip()
    url=f"{GRAPH_BASE}/{GRAPH_VERSION}/{a['ig_user_id']}/subscribed_apps"
    s,p=_request('POST',url,{'subscribed_fields':fields},token=token,form=True)
    ok=s<300
    log_event(account_id,'webhook.subscription',{'ok':ok,'response':p})
    return ok,('Webhook assinado.' if ok else _err(p))

def sync_conversations(account_id):
    a=get_account(account_id); token=token_for(account_id)
    if not a or not token: return False,'Conta Meta desconectada.',0
    # Instagram Messaging API: list conversations, then hydrate each conversation with messages.
    params={'platform':'instagram','fields':'id,updated_time,participants','limit':'50'}
    ok,msg,convs=_get_all(f"{GRAPH_BASE}/{GRAPH_VERSION}/{a['ig_user_id']}/conversations?"+urlencode(params),token)
    if not ok:
        log_event(account_id,'sync.inbox.error',{'error':msg}); return False,msg,0
    imported=0
    for conv in convs:
        cid=str(conv.get('id') or '')
        if not cid: continue
        participants=(conv.get('participants') or {}).get('data') if isinstance(conv.get('participants'),dict) else (conv.get('participants') or [])
        other=None
        for p in participants or []:
            if str(p.get('id') or '') != str(a.get('ig_user_id') or ''):
                other=p; break
        if not other and participants: other=participants[0]
        other=other or {}
        user_id=str(other.get('id') or '')
        username=other.get('username') or other.get('name') or user_id or 'instagram'
        full_name=other.get('name') or username
        contact_pk=f'{account_id}:{user_id or cid}'
        _upsert_contact(contact_pk,username,full_name,'Instagram Inbox',conv.get('updated_time') or utcnow(),score_delta=0)
        thread_id=f'{account_id}:{user_id or cid}'
        # Hydrate message history. Nested fields are not consistently available across API revisions,
        # so query conversation messages separately for better compatibility.
        mparams={'fields':'id,created_time,from,to,message,attachments','limit':'100'}
        mok,mmsg,messages=_get_all(f'{GRAPH_BASE}/{GRAPH_VERSION}/{cid}/messages?'+urlencode(mparams),token)
        if not mok:
            # Fallback used by some Graph responses: messages edge on the conversation object.
            s2,p2=_request('GET',f'{GRAPH_BASE}/{GRAPH_VERSION}/{cid}?'+urlencode({'fields':'messages.limit(100){id,created_time,from,to,message,attachments}'}),token=token)
            messages=((p2.get('messages') or {}).get('data') or []) if s2<300 else []
        messages=sorted(messages,key=lambda x:x.get('created_time') or '')
        last_text=''; last_at=conv.get('updated_time') or utcnow()
        for m in messages:
            mid=str(m.get('id') or '')
            created=m.get('created_time') or last_at
            frm=m.get('from') or {}; frm_id=str(frm.get('id') or '')
            direction='out' if frm_id==str(a.get('ig_user_id') or '') else 'in'
            text=m.get('message') or ''
            mtype='text' if text else ('media' if m.get('attachments') else 'event')
            try:
                execute('INSERT INTO inbox_messages(thread_id,item_id,user_pk,username,direction,message_type,text,created_at,raw_json) VALUES(?,?,?,?,?,?,?,?,?)',(thread_id,mid or f'meta:{cid}:{created}:{direction}',contact_pk,username,direction,mtype,text,created,json.dumps(m,ensure_ascii=False)))
                imported+=1
            except Exception: pass
            if created>=last_at or not last_text:
                last_at=created; last_text=text or '[mídia]'
        if not last_text: last_text='Conversa do Instagram'
        _upsert_thread(thread_id,full_name,username,contact_pk,last_text,last_at,0,conv)
    log_event(account_id,'sync.inbox.completed',{'conversations':len(convs),'messages_imported':imported})
    return True,f'{len(convs)} conversas carregadas.',len(convs)

def sync_media(account_id,with_comments=True,comment_media_limit=25):
    a=get_account(account_id); token=token_for(account_id)
    if not a or not token: return False,'Conta Meta desconectada.',0
    fields='id,caption,media_type,media_product_type,media_url,thumbnail_url,timestamp,like_count,comments_count,permalink'
    ok,msg,items=_get_all(f"{GRAPH_BASE}/{GRAPH_VERSION}/{a['ig_user_id']}/media?"+urlencode({'fields':fields,'limit':'50'}),token)
    if not ok:
        log_event(account_id,'sync.media.error',{'error':msg}); return False,msg,0
    for idx,m in enumerate(items):
        pk=str(m.get('id') or '')
        if not pk: continue
        existing=rows('SELECT pk FROM media_cache WHERE pk=?',(pk,))
        vals=(m.get('media_type',''),m.get('media_product_type',''),m.get('caption',''),m.get('thumbnail_url') or m.get('media_url',''),m.get('media_url',''),m.get('timestamp') or utcnow(),int(m.get('like_count') or 0),int(m.get('comments_count') or 0),json.dumps(m,ensure_ascii=False),account_id)
        if existing:
            execute('UPDATE media_cache SET media_type=?,product_type=?,caption=?,thumbnail_url=?,media_url=?,taken_at=?,like_count=?,comment_count=?,raw_json=?,account_id=? WHERE pk=?',vals+(pk,))
        else:
            execute('INSERT INTO media_cache(pk,media_type,product_type,caption,thumbnail_url,media_url,taken_at,like_count,comment_count,raw_json,account_id) VALUES(?,?,?,?,?,?,?,?,?,?,?)',(pk,)+vals)
        if with_comments and idx<int(comment_media_limit): sync_comments(account_id,pk,emit=False)
    log_event(account_id,'sync.media.completed',{'media':len(items)})
    return True,f'{len(items)} publicações carregadas.',len(items)

def sync_comments(account_id,media_id,emit=True):
    token=token_for(account_id)
    if not token: return False,'Conta Meta desconectada.',0
    fields='id,text,timestamp,username,from,like_count,hidden'
    ok,msg,items=_get_all(f'{GRAPH_BASE}/{GRAPH_VERSION}/{media_id}/comments?'+urlencode({'fields':fields,'limit':'100'}),token)
    if not ok:
        if emit: log_event(account_id,'sync.comments.error',{'media_id':media_id,'error':msg})
        return False,msg,0
    for c in items: _upsert_comment(account_id,media_id,c,run_automation=False)
    if emit: log_event(account_id,'sync.comments.completed',{'media_id':media_id,'comments':len(items)})
    return True,f'{len(items)} comentários carregados.',len(items)

def sync_all(account_id):
    lock=_sync_locks.setdefault(account_id,threading.Lock())
    if not lock.acquire(blocking=False): return False,'Sincronização já em andamento.'
    try:
        c_ok,c_msg,c_n=sync_conversations(account_id)
        existing_comments=rows('SELECT COUNT(*) AS n FROM comment_cache WHERE account_id=?',(account_id,))
        first_comment_seed=not existing_comments or int(existing_comments[0].get('n') or 0)==0
        m_ok,m_msg,m_n=sync_media(account_id,with_comments=True,comment_media_limit=(25 if first_comment_seed else 5))
        execute('UPDATE instagram_accounts SET last_sync_at=? WHERE account_id=?',(utcnow(),account_id))
        log_event(account_id,'sync.full.completed',{'inbox_ok':c_ok,'media_ok':m_ok,'conversations':c_n,'media':m_n})
        return c_ok or m_ok, f'{c_msg} {m_msg}'
    finally: lock.release()

def kick_full_sync(account_id):
    if not account_id: return
    threading.Thread(target=sync_all,args=(account_id,),daemon=True,name=f'meta-sync-{account_id[-8:]}').start()

def start_auto_sync_worker():
    global _worker_started
    with _worker_lock:
        if _worker_started: return
        _worker_started=True
    def loop():
        # Small startup delay so Flask/DB can finish booting on Railway.
        time.sleep(5)
        while True:
            try:
                for a in list_accounts():
                    if a.get('status')=='connected': sync_all(a['account_id'])
            except Exception as e:
                try: log_event('system','sync.worker.error',{'error':str(e)})
                except Exception: pass
            time.sleep(AUTO_SYNC_SECONDS)
    threading.Thread(target=loop,daemon=True,name='meta-auto-sync').start()

def verify_signature(raw,signature):
    secret=os.getenv('META_APP_SECRET','').strip()
    if not secret or not signature or not signature.startswith('sha256='): return False
    digest=hmac.new(secret.encode(),raw,hashlib.sha256).hexdigest()
    return hmac.compare_digest(signature[7:],digest)

def webhook_verify(args):
    if args.get('hub.mode')=='subscribe' and args.get('hub.verify_token')==os.getenv('META_WEBHOOK_VERIFY_TOKEN','').strip(): return args.get('hub.challenge','')
    return None

def ingest_webhook(payload):
    processed=0
    for entry in payload.get('entry',[]) or []:
        account_id='meta:'+str(entry.get('id') or '')
        if not get_account(account_id): account_id=None
        events=list(entry.get('messaging',[]) or [])
        for change in entry.get('changes',[]) or []: events.append({'_change':change})
        for evt in events:
            try:
                aid=account_id
                recipient=(evt.get('recipient') or {}).get('id')
                if recipient and get_account('meta:'+str(recipient)): aid='meta:'+str(recipient)
                if not aid: continue
                if evt.get('message'): _ingest_message(aid,evt); processed+=1
                elif evt.get('_change'): _ingest_change(aid,evt['_change']); processed+=1
                elif evt.get('postback'): log_event(aid,'messaging.postback',evt); processed+=1
            except Exception as e:
                if account_id: log_event(account_id,'webhook.error',{'error':str(e)})
    return processed

def _ingest_message(account_id,evt):
    a=get_account(account_id) or {}
    sender=str((evt.get('sender') or {}).get('id') or '')
    recipient=str((evt.get('recipient') or {}).get('id') or '')
    business_id=str(a.get('ig_user_id') or '')
    direction='out' if sender==business_id else 'in'
    other_id=recipient if direction=='out' else sender
    if not other_id or other_id==business_id: return
    mid=str((evt.get('message') or {}).get('mid') or '')
    text=(evt.get('message') or {}).get('text') or ''
    ts=evt.get('timestamp'); created=datetime.fromtimestamp(ts/1000,tz=timezone.utc).isoformat() if ts else utcnow()
    thread_id=f'{account_id}:{other_id}'; contact_pk=f'{account_id}:{other_id}'
    display=_contact_display(account_id,other_id)
    username=display.get('username') or other_id; full_name=display.get('name') or username
    _upsert_contact(contact_pk,username,full_name,'Meta webhook',created,score_delta=(3 if direction=='in' else 0),avatar=display.get('profile_picture_url',''))
    _upsert_thread(thread_id,full_name,username,contact_pk,text or '[mídia]',created,1 if direction=='in' else 0,evt)
    try: execute('INSERT INTO inbox_messages(thread_id,item_id,user_pk,username,direction,message_type,text,created_at,raw_json) VALUES(?,?,?,?,?,?,?,?,?)',(thread_id,mid or f'wh:{ts}:{other_id}:{direction}',contact_pk,username,direction,'text' if text else 'media',text,created,json.dumps(evt,ensure_ascii=False)))
    except Exception: pass
    execute('UPDATE instagram_accounts SET last_webhook_at=? WHERE account_id=?',(utcnow(),account_id))
    event='message.received' if direction=='in' else 'message.sent.webhook'
    log_event(account_id,event,{'thread_id':thread_id,'sender_id':sender,'recipient_id':recipient,'username':username,'name':full_name,'text':text,'message_id':mid,'direction':direction})
    if direction=='in': _run_dm_automations(account_id,other_id,text)

def _contact_display(account_id,user_id):
    token=token_for(account_id)
    if not token or not user_id: return {}
    s,p=_request('GET',f'{GRAPH_BASE}/{GRAPH_VERSION}/{user_id}?'+urlencode({'fields':'id,username,name,profile_picture_url'}),token=token)
    return p if s<300 else {}

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
            if a.get('tag'): _append_contact_tag(f'{account_id}:{sender}',a['tag'])
            log_event(account_id,'automation.executed',{'automation_id':a['id'],'ok':ok,'message':msg})

def _run_comment_automations(account_id,comment):
    text=(comment.get('text') or '').strip(); cid=str(comment.get('id') or '')
    if not text or not cid: return
    media=comment.get('media') or {}; media_id=str(media.get('id') or comment.get('media_id') or '')
    frm=comment.get('from') or {}; user_id=str(frm.get('id') or comment.get('user_id') or '')
    autos=[a for a in rows("SELECT * FROM automations WHERE trigger_type='comment_keyword' AND account_id=? ORDER BY id",(account_id,)) if bool(a.get('enabled'))]
    low=text.lower()
    for a in autos:
        if a.get('scope')=='media' and a.get('media_id') and str(a.get('media_id'))!=media_id: continue
        kw=(a.get('keyword') or '').strip().lower()
        if not kw: continue
        match=kw==low if a.get('match_mode')=='equals' else kw in low
        if not match: continue
        ok_any=True; messages=[]
        if a.get('reply_text'):
            ok,msg=reply_comment(account_id,cid,a['reply_text']); ok_any=ok_any and ok; messages.append(msg)
        if a.get('dm_text'):
            ok,msg=private_reply_comment(account_id,cid,a['dm_text']); ok_any=ok_any and ok; messages.append(msg)
        if a.get('tag') and user_id: _append_contact_tag(f'{account_id}:{user_id}',a['tag'])
        execute('UPDATE automations SET executions=executions+1,failures=failures+?,updated_at=? WHERE id=?',(0 if ok_any else 1,utcnow(),a['id']))
        log_event(account_id,'automation.executed',{'automation_id':a['id'],'source':'comment','comment_id':cid,'ok':ok_any,'message':' | '.join(messages)})

def _ingest_change(account_id,change):
    field=change.get('field') or 'change'; value=change.get('value') or {}
    if field in ('comments','comment'):
        media=value.get('media') or {}; media_id=str(media.get('id') or value.get('media_id') or '')
        if value.get('id') and media_id:
            _upsert_comment(account_id,media_id,value,run_automation=True)
            log_event(account_id,'comment.received',{'comment_id':str(value.get('id')),'media_id':media_id,'text':value.get('text',''),'username':((value.get('from') or {}).get('username') or value.get('username',''))})
            return
    log_event(account_id,f'webhook.{field}',value)
    # Media changes can vary by subscription/version. Pull current state asynchronously instead of guessing payload shape.
    if field in ('media','live_comments'): kick_full_sync(account_id)

def _upsert_contact(pk,username,full_name,source,when,score_delta=0,avatar=''):
    existing=rows('SELECT pk,score,avatar_url FROM contacts WHERE pk=?',(pk,))
    if existing:
        execute('UPDATE contacts SET username=?,full_name=?,avatar_url=?,last_interaction=?,score=score+? WHERE pk=?',(username or '',full_name or username or '',avatar or existing[0].get('avatar_url',''),when,int(score_delta),pk))
    else:
        execute('INSERT INTO contacts(pk,username,full_name,avatar_url,status,score,tags,notes,source,first_contact,last_interaction) VALUES(?,?,?,?,?,?,?,?,?,?,?)',(pk,username or '',full_name or username or '',avatar or '','lead',int(score_delta),'','',source,when,when))

def _upsert_thread(thread_id,title,username,user_pk,last_message,last_at,unread,raw):
    th=rows('SELECT thread_id,unread FROM inbox_threads WHERE thread_id=?',(thread_id,))
    rawj=json.dumps(raw,ensure_ascii=False)
    if th:
        execute('UPDATE inbox_threads SET title=?,username=?,user_pk=?,last_message=?,last_message_at=?,unread=?,raw_json=? WHERE thread_id=?',(title,username,user_pk,last_message,last_at,max(int(th[0].get('unread') or 0),int(unread)),rawj,thread_id))
    else:
        execute('INSERT INTO inbox_threads(thread_id,title,username,user_pk,last_message,last_message_at,unread,raw_json) VALUES(?,?,?,?,?,?,?,?)',(thread_id,title,username,user_pk,last_message,last_at,int(unread),rawj))

def _upsert_comment(account_id,media_id,c,run_automation=False):
    cid=str(c.get('id') or '')
    if not cid: return
    frm=c.get('from') or {}; username=c.get('username') or frm.get('username') or frm.get('name') or ''
    user_id=str(frm.get('id') or '')
    when=c.get('timestamp') or c.get('created_time') or utcnow(); text=c.get('text') or ''
    existing=rows('SELECT pk FROM comment_cache WHERE pk=?',(cid,))
    raw=json.dumps(c,ensure_ascii=False)
    if existing:
        execute('UPDATE comment_cache SET media_pk=?,user_pk=?,username=?,text=?,created_at=?,raw_json=?,account_id=? WHERE pk=?',(media_id,user_id,username,text,when,raw,account_id,cid))
    else:
        execute('INSERT INTO comment_cache(pk,media_pk,user_pk,username,text,created_at,replied,raw_json,account_id) VALUES(?,?,?,?,?,?,?,?,?)',(cid,media_id,user_id,username,text,when,0,raw,account_id))
    if user_id:
        _upsert_contact(f'{account_id}:{user_id}',username,frm.get('name') or username,'Comentário',when,score_delta=1)
    if run_automation: _run_comment_automations(account_id,{**c,'media_id':media_id,'user_id':user_id})

def _append_contact_tag(pk,tag):
    c=rows('SELECT tags FROM contacts WHERE pk=?',(pk,))
    if not c: return
    tags=[x.strip() for x in (c[0].get('tags') or '').split(',') if x.strip()]
    if tag not in tags: tags.append(tag)
    execute('UPDATE contacts SET tags=? WHERE pk=?',(', '.join(tags),pk))

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
