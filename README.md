# Instagram Welcome Automation

Painel web em Flask + instagrapi para detectar seguidores novos e enviar uma DM de boas-vindas.

## Como funciona

1. Você entra no painel administrativo.
2. Conecta a conta do Instagram com usuário/senha e, se necessário, 2FA.
3. O sistema salva/reutiliza a sessão do instagrapi.
4. Na primeira sincronização, todos os seguidores atuais viram a **base inicial** e não recebem mensagem.
5. Nas sincronizações seguintes, IDs novos entram na fila e recebem a mensagem configurada.
6. Há limite de DMs por hora e intervalo mínimo entre envios.

## Railway

### 1. Crie um projeto e suba este repositório
Conecte o GitHub ao Railway.

### 2. Banco
Recomendado: adicione PostgreSQL ao projeto e deixe o Railway injetar `DATABASE_URL`.

### 3. Volume persistente
Adicione um Volume e monte em:

`/data`

Isso preserva a sessão `instagram_session.json`. Sem volume, um redeploy pode apagar a sessão e causar novos logins/challenges.

### 4. Variáveis
Use o `.env.example` como referência:

- `SECRET_KEY`: chave longa aleatória.
- `ADMIN_USERNAME`: login do painel.
- `ADMIN_PASSWORD`: senha do painel.
- `DATABASE_URL`: fornecida pelo PostgreSQL do Railway.
- `DATA_DIR=/data`
- `FERNET_KEY`: opcional; chave Fernet para criptografar a senha. Se omitida, deriva de `SECRET_KEY`.
- `POLL_SECONDS=90`
- `MAX_DMS_PER_HOUR=12`
- `MIN_DM_DELAY_SECONDS=25`
- `WELCOME_ENABLED=false`

### Gerar uma FERNET_KEY

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## Desenvolvimento local

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
cp .env.example .env
flask --app app.main run --debug
```

## Observações importantes

- `instagrapi` é uma API privada/não oficial. Mudanças do Instagram podem quebrar login, seguidores ou Direct.
- Use sessão persistente e evite trocar de IP/região constantemente.
- Se aparecer challenge, abra o app oficial do Instagram, aprove o login e depois tente reconectar.
- Comece com limites conservadores. Não use esta base para spam, listas compradas ou DMs em massa.
- O worker é executado dentro do mesmo processo web. O `Procfile` fixa 1 worker para impedir dois loops enviando DMs duplicadas.
