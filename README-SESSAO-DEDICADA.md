# Sessão dedicada do Instagram

Esta atualização remove o uso operacional do `sessionid` do Chrome. A automação passa a criar e reutilizar uma sessão própria do aiograpi.

## Railway

Mantenha o Volume montado em `/data` e estas variáveis:

```env
DATA_DIR=/data
IG_PROXY_URL=http://usuario:senha@host:porta
```

Use um proxy residencial ou mobile com IP/região consistentes para esta conta. Evite proxy rotativo que troque de IP a cada requisição.

## Depois do deploy

1. O painel vai considerar qualquer sessão antiga do Chrome como desconectada.
2. Configure `IG_PROXY_URL` no Railway e aguarde o redeploy.
3. No painel, faça login com o usuário e a senha própria do Instagram.
4. Se houver 2FA, informe o código quando solicitado.
5. Depois do primeiro login aceito, a sessão será salva em `/data/instagram_session.json` e reutilizada.

O login dedicado é bloqueado enquanto `IG_PROXY_URL` estiver ausente para evitar novas tentativas pelo IP de datacenter do Railway.
