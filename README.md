# Instagram Welcome — instagrapi

Build: `2026.08.14-instagrapi-clean-1`

Projeto Flask para diagnóstico controlado de automação de boas-vindas no Instagram usando `instagrapi==2.18.14`.

## Railway

Variáveis mínimas:

```env
SECRET_KEY=...
ADMIN_USERNAME=admin
ADMIN_PASSWORD=...
FERNET_KEY=...
DATA_DIR=/data
DATABASE_URL=...
```

Mantenha um Volume montado em `/data`. O start command está definido em `railway.json` e `Procfile` com 1 worker.

## Como testar sem misturar causas

1. Faça login no painel administrativo.
2. Importe um `sessionid` válido do Chrome.
3. A importação pausa automaticamente Detector, Disparo e automação.
4. Observe se a sessão permanece válida sem nenhuma outra operação.
5. Ative somente o Detector e observe os logs.
6. Depois teste somente o Disparo.

O detector consulta no máximo os 25 seguidores mais recentes a cada 60 segundos. A primeira base também usa somente 25, evitando baixar a lista inteira logo após o login.

## Confirmar qual deploy está ativo

Abra `/health`. A resposta deve conter:

```json
{
  "app_version": "2026.08.14-instagrapi-clean-1",
  "library": "instagrapi"
}
```

O mesmo build aparece no topo do painel. Se outro valor aparecer, o Railway ainda não está executando este código.

## Command Center MVP — 19/08/2026

O painel foi ampliado mantendo a integração `instagrapi` e a automação original de novos seguidores. Novos módulos:

- Dashboard executivo com métricas e atividade.
- Inbox sincronizável, leitura de threads, envio manual e respostas rápidas.
- CRM de contatos com status, score, tags, notas, telefone, e-mail e atendente.
- Automações por palavra em comentário e Direct, com resposta, DM e tag.
- Central de comentários por publicação.
- Conteúdo: sincronização do feed e publicação de imagem.
- Logs técnicos em tela separada.
- Configurações da sessão e boas-vindas preservadas.

As sincronizações de Inbox, conteúdo e comentários são explícitas no painel para evitar polling excessivo em endpoints privados. As automações de palavra-chave são processadas quando os itens novos são sincronizados; a automação de novos seguidores continua no worker original.
