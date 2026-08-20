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

## Meta API oficial + multi-conta (V2)

Esta build mantém o provider legado `instagrapi`, mas adiciona a arquitetura oficial para produção:

- OAuth via **Instagram API with Instagram Login**;
- várias contas Business/Creator na mesma aplicação Welcome;
- token separado e criptografado por conta;
- seletor de conta no painel;
- webhook central em `/webhooks/instagram`;
- DMs recebidas gravadas no Inbox automaticamente;
- atualização do Inbox em tempo real sem consultar o Instagram a cada refresh;
- envio de resposta pelo endpoint oficial `/<IG_ID>/messages`;
- automações de palavra-chave de DM isoladas por conta;
- provider legado preservado para a automação experimental de novos seguidores.

### Variáveis Railway

```env
META_APP_ID=...
META_APP_SECRET=...
META_REDIRECT_URI=https://SEU-DOMINIO/meta/callback
META_WEBHOOK_VERIFY_TOKEN=uma-chave-forte-que-voce-inventa
META_GRAPH_VERSION=v26.0
META_VERIFY_SIGNATURE=1
```

Mantenha também `SECRET_KEY` e, de preferência, defina `FERNET_KEY` para criptografia dos tokens. Se `FERNET_KEY` não for informado, o projeto deriva a chave de `SECRET_KEY`.

### Configuração no painel da Meta

1. Crie/abra seu App da Meta e adicione a configuração da Instagram API com Instagram Login.
2. Cadastre exatamente a Redirect URI usada em `META_REDIRECT_URI`.
3. Configure o Callback URL do webhook como `https://SEU-DOMINIO/webhooks/instagram`.
4. Use em Verify Token exatamente o valor de `META_WEBHOOK_VERIFY_TOKEN`.
5. Assine os campos necessários no painel de Webhooks, principalmente `messages`, além dos eventos de comentários/messaging usados pelo seu produto.
6. Solicite/ative as permissões necessárias para o app, como `instagram_business_basic`, `instagram_business_manage_messages`, `instagram_business_manage_comments` e `instagram_business_content_publish`, de acordo com os módulos que serão colocados em produção.
7. No Welcome, abra **Configurações > Conectar Instagram** e autorize cada conta profissional.

### Tempo real

O navegador do painel não faz polling na conta do Instagram. A Meta chama o webhook quando uma DM chega; o Welcome salva o evento e a interface consulta apenas o próprio backend a cada 2 segundos para refletir eventos novos. Assim não há o botão “forçar atualização” no fluxo oficial e não são feitas consultas repetitivas à conta para descobrir mensagens.

### Observação sobre novo seguidor

O gatilho original `novo seguidor -> DM` continua isolado no provider legado. Não foi misturado ao webhook oficial de mensagens, evitando tratar esse gatilho como se fosse garantido universalmente pela API pública.
