# dramio-lote — shards do GitHub Actions

Repo público, então os minutos de runner são ilimitados. Cada shard processa uma fatia dos
episódios (`matrix` de 100 no workflow).

## ⚠️ Mudança importante (13/10): os dados vêm do REPOSITÓRIO, não do Backblaze

O `lote_gh.py` baixava os JSON de `_dados/` no B2 **em cada um dos 100 runners**.
100 × ~70 MB = **~7 GB num dia** — e isso estourou a cota diária de download do Backblaze.
Resultado: **toda** leitura de arquivo passou a devolver 403 e o acervo ficou ilegível
(nem o player funcionava) até renovar.

Agora `pega_dados()` lê do próprio checkout e **baixa 0 bytes do B2** ✓

Os 4 arquivos que precisam estar **na raiz deste repositório** (já estão nesta pasta):

| arquivo | MB | para que serve |
|---|---|---|
| `links_unificado_num.json` | 34,3 | **o principal**: merge das 7 fontes, 3.640 books · 237.515 links, com as dublagens. Sem ele só o PT entra |
| `dramio_preencher.json` | 0,30 | a lista de dramas da Dramio (700 entradas · 559 book_ids · **433 drama_ids**) |
| `matriz_rapida.json` | 0,55 | idiomas por drama: `idiomas{lg:{book,is_dub,lang,original_lang,titulo}}` |
| `reelshort_catalog_pt_full.json` | 0,63 | catálogo PT (capa, contagem de capítulos) |

Total 35,8 MB. O maior é 34 MB — abaixo do limite de 100 MB por arquivo do GitHub ✓

⚠️ **Nunca** pôr `links_dub.json` (28 MB) aqui: ele **não** tem as dublagens novas e faria o
shard gerar episódio monolíngue.

## O que falta pra rodar

```
1. token do GitHub (o antigo ghp_9j55RWB… está revogado -> 403)
2. git push deste conteúdo pro repo lucaue007/dramio-lote
3. secrets B2_KEY_ID / B2_APP_KEY (já configurados)
4. disparar o workflow (matrix de 100 shards)
```

⚠️ Antes de disparar, conferir que o shard usa o `mapa_links_num()` (casa por **número** do
episódio, não por posição). **1.150 dos 3.640 books têm buraco na numeração** — por posição,
o áudio de um idioma cairia no episódio errado, silenciosamente. O `lote.py` da VPS já foi
corrigido; o `lote_gh.py` precisa da mesma correção antes de valer a pena disparar.

## Por que os shards importam

A VPS sozinha faz ~3,5 dias (PAR=8) para os 36.548 episódios. Com 100 shards rodando em
paralelo, o mesmo trabalho cai para poucas horas ✓
