# social_analysis


## Problema
Uma startup de análise de mídias sociais deseja criar um novo produto que ofereça insights sobre o engajamento e as conexões entre usuários em uma plataforma.
Eles precisam de um protótipo funcional que possa responder a perguntas complexas sobre interações de usuários, popularidade de conteúdo e comunidades de interesse.


## Descrição do DATASET

### Pushshift Reddit Dataset

O **Pushshift Reddit Dataset** é uma coleção abrangente de submissões e comentários do Reddit, coletados e disponibilizados publicamente pelo projeto Pushshift.

Os arquivos utilizados neste projeto são uma **amostra** do dataset original, disponibilizada no Zenodo ([DOI: 10.5281/zenodo.3608135](https://doi.org/10.5281/zenodo.3608135)), referente ao mês de abril de 2019:

| Arquivo | Tamanho | Descrição |
|---|---|---|
| `RS_2019-04.zst` | 5,6 GB | Todas as submissões (*posts*) publicadas no Reddit durante abril de 2019 |
| `RC_2019-04.zst` | 15,5 GB | Todos os comentários publicados no Reddit durante abril de 2019 |

> Os arquivos estão comprimidos no formato `.zst` (Zstandard). Os links originais do Pushshift (`files.pushshift.io`) parecem não estar mais disponíveis.

### Formato dos Arquivos

Cada arquivo segue o formato **NDJSON** (*Newline Delimited JSON*), onde cada linha representa um objeto JSON independente contendo os dados de uma submissão ou comentário.


## Preparação dos Dados — `prepara_data_01.py`

Script de pré-processamento que transforma os arquivos brutos do Pushshift em CSVs limpos e prontos para importação no Neo4j.

### Dependências

Execute dentro de seu virtualenv
```bash
pip install zstandard vaderSentiment psutil
```
ou
Execute na raiz do projeto:
```bash
$(poetry env activate) 
poetry install
```

### Arquivos de entrada

| Arquivo | Descrição |
|---|---|
| `RC_2019-04.zst` | Todos os comentários do Reddit em abril/2019 |
| `RS_2019-04.zst` | Todas as submissions (posts) do Reddit em abril/2019 |

### Arquivos de saída

| Arquivo | Descrição |
|---|---|
| `users.csv` | Usuários únicos encontrados no dataset |
| `submissions.csv` | Posts com `post_id`, `author`, `subreddit` e `score` |
| `user_relations.csv` | Relações entre usuários com `sentiment_sum` e `interaction_count` |

### Como funciona

O processamento ocorre em 6 etapas:

**0. Contagem de linhas** — conta o total de linhas de cada `.zst` para exibir o progresso percentual nas etapas seguintes. O resultado é cacheado em `_line_counts.json` e reutilizado em execuções futuras enquanto os arquivos não mudarem.

**1. Indexação das submissions** — lê o `RS`, filtra autores deletados/bots e indexa cada `post_id → author` num banco SQLite local (`_index.db`), gerando também o `submissions.csv`.

**2. Indexação dos comentários** — primeira passagem no `RC`, indexando `comment_id → author` no mesmo SQLite. Necessário para resolver respostas a outros comentários na etapa seguinte.

**3. Análise de sentimento** — segunda passagem no `RC`. O texto de cada comentário é enviado em lotes para um pool de workers paralelos que executam o VADER e retornam o `compound score` (-1.0 a +1.0). O processo principal resolve o autor do `parent_id` via SQLite e acumula a soma dos scores por par `(author → target_author)`.

**4 e 5. Exportação** — gera o `user_relations.csv` com o sentimento agregado e o `user_relations.csv` com todos os usuários únicos.

### Configurações relevantes

| Constante         | Descrição                                                                 |
|-------------------|---------------------------------------------------------------------------|
| `KEEP_DB`         | `True` mantém o `_index.db` após o processamento (padrão); `False` apaga  |
| `NUM_WORKERS`     | Calculado automaticamente: cores físicos menos 2 (se > 4)                 |
| `READ_BUFFER`     | Tamanho do buffer de leitura, escala com o número de workers              |
| `IGNORED_AUTHORS` | Autores filtrados: `[deleted]`, `[removed]`, `AutoModerator`              |

### Observações

- Os arquivos `.zst` são lidos como streams linha a linha, sem carregar o conteúdo inteiro na memória.
- O SQLite é usado apenas como índice auxiliar em disco — não é o destino final dos dados.
- O processamento pode levar várias horas dependendo do hardware, dado o volume do arquivo de comentários (15,5 GB comprimido).


## Refinamento dos dados — `prepare_data_02.py`

### Dependências
```bash
pip install numpy
```

### Como funciona

O script opera em 4 etapas sequenciais, todas processadas em streaming linha a linha para evitar carregar os arquivos inteiros na memória. O progresso é salvo em `_clean_state.json` após cada etapa, permitindo retomar a execução em caso de interrupção.

**1. Cálculo do threshold**
Faz uma passagem no `user_relations.csv` coletando os valores de `interaction_count` de todas as relações válidas (excluindo auto-relações) e calcula o percentil 5 desses valores. Esse valor vira o threshold mínimo para a etapa seguinte.

**2. Filtragem de relações**
Segunda passagem no `user_relations.csv` removendo auto-relações (`source_author == target_author`) e relações com `interaction_count` abaixo do threshold. O conjunto de autores que sobreviveram a essa filtragem é preservado para as etapas seguintes.

**3. Filtragem de submissions**
Percorre o `submissions.csv` removendo posts cujo autor não consta entre os autores válidos identificados na etapa anterior.

**4. Filtragem de usuários**
Percorre o `users.csv` removendo qualquer usuário que não apareça em nenhuma relação nem submission — garantindo que o grafo não contenha nós isolados.

### Arquivos temporários

Cada etapa escreve os resultados em um arquivo `.tmp` antes de substituir o original. Se o processo for interrompido no meio de uma escrita, o arquivo original permanece intacto e a etapa é refeita do zero na próxima execução.

### Resumo das remoções

Ao final, o script exibe um resumo com o threshold aplicado e a contagem final de registros em cada arquivo:
```
✅ Limpeza concluída!
   Threshold aplicado : interaction_count >= N
   users.csv          → X usuários
   submissions.csv    → Y posts
   user_relations.csv → Z relações
```
PS: disponibilizei os arquivos csv finais no Kaggle [aqui](https://www.kaggle.com/datasets/jaymetosineto/the-pushshift-reddit-dataset-csv).



