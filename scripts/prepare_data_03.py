"""
Neo4j Import Preparation
=========================
Adapta os CSVs limpos para o formato esperado pelo neo4j-admin import
e gera os arquivos de arestas derivados.

Arquivos gerados em neo4j_import/:
  Nós:
    neo4j_users.csv         → (:User)
    neo4j_submissions.csv   → (:Submission)
    neo4j_subreddits.csv    → (:Subreddit)  — extraído do submissions.csv

  Arestas:
    neo4j_interacted.csv    → (:User)-[:INTERACTED]->(:User)
    neo4j_posted.csv        → (:User)-[:POSTED]->(:Submission)
    neo4j_belongs_to.csv    → (:Submission)-[:BELONGS_TO]->(:Subreddit)
    neo4j_active_in.csv     → (:User)-[:ACTIVE_IN]->(:Subreddit)

  Script:
    import_command.sh       → comando neo4j-admin pronto para executar

Uso:
  python prepare_neo4j_import.py
  Depois execute: bash neo4j_import/import_command.sh
"""

import csv
import json
import os
from collections import defaultdict

# ── Configuração ──────────────────────────────────────────────────────────────

DATASET_DIR      = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dataset'))
NEO4J_DIR        = os.path.join(DATASET_DIR, "neo4j_import")
STATE_FILE       = os.path.join(NEO4J_DIR, "_neo4j_state.json")

USERS_FILE       = os.path.join(DATASET_DIR, "users.csv")
SUBMISSIONS_FILE = os.path.join(DATASET_DIR, "submissions.csv")
RELATIONS_FILE   = os.path.join(DATASET_DIR, "user_relations.csv")

# Nome do banco Neo4j de destino
NEO4J_DATABASE = "neo4j"

# Credenciais Neo4j
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "neo4j123"

# Portas Neo4j
NEO4J_HTTP_PORT = 7474
NEO4J_BOLT_PORT = 7687

# Versão da imagem Docker
NEO4J_IMAGE = "neo4j:5"

# Diretório local com os CSVs gerados por este script
NEO4J_IMPORT_DIR = os.path.join(os.path.expanduser("~"), "neo4j-data", "import")

# Diretório local para os dados do banco
NEO4J_DATA_DIR = os.path.join(os.path.expanduser("~"), "neo4j-data", "data")

# ── Estado de execução ────────────────────────────────────────────────────────

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def mark_done(state, step, **metadata):
    state[step] = {"done": True, **metadata}
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def is_done(state, step):
    return state.get(step, {}).get("done", False)

# ── Utilitários ───────────────────────────────────────────────────────────────

def iter_csv(filepath):
    """Itera um CSV linha a linha sem carregar tudo na memória."""
    with open(filepath, newline="", encoding="utf-8") as f:
        yield from csv.DictReader(f)


def open_writer(filepath, fieldnames):
    f      = open(filepath, "w", newline="", encoding="utf-8")
    writer = csv.writer(f)
    writer.writerow(fieldnames)
    return f, writer

# ── Passos ────────────────────────────────────────────────────────────────────

def step_nodes_users(state):
    print("[ 1/5 ] Gerando neo4j_users.csv...")
    f, writer = open_writer(
        os.path.join(NEO4J_DIR, "neo4j_users.csv"),
        ["username:ID(User)"]
    )
    count = 0
    for row in iter_csv(USERS_FILE):
        writer.writerow([row["username"]])
        count += 1
    f.close()

    print(f"         {count:,} usuários exportados.")
    mark_done(state, "step_1_users", count=count)


def step_nodes_submissions_and_derived(state):
    """
    Passagem única no submissions.csv para gerar todos os arquivos derivados:
      - neo4j_submissions.csv   (nós)
      - neo4j_subreddits.csv    (nós — extraídos e deduplicados aqui)
      - neo4j_posted.csv        (User-[:POSTED]->Submission)
      - neo4j_belongs_to.csv    (Submission-[:BELONGS_TO]->Subreddit)
      - neo4j_active_in.csv     (User-[:ACTIVE_IN]->Subreddit)
    """
    print("[ 2/5 ] Gerando submissions, subreddits e arestas derivadas...")

    f_sub, w_sub = open_writer(
        os.path.join(NEO4J_DIR, "neo4j_submissions.csv"),
        ["post_id:ID(Submission)", "score:INT"]
    )
    f_srd, w_srd = open_writer(
        os.path.join(NEO4J_DIR, "neo4j_subreddits.csv"),
        ["name:ID(Subreddit)"]
    )
    f_pos, w_pos = open_writer(
        os.path.join(NEO4J_DIR, "neo4j_posted.csv"),
        [":START_ID(User)", ":END_ID(Submission)"]
    )
    f_bel, w_bel = open_writer(
        os.path.join(NEO4J_DIR, "neo4j_belongs_to.csv"),
        [":START_ID(Submission)", ":END_ID(Subreddit)"]
    )
    f_act, w_act = open_writer(
        os.path.join(NEO4J_DIR, "neo4j_active_in.csv"),
        [":START_ID(User)", ":END_ID(Subreddit)"]
    )

    subreddits_seen = set()
    active_in_seen  = set()
    counts          = defaultdict(int)

    for row in iter_csv(SUBMISSIONS_FILE):
        author    = row["author"]
        post_id   = row["post_id"]
        subreddit = row["subreddit"]

        # Nó Submission
        w_sub.writerow([post_id, row["score"]])
        counts["submissions"] += 1

        # Nó Subreddit (deduplica)
        if subreddit not in subreddits_seen:
            w_srd.writerow([subreddit])
            subreddits_seen.add(subreddit)

        # Aresta POSTED
        w_pos.writerow([author, post_id])

        # Aresta BELONGS_TO
        w_bel.writerow([post_id, subreddit])

        # Aresta ACTIVE_IN (deduplica por par author+subreddit)
        key = (author, subreddit)
        if key not in active_in_seen:
            w_act.writerow([author, subreddit])
            active_in_seen.add(key)

        if counts["submissions"] % 1_000_000 == 0:
            print(f"         {counts['submissions']:,} submissions processadas...")

    for f in (f_sub, f_srd, f_pos, f_bel, f_act):
        f.close()

    print(f"         Submissions    : {counts['submissions']:,}")
    print(f"         Subreddits     : {len(subreddits_seen):,}")
    print(f"         POSTED         : {counts['submissions']:,}")
    print(f"         BELONGS_TO     : {counts['submissions']:,}")
    print(f"         ACTIVE_IN      : {len(active_in_seen):,}")

    mark_done(state, "step_2_derived",
              submissions=counts["submissions"],
              subreddits=len(subreddits_seen),
              active_in=len(active_in_seen))


def step_edges_interacted(state):
    print("[ 3/5 ] Gerando neo4j_interacted.csv...")
    f, writer = open_writer(
        os.path.join(NEO4J_DIR, "neo4j_interacted.csv"),
        [":START_ID(User)", ":END_ID(User)", "sentiment_sum:FLOAT", "interaction_count:INT", "sentiment_avg:FLOAT"]
    )
    count = 0
    for row in iter_csv(RELATIONS_FILE):
        s_sum   = float(row["sentiment_sum"])
        i_count = int(row["interaction_count"])
        s_avg   = round(s_sum / i_count, 4) if i_count > 0 else 0.0
        writer.writerow([row["source_author"], row["target_author"], round(s_sum, 4), i_count, s_avg])
        count += 1

        if count % 1_000_000 == 0:
            print(f"         {count:,} relações exportadas...")

    f.close()
    print(f"         {count:,} relações exportadas.")
    mark_done(state, "step_3_interacted", count=count)


def step_generate_import_command(state):
    print("[ 4/5 ] Gerando import_command.sh...")

    imp = "/import"
    cmd = """\
#!/bin/bash
# Comando gerado automaticamente por prepare_neo4j_import.py
#
# Pré-requisitos:
#   1. Os CSVs devem estar em {import_dir}
#   2. O container Neo4j deve estar parado
#
# Uso:
#   bash import_command.sh

set -e

echo ">>> Iniciando importação..."

docker run --rm \\
  -v "{import_dir}:/import" \\
  -v "{data_dir}:/data" \\
  {image} \\
  neo4j-admin database import full \\
    --nodes=User="{imp}/neo4j_users.csv" \\
    --nodes=Submission="{imp}/neo4j_submissions.csv" \\
    --nodes=Subreddit="{imp}/neo4j_subreddits.csv" \\
    --relationships=INTERACTED="{imp}/neo4j_interacted.csv" \\
    --relationships=POSTED="{imp}/neo4j_posted.csv" \\
    --relationships=BELONGS_TO="{imp}/neo4j_belongs_to.csv" \\
    --relationships=ACTIVE_IN="{imp}/neo4j_active_in.csv" \\
    --delimiter="," \\
    --ignore-empty-strings=true \\
    --bad-tolerance=0 \\
    --overwrite-destination \\
    --verbose \\
    {database}

echo ">>> Importação concluída! Inicie o Neo4j com neo4j-docker.sh"
""".format(
        import_dir=NEO4J_IMPORT_DIR,
        data_dir=NEO4J_DATA_DIR,
        image=NEO4J_IMAGE,
        imp=imp,
        database=NEO4J_DATABASE,
    )

    sh_path = os.path.join(NEO4J_DIR, "import_command.sh")
    with open(sh_path, "w", encoding="utf-8") as f:
        f.write(cmd)
    print(f"         Salvo em: {sh_path}")
    mark_done(state, "step_4_command")


def step_print_summary(state):
    print("\n[ 5/5 ] Resumo:")
    print(f"   neo4j_users.csv       → {state['step_1_users']['count']:,} nós User")
    print(f"   neo4j_submissions.csv → {state['step_2_derived']['submissions']:,} nós Submission")
    print(f"   neo4j_subreddits.csv  → {state['step_2_derived']['subreddits']:,} nós Subreddit")
    print(f"   neo4j_interacted.csv  → {state['step_3_interacted']['count']:,} arestas INTERACTED")
    print(f"   neo4j_posted.csv      → {state['step_2_derived']['submissions']:,} arestas POSTED")
    print(f"   neo4j_belongs_to.csv  → {state['step_2_derived']['submissions']:,} arestas BELONGS_TO")
    print(f"   neo4j_active_in.csv   → {state['step_2_derived']['active_in']:,} arestas ACTIVE_IN")
    print(f"\n   ⚠️  Copie os CSVs para {NEO4J_IMPORT_DIR} antes de executar o import_command.sh")

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    os.makedirs(NEO4J_DIR, exist_ok=True)
    state = load_state()

    if state:
        completed = [k for k, v in state.items() if v.get("done")]
        print(f"⚡ Retomando — etapas já concluídas: {', '.join(completed)}\n")

    if is_done(state, "step_1_users"):
        print("[ 1/5 ] neo4j_users.csv já gerado, pulando...")
    else:
        step_nodes_users(state)

    if is_done(state, "step_2_derived"):
        print("[ 2/5 ] Submissions, subreddits e arestas derivadas já geradas, pulando...")
    else:
        step_nodes_submissions_and_derived(state)

    if is_done(state, "step_3_interacted"):
        print("[ 3/5 ] neo4j_interacted.csv já gerado, pulando...")
    else:
        step_edges_interacted(state)

    if is_done(state, "step_4_command"):
        print("[ 4/5 ] import_command.sh já gerado, pulando...")
    else:
        step_generate_import_command(state)

    mark_done(state, "completed")
    step_print_summary(state)
