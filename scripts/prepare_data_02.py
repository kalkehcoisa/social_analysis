"""
Pushshift Reddit Data Cleaner
==============================
Limpa e valida os CSVs gerados pelo prepare_reddit_data.py antes
da importação no Neo4j.

Operações realizadas:
  1. Remove auto-relações e calcula threshold do percentil 5 de interaction_count
  2. Filtra relações abaixo do threshold e reconstrói users.csv e submissions.csv
  3. Remove usuários sem relações nem submissions
  4. Repassagem final de consistência

Uso:
  pip install numpy
  python clean_data.py
"""

import csv
import json
import os
import numpy as np

# Configuração 

DATASET_DIR      = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dataset'))
USERS_FILE       = os.path.join(DATASET_DIR, "users.csv")
SUBMISSIONS_FILE = os.path.join(DATASET_DIR, "submissions.csv")
RELATIONS_FILE   = os.path.join(DATASET_DIR, "user_relations.csv")
STATE_FILE       = os.path.join(DATASET_DIR, "_clean_state.json")

TMP_SUFFIX = ".tmp"

MIN_INTERACTION_PERCENTILE = 5

# Estado de execução 

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


def rebuild_valid_users(valid_users):
    # Reconstrói valid_users do arquivo já filtrado
    for row in iter_csv(RELATIONS_FILE):
        valid_users.add(row["source_author"])
        valid_users.add(row["target_author"])
    return valid_users


# Utilitários de streaming 

def iter_csv(filepath):
    """Itera um CSV linha a linha sem carregar tudo na memória."""
    with open(filepath, newline="", encoding="utf-8") as f:
        yield from csv.DictReader(f)


def atomic_replace(tmp_path, final_path):
    """Substitui o arquivo final pelo temporário atomicamente."""
    os.replace(tmp_path, final_path)

# Passos de limpeza 

def step_calc_threshold(state):
    """
    Passagem 1 no relations: coleta interaction_counts para calcular o percentil.
    Não escreve nada — apenas lê.
    """
    print("[ 1/4 ] Calculando threshold de interaction_count...")

    counts     = []
    self_loops = 0

    for row in iter_csv(RELATIONS_FILE):
        if row["source_author"] == row["target_author"]:
            self_loops += 1
            continue
        counts.append(int(row["interaction_count"]))

    threshold = int(np.percentile(counts, MIN_INTERACTION_PERCENTILE))
    print(f"         Auto-relações encontradas: {self_loops:,}")
    print(f"         Threshold (percentil {MIN_INTERACTION_PERCENTILE}): interaction_count >= {threshold}")
    print(f"         Relações válidas para análise: {len(counts):,}")

    mark_done(state, "step_1_threshold",
              threshold=threshold,
              self_loops=self_loops,
              total_valid=len(counts))
    return threshold


def step_filter_relations(state, threshold):
    """
    Passagem 2 no relations: filtra e escreve o arquivo limpo em streaming.
    Também coleta o conjunto de autores válidos.
    """
    print("[ 2/4 ] Filtrando relações e coletando autores válidos...")

    tmp_path    = RELATIONS_FILE + TMP_SUFFIX
    kept        = 0
    removed     = 0
    valid_users = set()

    with open(tmp_path, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(["source_author", "target_author", "sentiment_sum", "interaction_count"])

        for row in iter_csv(RELATIONS_FILE):
            # Auto-relações
            if row["source_author"] == row["target_author"]:
                removed += 1
                continue
            # Abaixo do threshold
            if int(row["interaction_count"]) < threshold:
                removed += 1
                continue

            writer.writerow([
                row["source_author"],
                row["target_author"],
                row["sentiment_sum"],
                row["interaction_count"],
            ])
            valid_users.add(row["source_author"])
            valid_users.add(row["target_author"])
            kept += 1

            if kept % 1_000_000 == 0:
                print(f"         {kept:,} relações mantidas...")

    atomic_replace(tmp_path, RELATIONS_FILE)
    print(f"         Relações mantidas: {kept:,} | removidas: {removed:,}")
    print(f"         Autores únicos nas relações: {len(valid_users):,}")

    mark_done(state, "step_2_relations", kept=kept, removed=removed)
    return valid_users


def step_filter_submissions(state, valid_users):
    """Filtra submissions em streaming, mantendo apenas autores em valid_users."""
    print("[ 3/4 ] Filtrando submissions...")

    tmp_path = SUBMISSIONS_FILE + TMP_SUFFIX
    kept     = 0
    removed  = 0
    sub_authors = set()

    with open(tmp_path, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(["post_id", "author", "subreddit", "score"])

        for row in iter_csv(SUBMISSIONS_FILE):
            if row["author"] not in valid_users:
                removed += 1
                continue
            writer.writerow([row["post_id"], row["author"], row["subreddit"], row["score"]])
            sub_authors.add(row["author"])
            kept += 1

            if kept % 1_000_000 == 0:
                print(f"         {kept:,} submissions mantidas...")

    atomic_replace(tmp_path, SUBMISSIONS_FILE)
    print(f"         Submissions mantidas: {kept:,} | removidas: {removed:,}")

    mark_done(state, "step_3_submissions", kept=kept, removed=removed)
    return sub_authors


def step_filter_users(state, valid_users):
    """Filtra users.csv em streaming, mantendo apenas usuários em valid_users."""
    print("[ 4/4 ] Filtrando users.csv...")

    tmp_path = USERS_FILE + TMP_SUFFIX
    kept     = 0
    removed  = 0

    with open(tmp_path, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(["username"])

        for row in iter_csv(USERS_FILE):
            if row["username"] not in valid_users:
                removed += 1
                continue
            writer.writerow([row["username"]])
            kept += 1

    atomic_replace(tmp_path, USERS_FILE)
    print(f"         Usuários mantidos: {kept:,} | removidos: {removed:,}")

    mark_done(state, "step_4_users", kept=kept, removed=removed)

# Entry point 

if __name__ == "__main__":
    state = load_state()
    valid_users = None

    if state:
        completed = [k for k, v in state.items() if v.get("done")]
        print(f"⚡ Retomando limpeza — etapas já concluídas: {', '.join(completed)}\n")

    # Passo 1: calcular threshold 
    if is_done(state, "step_1_threshold"):
        threshold = state["step_1_threshold"]["threshold"]
        print(f"[ 1/4 ] Threshold já calculado ({threshold}), pulando...")
    else:
        threshold = step_calc_threshold(state)

    # Passo 2: filtrar relações 
    if is_done(state, "step_2_relations"):
        print("[ 2/4 ] Relações já filtradas, pulando...")
    else:
        valid_users = step_filter_relations(state, threshold)

    # Passo 3: filtrar submissions 
    if is_done(state, "step_3_submissions"):
        print("[ 3/4 ] Submissions já filtradas, pulando...")
    else:
        if valid_users is None:
            valid_users = rebuild_valid_users(valid_users)
        sub_authors = step_filter_submissions(state, valid_users)
        valid_users = valid_users | sub_authors

    # Passo 4: filtrar usuários 
    if is_done(state, "step_4_users"):
        print("[ 4/4 ] Usuários já filtrados, pulando...")
    else:
        if valid_users is None:
            valid_users = rebuild_valid_users(valid_users)
        step_filter_users(state, valid_users)

    # Resumo 
    mark_done(state, "completed")
    print("\n✅ Limpeza concluída!")
    print(f"   Threshold aplicado : interaction_count >= {threshold}")
    print(f"   users.csv          → {state['step_4_users']['kept']:,} usuários")
    print(f"   submissions.csv    → {state['step_3_submissions']['kept']:,} posts")
    print(f"   user_relations.csv → {state['step_2_relations']['kept']:,} relações")
