"""
Pushshift Reddit Data Processor
================================
Lê RC_2019-04.zst (comentários) e RS_2019-04.zst (submissions),
roda análise de sentimento VADER e gera CSVs prontos para Neo4j.

Saída:
  - users.csv            → nós de usuário
  - submissions.csv      → nós de post
  - user_relations.csv   → arestas entre usuários (sentimento agregado)

Uso:
  pip install zstandard vaderSentiment psutil
  python prepare_reddit_data.py
"""

import csv
import json
import os
import sqlite3
from collections import defaultdict
from multiprocessing import Pool

import psutil
import zstandard as zstd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ── Configuração ──────────────────────────────────────────────────────────────

DATASET_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dataset'))
RC_FILE     = os.path.join(DATASET_DIR, "RC_2019-04.zst")
RS_FILE     = os.path.join(DATASET_DIR, "RS_2019-04.zst")
OUT_DIR     = DATASET_DIR
DB_FILE     = os.path.join(OUT_DIR, "_index.db")

LINE_COUNTS_CACHE = os.path.join(OUT_DIR, "_line_counts.json")
STATE_FILE        = os.path.join(OUT_DIR, "_state.json")

IGNORED_AUTHORS = {"[deleted]", "AutoModerator", "[removed]"}

BATCH_SIZE = 500  # linhas por lote enviado aos workers VADER

# True  → mantém o .db ao final (útil durante testes e depuração)
# False → apaga o .db ao final (comportamento de produção)
KEEP_DB = True

# Workers: reserva 2 cores para o SO se houver mais de 4
_physical_cores = psutil.cpu_count(logical=False) or 1
NUM_WORKERS     = max(1, _physical_cores - 2) if _physical_cores > 4 else _physical_cores

# Buffer de leitura: escala com workers até o limite de 4×16 MB
READ_BUFFER = 2 ** 24 * min(NUM_WORKERS, 4)

# ── Utilitários de I/O ────────────────────────────────────────────────────────

def open_zst(filepath):
    """Generator: descomprime .zst e itera linha a linha sem carregar na memória."""
    dctx = zstd.ZstdDecompressor(max_window_size=2 ** 31)
    with open(filepath, "rb") as fh:
        with dctx.stream_reader(fh, read_size=READ_BUFFER) as reader:
            buffer = b""
            while True:
                chunk = reader.read(READ_BUFFER)
                if not chunk:
                    if buffer:
                        yield buffer.decode("utf-8", errors="ignore")
                    break
                buffer += chunk
                lines  = buffer.split(b"\n")
                buffer = lines[-1]
                for line in lines[:-1]:
                    text = line.decode("utf-8", errors="ignore").strip()
                    if text:
                        yield text


def count_lines(filepath):
    """Conta linhas de um .zst sem extrair para disco."""
    print(f"         Contando linhas em {os.path.basename(filepath)}...")
    dctx  = zstd.ZstdDecompressor(max_window_size=2 ** 31)
    total = 0
    with open(filepath, "rb") as fh:
        with dctx.stream_reader(fh, read_size=READ_BUFFER) as reader:
            buffer = b""
            while True:
                chunk = reader.read(READ_BUFFER)
                if not chunk:
                    if buffer:
                        total += 1
                    break
                buffer += chunk
                total += buffer.count(b"\n")
                buffer = buffer[buffer.rfind(b"\n") + 1:]
    return total


def get_line_counts():
    """Retorna contagem de linhas do cache se os arquivos não mudaram, ou reconta."""
    if os.path.exists(LINE_COUNTS_CACHE):
        with open(LINE_COUNTS_CACHE) as f:
            cache = json.load(f)
        if (cache.get("rs_size") == os.path.getsize(RS_FILE) and
                cache.get("rc_size") == os.path.getsize(RC_FILE)):
            print(f"         Cache encontrado — RS: {cache['total_rs']:,} | RC: {cache['total_rc']:,}")
            return cache["total_rs"], cache["total_rc"]
        print("         Cache inválido (arquivo mudou), recontando...")

    total_rs = count_lines(RS_FILE)
    total_rc = count_lines(RC_FILE)
    with open(LINE_COUNTS_CACHE, "w") as f:
        json.dump({
            "total_rs": total_rs,
            "total_rc": total_rc,
            "rs_size":  os.path.getsize(RS_FILE),
            "rc_size":  os.path.getsize(RC_FILE),
        }, f)
    print("         Contagem salva em cache.")
    return total_rs, total_rc

# ── Estado de execução ───────────────────────────────────────────────────────

def load_state():
    """Carrega o estado de execução salvo, ou retorna estado vazio."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def mark_done(state, step, **metadata):
    """Marca uma etapa como concluída e persiste o estado."""
    state[step] = {"done": True, **metadata}
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def is_done(state, step):
    return state.get(step, {}).get("done", False)

# ── SQLite ────────────────────────────────────────────────────────────────────

def setup_db(conn):
    """Cria tabelas de índice para submissions e comentários."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS submissions (
            post_id TEXT PRIMARY KEY,
            author  TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS comments (
            comment_id TEXT PRIMARY KEY,
            author     TEXT NOT NULL
        );
    """)
    conn.commit()


def lookup_author(conn, prefix, parent_id_clean):
    """Resolve o autor do pai a partir do prefixo (t1 = comentário, t3 = post)."""
    table  = "submissions" if prefix == "t3" else "comments"
    column = "post_id"     if prefix == "t3" else "comment_id"
    row    = conn.execute(
        f"SELECT author FROM {table} WHERE {column} = ?", (parent_id_clean,)
    ).fetchone()
    return row[0] if row else None

# ── VADER (executado nos worker processes) ────────────────────────────────────

def process_batch(batch):
    """Roda VADER em cada item do lote. Cada worker instancia seu próprio analyzer."""
    analyzer = SentimentIntensityAnalyzer()
    return [
        (author, prefix, parent_id_clean, analyzer.polarity_scores(body)["compound"])
        for author, body, prefix, parent_id_clean in batch
    ]

# ── Passos do pipeline ────────────────────────────────────────────────────────

def step_count_lines():
    print("[ 0/5 ] Contando linhas dos arquivos (para progresso)...")
    total_rs, total_rc = get_line_counts()
    print(f"         RS: {total_rs:,} submissions | RC: {total_rc:,} comentários")
    return total_rs, total_rc


def step_index_submissions(conn, total_rs):
    print("[ 1/5 ] Indexando submissions (RS) no SQLite...")
    sub_count = 0
    sub_users = set()
    batch     = []

    with open(os.path.join(OUT_DIR, "submissions.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["post_id", "author", "subreddit", "score"])

        for i, line in enumerate(open_zst(RS_FILE)):
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            author  = obj.get("author", "")
            post_id = obj.get("id", "")
            if not post_id or not author or author in IGNORED_AUTHORS:
                continue

            writer.writerow([post_id, author, obj.get("subreddit", ""), obj.get("score", 0)])
            batch.append((post_id, author))
            sub_users.add(author)
            sub_count += 1

            if len(batch) >= 50_000:
                conn.executemany("INSERT OR IGNORE INTO submissions VALUES (?, ?)", batch)
                conn.commit()
                batch.clear()

            if (i + 1) % 500_000 == 0:
                print(f"         {i+1:,} / {total_rs:,} ({(i+1)/total_rs*100:.1f}%)...")

        if batch:
            conn.executemany("INSERT OR IGNORE INTO submissions VALUES (?, ?)", batch)
            conn.commit()

    print(f"         {sub_count:,} submissions indexadas.")
    return sub_count, sub_users


def step_index_comments(conn, total_rc):
    print("[ 2/5 ] Indexando comentários (RC) no SQLite (1ª passagem)...")
    comment_count = 0
    batch         = []

    for i, line in enumerate(open_zst(RC_FILE)):
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue

        author     = obj.get("author", "")
        comment_id = obj.get("id", "")
        if not comment_id or not author or author in IGNORED_AUTHORS:
            continue

        batch.append((comment_id, author))
        comment_count += 1

        if len(batch) >= 50_000:
            conn.executemany("INSERT OR IGNORE INTO comments VALUES (?, ?)", batch)
            conn.commit()
            batch.clear()

        if (i + 1) % 1_000_000 == 0:
            print(f"         {i+1:,} / {total_rc:,} ({(i+1)/total_rc*100:.1f}%)...")

    if batch:
        conn.executemany("INSERT OR IGNORE INTO comments VALUES (?, ?)", batch)
        conn.commit()

    print("         Criando índices no SQLite...")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sub ON submissions(post_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_com ON comments(comment_id)")
    conn.commit()
    print(f"         {comment_count:,} comentários indexados.")


def iter_batches():
    """Generator: lê o RC e agrupa linhas válidas em lotes para os workers."""
    batch = []
    for line in open_zst(RC_FILE):
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue

        author    = obj.get("author", "")
        body      = obj.get("body", "")
        parent_id = obj.get("parent_id", "")

        if not author or author in IGNORED_AUTHORS:
            continue
        if not body or body in ("[deleted]", "[removed]"):
            continue
        if not parent_id or len(parent_id) < 4:
            continue

        prefix = parent_id[:2]
        if prefix not in ("t1", "t3"):
            continue

        batch.append((author, body, prefix, parent_id[3:]))
        if len(batch) >= BATCH_SIZE:
            yield batch
            batch = []
    if batch:
        yield batch


def step_process_sentiment(conn, total_rc):
    print(f"[ 3/5 ] Processando sentimento e relações (2ª passagem RC) com {NUM_WORKERS} workers...")
    relations       = defaultdict(lambda: [0.0, 0])
    comment_users   = set()
    total_processed = 0

    with Pool(processes=NUM_WORKERS) as pool:
        for batch_results in pool.imap_unordered(process_batch, iter_batches(), chunksize=10):
            for author, prefix, parent_id_clean, compound in batch_results:
                target = lookup_author(conn, prefix, parent_id_clean)
                if not target or target in IGNORED_AUTHORS or target == author:
                    continue
                relations[(author, target)][0] += compound
                relations[(author, target)][1] += 1
                comment_users.add(author)

            total_processed += len(batch_results)
            if total_processed % 1_000_000 < BATCH_SIZE:
                pct = total_processed / total_rc * 100
                print(f"         {total_processed:,} / {total_rc:,} ({pct:.1f}%) | {len(relations):,} pares únicos...")

    print(f"         {len(relations):,} pares de usuários gerados.")
    return relations, comment_users


def step_export_relations(relations):
    print("[ 4/5 ] Exportando user_relations.csv...")
    with open(os.path.join(OUT_DIR, "user_relations.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["source_author", "target_author", "sentiment_sum", "interaction_count"])
        for (src, tgt), (s_sum, count) in relations.items():
            writer.writerow([src, tgt, round(s_sum, 4), count])
    print("         Exportado.")


def step_export_users(sub_users, comment_users, relations):
    print("[ 5/5 ] Exportando users.csv...")
    all_users = sub_users | comment_users
    for src, tgt in relations:
        all_users.add(src)
        all_users.add(tgt)

    with open(os.path.join(OUT_DIR, "users.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["username"])
        for user in sorted(all_users):
            writer.writerow([user])

    print(f"         {len(all_users):,} usuários únicos exportados.")
    return all_users

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    os.makedirs(OUT_DIR, exist_ok=True)
    state = load_state()

    if state:
        completed = [k for k, v in state.items() if v.get("done")]
        print(f"⚡ Retomando execução — etapas já concluídas: {', '.join(completed)}")

    # ── Passo 0: contagem de linhas ───────────────────────────────────────────
    total_rs, total_rc = step_count_lines()

    # ── Conexão SQLite ────────────────────────────────────────────────────────
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    setup_db(conn)

    # ── Passo 1: indexar submissions ──────────────────────────────────────────
    if is_done(state, "step_1_submissions"):
        print("[ 1/5 ] Submissions já indexadas, pulando...")
        sub_count = state["step_1_submissions"]["sub_count"]
        sub_users = set()  # reconstruído no passo 5 a partir dos CSVs
    else:
        sub_count, sub_users = step_index_submissions(conn, total_rs)
        mark_done(state, "step_1_submissions", sub_count=sub_count)

    # ── Passo 2: indexar comentários ──────────────────────────────────────────
    if is_done(state, "step_2_comments"):
        print("[ 2/5 ] Comentários já indexados, pulando...")
    else:
        step_index_comments(conn, total_rc)
        mark_done(state, "step_2_comments")

    # ── Passo 3: sentimento ───────────────────────────────────────────────────
    if is_done(state, "step_3_sentiment"):
        print("[ 3/5 ] Sentimento já processado, pulando...")
        # Recarrega relações do CSV para os passos seguintes
        relations     = defaultdict(lambda: [0.0, 0])
        comment_users = set()
        with open(os.path.join(OUT_DIR, "user_relations.csv"), newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                key = (row["source_author"], row["target_author"])
                relations[key] = [float(row["sentiment_sum"]), int(row["interaction_count"])]
                comment_users.add(row["source_author"])
    else:
        relations, comment_users = step_process_sentiment(conn, total_rc)
        mark_done(state, "step_3_sentiment", pairs=len(relations))

    conn.close()

    # ── Passo 4: exportar relações ────────────────────────────────────────────
    if is_done(state, "step_4_relations"):
        print("[ 4/5 ] user_relations.csv já exportado, pulando...")
    else:
        step_export_relations(relations)
        mark_done(state, "step_4_relations")

    # ── Passo 5: exportar usuários ────────────────────────────────────────────
    if is_done(state, "step_5_users"):
        print("[ 5/5 ] users.csv já exportado, pulando...")
        all_users = set()
        with open(os.path.join(OUT_DIR, "users.csv"), newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                all_users.add(row["username"])
    else:
        # Reconstrói sub_users do CSV se o passo 1 foi pulado
        if not sub_users:
            with open(os.path.join(OUT_DIR, "submissions.csv"), newline="", encoding="utf-8") as f:
                sub_users = {row["author"] for row in csv.DictReader(f)}
        all_users = step_export_users(sub_users, comment_users, relations)
        mark_done(state, "step_5_users", user_count=len(all_users))

    # ── Limpeza ───────────────────────────────────────────────────────────────
    if KEEP_DB:
        print(f"\n         Banco de índices mantido em: {os.path.abspath(DB_FILE)}")
        print("         (defina KEEP_DB = False para apagá-lo automaticamente)")
    else:
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)
        print("\n         Banco temporário removido.")

    mark_done(state, "completed")
    print("\n✅ Concluído! Arquivos gerados em:", os.path.abspath(OUT_DIR))
    print(f"   users.csv          → {len(all_users):,} usuários")
    print(f"   submissions.csv    → {sub_count:,} posts")
    print(f"   user_relations.csv → {len(relations):,} relações entre usuários")
