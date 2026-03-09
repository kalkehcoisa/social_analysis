"""
prepare_data_04.py
==================
Cria índices e constraints no Neo4j para otimizar as queries de análise.
Os índices são do tipo RANGE, mais eficientes para filtros e ordenações
numéricas em dados estáticos (sem inserções após a importação).

Constraints (unicidade + índice implícito):
  User(username)
  Subreddit(name)
  Submission(post_id)

Índices RANGE em nós:
  User(community)       → WHERE u.community IS NOT NULL, ORDER BY community
  User(bot_suspect)     → WHERE u.bot_suspect IS NOT NULL
  Submission(score)     → WHERE s.score >= 0, ORDER BY score

Índices RANGE em relacionamentos:
  INTERACTED(sentiment_avg)     → WHERE r.sentiment_avg > 0 / < 0, ORDER BY
  INTERACTED(interaction_count) → ORDER BY, WHERE >= threshold, peso Louvain
  INTERACTED(sentiment_sum)     → ORDER BY sentiment_sum

Uso:
  python scripts/prepare_data_04.py
"""

import time
from neo4j_base import Neo4jConnection, NEO4J_DATABASE

# Definição dos índices 

CONSTRAINTS = [
    {
        "name":        "constraint_user_username",
        "cypher":      "CREATE CONSTRAINT constraint_user_username IF NOT EXISTS FOR (u:User) REQUIRE u.username IS UNIQUE",
        "description": "User(username) — unicidade + índice",
    },
    {
        "name":        "constraint_subreddit_name",
        "cypher":      "CREATE CONSTRAINT constraint_subreddit_name IF NOT EXISTS FOR (r:Subreddit) REQUIRE r.name IS UNIQUE",
        "description": "Subreddit(name) — unicidade + índice",
    },
    {
        "name":        "constraint_submission_post_id",
        "cypher":      "CREATE CONSTRAINT constraint_submission_post_id IF NOT EXISTS FOR (s:Submission) REQUIRE s.post_id IS UNIQUE",
        "description": "Submission(post_id) — unicidade + índice",
    },
]

NODE_INDEXES = [
    {
        "name":        "index_user_community",
        "cypher":      "CREATE INDEX index_user_community IF NOT EXISTS FOR (u:User) ON (u.community)",
        "description": "User(community) — filtro e agrupamento pós-Louvain",
    },
    {
        "name":        "index_user_bot_suspect",
        "cypher":      "CREATE INDEX index_user_bot_suspect IF NOT EXISTS FOR (u:User) ON (u.bot_suspect)",
        "description": "User(bot_suspect) — filtro de suspeitos",
    },
    {
        "name":        "index_submission_score",
        "cypher":      "CREATE INDEX index_submission_score IF NOT EXISTS FOR (s:Submission) ON (s.score)",
        "description": "Submission(score) — filtros e distribuição de score",
    },
]

REL_INDEXES = [
    {
        "name":        "index_interacted_sentiment_avg",
        "cypher":      "CREATE INDEX index_interacted_sentiment_avg IF NOT EXISTS FOR ()-[r:INTERACTED]-() ON (r.sentiment_avg)",
        "description": "INTERACTED(sentiment_avg) — filtros WHERE > 0 / < 0 e ORDER BY",
    },
    {
        "name":        "index_interacted_interaction_count",
        "cypher":      "CREATE INDEX index_interacted_interaction_count IF NOT EXISTS FOR ()-[r:INTERACTED]-() ON (r.interaction_count)",
        "description": "INTERACTED(interaction_count) — filtros de threshold, ORDER BY e peso do Louvain",
    },
    {
        "name":        "index_interacted_sentiment_sum",
        "cypher":      "CREATE INDEX index_interacted_sentiment_sum IF NOT EXISTS FOR ()-[r:INTERACTED]-() ON (r.sentiment_sum)",
        "description": "INTERACTED(sentiment_sum) — ORDER BY",
    },
]

Q_LIST_INDEXES = """
SHOW INDEXES
YIELD name, type, labelsOrTypes, properties, state
RETURN name, type, labelsOrTypes, properties, state
ORDER BY labelsOrTypes, properties
"""

Q_WAIT_FOR_INDEXES = "CALL db.awaitIndexes(600)"

# Funções 

def create_constraints(conn):
    print("[ 1/4 ] Criando constraints...")
    for c in CONSTRAINTS:
        print(f"  {c['description']}")
        conn.query(c["cypher"])
    print(f"  {len(CONSTRAINTS)} constraints criadas.")


def create_node_indexes(conn):
    print("[ 2/4 ] Criando índices em nós...")
    for idx in NODE_INDEXES:
        print(f"  {idx['description']}")
        conn.query(idx["cypher"])
    print(f"  {len(NODE_INDEXES)} índices criados.")


def create_rel_indexes(conn):
    print("[ 3/4 ] Criando índices em relacionamentos...")
    for idx in REL_INDEXES:
        print(f"  {idx['description']}")
        conn.query(idx["cypher"])
    print(f"  {len(REL_INDEXES)} índices criados.")


def wait_and_report(conn):
    print("[ 4/4 ] Aguardando indexação online (timeout: 600s)...")
    start = time.time()
    conn.query(Q_WAIT_FOR_INDEXES)
    elapsed = time.time() - start
    print(f"  Indexação concluída em {elapsed:.1f}s\n")

    def to_str(value):
        """Normaliza string ou lista para string separada por vírgula."""
        if isinstance(value, list):
            return ", ".join(value)
        return str(value) if value else ""

    print("  Índices ativos:")
    rows = conn.query(Q_LIST_INDEXES)
    col_name  = max(len(r["name"])                     for r in rows)
    col_type  = max(len(r["type"])                     for r in rows)
    col_label = max(len(to_str(r["labelsOrTypes"]))    for r in rows)
    for row in rows:
        labels = to_str(row["labelsOrTypes"])
        props  = to_str(row["properties"])
        print(
            f"  [{row['state']:6}]  "
            f"{row['name']:{col_name}}  "
            f"{row['type']:{col_type}}  "
            f"{labels:{col_label}}  ({props})"
        )


# Entry point 

if __name__ == "__main__":
    total = len(CONSTRAINTS) + len(NODE_INDEXES) + len(REL_INDEXES)
    print("[ prepare_data_04 ] Criando índices no Neo4j...")
    print(f"  Database : {NEO4J_DATABASE}")
    print(f"  Total    : {total} índices/constraints\n")

    with Neo4jConnection() as conn:
        create_constraints(conn)
        create_node_indexes(conn)
        create_rel_indexes(conn)
        wait_and_report(conn)

    print("\n✅ Índices prontos! Pode executar run_analysis.sh")
