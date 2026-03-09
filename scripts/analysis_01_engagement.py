"""
analysis_01_engagement.py
==========================
Engajamento:
  - Top usuários por volume de interações enviadas (GDS degree weighted)
  - Top usuários por volume de interações recebidas (GDS degree weighted)
  - Top usuários por influência — PageRank (GDS)
  - Top subreddits por volume total de interações
  - Distribuição de score dos posts
"""

import json
import os

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

# Estado de execução

STATE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "output", "_analysis_state.json"
)


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def mark_done(state, step, **metadata):
    state[step] = {"done": True, **metadata}
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def is_done(state, step):
    return state.get(step, {}).get("done", False)


from neo4j_base import (
    GDS_USER_INTERACTED,
    PALETTE,
    Neo4jConnection,
    bar_chart,
    ensure_user_interacted,
    save_chart,
)

TOP_N = 20

# Queries GDS

Q_GDS_OUT_DEGREE_WEIGHTED = """
CALL gds.degree.stream($graph, {
  orientation: 'NATURAL',
  relationshipWeightProperty: 'interaction_count'
})
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS u, score
WHERE score > 0
RETURN u.username AS user, toInteger(score) AS total_sent
ORDER BY total_sent DESC
LIMIT $n
"""

Q_GDS_IN_DEGREE_WEIGHTED = """
CALL gds.degree.stream($graph, {
  orientation: 'REVERSE',
  relationshipWeightProperty: 'interaction_count'
})
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS u, score
WHERE score > 0
RETURN u.username AS user, toInteger(score) AS total_received
ORDER BY total_received DESC
LIMIT $n
"""

Q_GDS_PAGERANK = """
CALL gds.pageRank.stream($graph, {
  relationshipWeightProperty: 'interaction_count',
  dampingFactor: 0.85,
  maxIterations: 20
})
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS u, score
RETURN u.username AS user, round(score, 4) AS pagerank
ORDER BY pagerank DESC
LIMIT $n
"""

# Queries Cypher

Q_TOP_SUBREDDITS_BY_INTERACTIONS = """
MATCH (u:User)-[r:INTERACTED]->(:User)-[:POSTED]->(:Submission)-[:BELONGS_TO]->(sr:Subreddit)
RETURN sr.name AS subreddit, sum(r.interaction_count) AS total_interactions
ORDER BY total_interactions DESC
LIMIT $n
"""

Q_SCORE_DISTRIBUTION = """
MATCH (s:Submission)
WHERE s.score >= 0
RETURN
  CASE
    WHEN s.score = 0     THEN '0'
    WHEN s.score <= 5    THEN '1-5'
    WHEN s.score <= 10   THEN '6-10'
    WHEN s.score <= 50   THEN '11-50'
    WHEN s.score <= 100  THEN '51-100'
    WHEN s.score <= 500  THEN '101-500'
    WHEN s.score <= 1000 THEN '501-1k'
    ELSE '1k+'
  END AS bucket,
  count(*) AS total
ORDER BY min(s.score)
"""

# Análises


def top_users_by_sent(conn):
    print("  Top usuários por interações enviadas (GDS weighted degree)...")
    rows = conn.query(
        Q_GDS_OUT_DEGREE_WEIGHTED, {"graph": GDS_USER_INTERACTED, "n": TOP_N}
    )
    df = pd.DataFrame(rows)
    bar_chart(
        labels=df["user"],
        values=df["total_sent"],
        title=f"Top {TOP_N} Usuários por Interações Enviadas",
        xlabel="Usuário",
        ylabel="Total de interações enviadas",
        filename="01_top_users_sent.png",
        color=PALETTE[0],
        horizontal=True,
    )


def top_users_by_received(conn):
    print("  Top usuários por interações recebidas (GDS weighted degree)...")
    rows = conn.query(
        Q_GDS_IN_DEGREE_WEIGHTED, {"graph": GDS_USER_INTERACTED, "n": TOP_N}
    )
    df = pd.DataFrame(rows)
    bar_chart(
        labels=df["user"],
        values=df["total_received"],
        title=f"Top {TOP_N} Usuários por Interações Recebidas",
        xlabel="Usuário",
        ylabel="Total de interações recebidas",
        filename="01_top_users_received.png",
        color=PALETTE[1],
        horizontal=True,
    )


def top_users_by_pagerank(conn):
    print("  Top usuários por influência (GDS PageRank)...")
    rows = conn.query(Q_GDS_PAGERANK, {"graph": GDS_USER_INTERACTED, "n": TOP_N})
    df = pd.DataFrame(rows)
    bar_chart(
        labels=df["user"],
        values=df["pagerank"],
        title=f"Top {TOP_N} Usuários por Influência (PageRank)",
        xlabel="Usuário",
        ylabel="PageRank",
        filename="01_top_users_pagerank.png",
        color=PALETTE[2],
        horizontal=True,
    )


def top_subreddits_by_interactions(conn):
    print("  Top subreddits por volume de interações...")
    rows = conn.query(Q_TOP_SUBREDDITS_BY_INTERACTIONS, {"n": TOP_N})
    df = pd.DataFrame(rows)
    bar_chart(
        labels=df["subreddit"],
        values=df["total_interactions"],
        title=f"Top {TOP_N} Subreddits por Volume de Interações",
        xlabel="Subreddit",
        ylabel="Total de interações",
        filename="01_top_subreddits_interactions.png",
        color=PALETTE[3],
        horizontal=True,
    )


def score_distribution(conn):
    print("  Distribuição de score dos posts...")
    rows = conn.query(Q_SCORE_DISTRIBUTION)
    df = pd.DataFrame(rows)
    fig, ax = plt.subplots()
    ax.bar(df["bucket"], df["total"], color=PALETTE[4])
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax.set_title("Distribuição de Score dos Posts")
    ax.set_xlabel("Faixa de Score")
    ax.set_ylabel("Número de Posts")
    fig.tight_layout()
    save_chart(fig, "01_score_distribution.png")


# Entry point

if __name__ == "__main__":
    print("[ analysis_01 ] Engajamento")
    state = load_state()
    if is_done(state, "analysis_01"):
        print(
            "  Já executado, pulando. Apague output/_analysis_state.json para reexecutar."
        )
    else:
        with Neo4jConnection() as conn:
            ensure_user_interacted(conn)
            if not is_done(state, "01_users_sent"):
                top_users_by_sent(conn)
                mark_done(state, "01_users_sent")
            if not is_done(state, "01_users_received"):
                top_users_by_received(conn)
                mark_done(state, "01_users_received")
            if not is_done(state, "01_pagerank"):
                top_users_by_pagerank(conn)
                mark_done(state, "01_pagerank")
            if not is_done(state, "01_subreddits"):
                top_subreddits_by_interactions(conn)
                mark_done(state, "01_subreddits")
            if not is_done(state, "01_score_dist"):
                score_distribution(conn)
                mark_done(state, "01_score_dist")
        mark_done(state, "analysis_01")
    print("[ analysis_01 ] Concluído!")
