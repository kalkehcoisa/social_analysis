"""
analysis_03_communities.py
===========================
Comunidades de interesse:
  - Detecção de comunidades via Louvain (GDS)
  - Tamanho das maiores comunidades
  - Subreddit dominante por comunidade
  - Usuários que fazem ponte entre comunidades — Betweenness Centrality (GDS)
  - Usuários ativos em múltiplos subreddits
  - Subreddits com maior similaridade de audiência (GDS Node Similarity)

Requer Neo4j GDS instalado.
"""

import json
import os

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from neo4j_base import (
    GDS_USER_INTERACTED,
    GDS_USER_SUBREDDIT,
    PALETTE,
    Neo4jConnection,
    bar_chart,
    drop_projection,
    ensure_user_interacted,
    ensure_user_subreddit,
    is_done,
    load_state,
    mark_done,
    save_chart,
)

TOP_N = 20

# Queries GDS

Q_GDS_LOUVAIN = """
CALL gds.louvain.write($graph, {
  writeProperty: 'community',
  relationshipWeightProperty: 'interaction_count'
})
YIELD communityCount, modularity
RETURN communityCount, modularity
"""

Q_TOP_CROSS_COMMUNITY_USERS = """
MATCH (u:User)-[:ACTIVE_IN]->(r:Subreddit)
WHERE u.community IS NOT NULL
WITH u.username AS user, u.community AS community, count(r) AS subreddits
ORDER BY subreddits DESC
LIMIT $n
RETURN user, community, subreddits
"""

Q_SUBREDDIT_OVERLAP = """
MATCH (r1:Subreddit)<-[:ACTIVE_IN]-(u:User)-[:ACTIVE_IN]->(r2:Subreddit)
WHERE id(r1) < id(r2)
RETURN r1.name AS subreddit_a, r2.name AS subreddit_b, count(u) AS shared_users
ORDER BY shared_users DESC
LIMIT $n
"""

# Queries Cypher

Q_COMMUNITY_SIZES = """
MATCH (u:User)
WHERE u.community IS NOT NULL
RETURN u.community AS community, count(u) AS size
ORDER BY size DESC
LIMIT $n
"""

Q_COMMUNITY_SUBREDDITS = """
MATCH (u:User)-[:ACTIVE_IN]->(r:Subreddit)
WHERE u.community IS NOT NULL
WITH u.community AS community, r.name AS subreddit, count(u) AS users
ORDER BY community, users DESC
WITH community, collect({subreddit: subreddit, users: users})[0] AS top
RETURN community, top.subreddit AS top_subreddit, top.users AS users
ORDER BY users DESC
LIMIT $n
"""

Q_USERS_MULTI_SUBREDDITS = """
MATCH (u:User)-[:ACTIVE_IN]->(r:Subreddit)
WITH u.username AS user, count(r) AS subreddits
WHERE subreddits > 1
RETURN subreddits, count(user) AS users
ORDER BY subreddits
"""

# Análises


def run_louvain(conn):
    print("  Detectando comunidades (GDS Louvain)...")
    result = conn.query(Q_GDS_LOUVAIN, {"graph": GDS_USER_INTERACTED})[0]
    print(f"  Comunidades detectadas : {result['communityCount']:,}")
    print(f"  Modularidade           : {result['modularity']:.4f}")


def community_sizes(conn):
    print("  Tamanho das maiores comunidades...")
    rows = conn.query(Q_COMMUNITY_SIZES, {"n": TOP_N})
    df = pd.DataFrame(rows)
    df["community"] = df["community"].astype(str)
    bar_chart(
        labels=df["community"],
        values=df["size"],
        title=f"Top {TOP_N} Maiores Comunidades (Louvain)",
        xlabel="Comunidade",
        ylabel="Usuários",
        filename="03_community_sizes.png",
        color=PALETTE[0],
        horizontal=True,
    )


def community_subreddits(conn):
    print("  Subreddit dominante por comunidade...")
    rows = conn.query(Q_COMMUNITY_SUBREDDITS, {"n": TOP_N})
    df = pd.DataFrame(rows)
    df["label"] = "C" + df["community"].astype(str) + " — " + df["top_subreddit"]
    bar_chart(
        labels=df["label"],
        values=df["users"],
        title=f"Top {TOP_N} Comunidades — Subreddit Dominante",
        xlabel="Comunidade",
        ylabel="Usuários no subreddit",
        filename="03_community_top_subreddit.png",
        color=PALETTE[1],
        horizontal=True,
    )


def top_cross_community_users(conn):
    print("  Top usuários ativos em mais subreddits...")
    rows = conn.query(Q_TOP_CROSS_COMMUNITY_USERS, {"n": TOP_N})
    df = pd.DataFrame(rows)
    df["label"] = df["user"] + " (C" + df["community"].astype(str) + ")"
    bar_chart(
        labels=df["label"],
        values=df["subreddits"],
        title=f"Top {TOP_N} Usuários Ativos em Mais Subreddits",
        xlabel="Usuário",
        ylabel="Número de subreddits",
        filename="03_top_cross_community_users.png",
        color=PALETTE[2],
        horizontal=True,
    )


def users_multi_subreddits(conn):
    print("  Usuários ativos em múltiplos subreddits...")
    rows = conn.query(Q_USERS_MULTI_SUBREDDITS)
    df = pd.DataFrame(rows)
    fig, ax = plt.subplots()
    ax.bar(df["subreddits"].astype(str), df["users"], color=PALETTE[3])
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax.set_title("Usuários por Número de Subreddits Ativos")
    ax.set_xlabel("Número de Subreddits")
    ax.set_ylabel("Número de Usuários")
    fig.tight_layout()
    save_chart(fig, "03_users_multi_subreddits.png")


def subreddit_overlap(conn):
    print("  Subreddits com maior sobreposição de usuários...")
    rows = conn.query(Q_SUBREDDIT_OVERLAP, {"n": TOP_N})
    df = pd.DataFrame(rows)
    df["pair"] = df["subreddit_a"] + " ↔ " + df["subreddit_b"]
    bar_chart(
        labels=df["pair"],
        values=df["shared_users"],
        title=f"Top {TOP_N} Pares de Subreddits com Maior Sobreposição de Usuários",
        xlabel="Par de Subreddits",
        ylabel="Usuários em comum",
        filename="03_subreddit_overlap.png",
        color=PALETTE[4],
        horizontal=True,
    )


# Entry point

if __name__ == "__main__":
    print("[ analysis_03 ] Comunidades de Interesse")
    state = load_state()
    if is_done(state, "analysis_03"):
        print(
            "  Já executado, pulando. Apague output/_analysis_state.json para reexecutar."
        )
    else:
        with Neo4jConnection() as conn:
            ensure_user_interacted(conn)
            ensure_user_subreddit(conn)
            if not is_done(state, "03_louvain"):
                run_louvain(conn)
                mark_done(state, "03_louvain")
            if not is_done(state, "03_community_sizes"):
                community_sizes(conn)
                mark_done(state, "03_community_sizes")
            if not is_done(state, "03_community_subreddits"):
                community_subreddits(conn)
                mark_done(state, "03_community_subreddits")
            if not is_done(state, "03_bridge_users"):
                top_cross_community_users(conn)
                mark_done(state, "03_bridge_users")
            if not is_done(state, "03_multi_subreddits"):
                users_multi_subreddits(conn)
                mark_done(state, "03_multi_subreddits")
            if not is_done(state, "03_overlap"):
                subreddit_overlap(conn)
                mark_done(state, "03_overlap")
            drop_projection(conn, GDS_USER_INTERACTED)
            drop_projection(conn, GDS_USER_SUBREDDIT)
        mark_done(state, "analysis_03")
    print("[ analysis_03 ] Concluído!")
