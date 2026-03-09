"""
analysis_02_content.py
=======================
Popularidade de conteúdo:
  - Top posts por score
  - Top subreddits por score médio por post
"""

import pandas as pd
from neo4j_base import (
    PALETTE,
    Neo4jConnection,
    bar_chart,
    is_done,
    load_state,
    mark_done,
)

TOP_N = 20

# Queries

Q_TOP_POSTS_BY_SCORE = """
MATCH (u:User)-[:POSTED]->(s:Submission)-[:BELONGS_TO]->(sr:Subreddit)
WHERE s.score > 0
RETURN u.username AS author, s.title AS title,
       sr.name AS subreddit, s.score AS score
ORDER BY score DESC
LIMIT $n
"""

Q_SUBREDDITS_BY_AVG_SCORE = """
MATCH (s:Submission)-[:BELONGS_TO]->(sr:Subreddit)
WHERE s.score >= 0
WITH sr.name AS subreddit, avg(s.score) AS avg_score, count(s) AS posts
WHERE posts >= 50
RETURN subreddit, round(avg_score, 2) AS avg_score, posts
ORDER BY avg_score DESC
LIMIT $n
"""

# Análises


def top_posts_by_score(conn):
    print("  Top posts por score...")
    rows = conn.query(Q_TOP_POSTS_BY_SCORE, {"n": TOP_N})
    df = pd.DataFrame(rows)
    df["label"] = (
        df["score"].astype(str) + "  r/" + df["subreddit"] + "  @" + df["author"]
    )
    bar_chart(
        labels=df["label"],
        values=df["score"],
        title=f"Top {TOP_N} Posts por Score",
        xlabel="Post",
        ylabel="Score",
        filename="02_top_posts_by_score.png",
        color=PALETTE[0],
        horizontal=True,
    )


def subreddits_by_avg_score(conn):
    print("  Top subreddits por score médio...")
    rows = conn.query(Q_SUBREDDITS_BY_AVG_SCORE, {"n": TOP_N})
    df = pd.DataFrame(rows)
    bar_chart(
        labels=df["subreddit"],
        values=df["avg_score"],
        title=f"Top {TOP_N} Subreddits por Score Médio por Post (mín. 50 posts)",
        xlabel="Subreddit",
        ylabel="Score médio",
        filename="02_subreddits_avg_score.png",
        color=PALETTE[1],
        horizontal=True,
    )


# Entry point

if __name__ == "__main__":
    print("[ analysis_02 ] Popularidade de Conteúdo")
    state = load_state()
    if is_done(state, "analysis_02"):
        print(
            "  Já executado, pulando. Apague output/_analysis_state.json para reexecutar."
        )
    else:
        with Neo4jConnection() as conn:
            if not is_done(state, "02_top_posts"):
                top_posts_by_score(conn)
                mark_done(state, "02_top_posts")
            if not is_done(state, "02_avg_score"):
                subreddits_by_avg_score(conn)
                mark_done(state, "02_avg_score")
        mark_done(state, "analysis_02")
    print("[ analysis_02 ] Concluído!")
