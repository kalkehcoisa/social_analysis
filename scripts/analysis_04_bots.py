"""
analysis_04_bots.py
====================
Qualidade dos dados — detecção de possíveis bots:
  - Volume anormal de interações enviadas (GDS weighted degree + IQR)
  - Sentimento sempre neutro (|sentiment_avg| < threshold)
  - Ratio interaction_count/unique_targets muito alto
  - Marca suspeitos com bot_suspect = true no Neo4j

Executar antes das análises principais para que os resultados
não sejam distorcidos por bots.
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
    ensure_user_interacted,
    save_chart,
)

NEUTRAL_THRESHOLD = 0.05
MIN_INTERACTIONS = 50
IQR_MULTIPLIER = 3.0

# Queries

Q_GDS_WEIGHTED_DEGREE = """
CALL gds.degree.stream($graph, {
  orientation: 'NATURAL',
  relationshipWeightProperty: 'interaction_count'
})
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS u, score
WHERE score >= $min_interactions
RETURN u.username AS user, toInteger(score) AS total_sent
ORDER BY total_sent DESC
"""

Q_UNIQUE_TARGETS = """
MATCH (u:User)-[r:INTERACTED]->()
WITH u.username AS user, count(r) AS unique_targets
RETURN user, unique_targets
"""

Q_NEUTRAL_SENTIMENT = """
MATCH (u:User)-[r:INTERACTED]->()
WITH u.username AS user,
     avg(r.sentiment_avg) AS avg_sentiment,
     count(r) AS interactions
WHERE interactions >= $min_interactions
  AND abs(avg_sentiment) <= $threshold
RETURN user, round(avg_sentiment, 4) AS avg_sentiment, interactions
ORDER BY interactions DESC
"""

Q_MARK_BOT_SUSPECTS = """
UNWIND $usernames AS username
MATCH (u:User {username: username})
SET u.bot_suspect = true
"""

Q_CLEAR_BOT_SUSPECTS = """
MATCH (u:User) WHERE u.bot_suspect IS NOT NULL
REMOVE u.bot_suspect
"""

# Análises


def detect_volume_outliers(conn):
    print("  Detectando outliers de volume (GDS weighted degree)...")
    rows_vol = conn.query(
        Q_GDS_WEIGHTED_DEGREE,
        {
            "graph": GDS_USER_INTERACTED,
            "min_interactions": MIN_INTERACTIONS,
        },
    )
    rows_targets = conn.query(Q_UNIQUE_TARGETS)
    df = pd.DataFrame(rows_vol).merge(pd.DataFrame(rows_targets), on="user", how="left")
    df["ratio"] = df["total_sent"] / df["unique_targets"].clip(lower=1)

    q1, q3 = df["total_sent"].quantile(0.25), df["total_sent"].quantile(0.75)
    ceiling = q3 + IQR_MULTIPLIER * (q3 - q1)
    outliers = df[df["total_sent"] > ceiling]
    normal = df[df["total_sent"] <= ceiling]

    print(f"  Threshold de volume : {ceiling:,.0f}")
    print(f"  Suspeitos por volume: {len(outliers):,}")

    fig, ax = plt.subplots()
    ax.hist(normal["total_sent"], bins=50, color=PALETTE[0], alpha=0.8, label="Normal")
    ax.hist(
        outliers["total_sent"], bins=20, color=PALETTE[3], alpha=0.8, label="Suspeito"
    )
    ax.axvline(
        ceiling, color="red", linestyle="--", label=f"Threshold ({ceiling:,.0f})"
    )
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax.set_title("Distribuição de Volume de Interações por Usuário")
    ax.set_xlabel("Total de Interações Enviadas")
    ax.set_ylabel("Número de Usuários")
    ax.legend()
    fig.tight_layout()
    save_chart(fig, "04_volume_outliers.png")

    return set(outliers["user"].tolist()), df


def detect_neutral_sentiment(conn):
    print("  Detectando usuários com sentimento sempre neutro...")
    rows = conn.query(
        Q_NEUTRAL_SENTIMENT,
        {
            "min_interactions": MIN_INTERACTIONS,
            "threshold": NEUTRAL_THRESHOLD,
        },
    )
    df = pd.DataFrame(rows)
    print(f"  Suspeitos por sentimento neutro: {len(df):,}")

    fig, ax = plt.subplots()
    ax.scatter(
        df["interactions"], df["avg_sentiment"], color=PALETTE[1], alpha=0.5, s=10
    )
    ax.axhline(NEUTRAL_THRESHOLD, color="red", linestyle="--", alpha=0.5)
    ax.axhline(-NEUTRAL_THRESHOLD, color="red", linestyle="--", alpha=0.5)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax.set_title("Usuários com Sentimento Neutro (possíveis bots)")
    ax.set_xlabel("Total de Interações")
    ax.set_ylabel("Sentimento Médio")
    fig.tight_layout()
    save_chart(fig, "04_neutral_sentiment.png")

    return set(df["user"].tolist())


def detect_high_ratio(df):
    """Reutiliza o dataframe de detect_volume_outliers — sem query extra."""
    print("  Detectando usuários com ratio interaction/target alto...")
    q1, q3 = df["ratio"].quantile(0.25), df["ratio"].quantile(0.75)
    ceiling = q3 + IQR_MULTIPLIER * (q3 - q1)
    outliers = df[df["ratio"] > ceiling]
    print(f"  Threshold de ratio  : {ceiling:.2f}")
    print(f"  Suspeitos por ratio : {len(outliers):,}")
    return set(outliers["user"].tolist())


def mark_suspects(conn, suspects):
    print(f"  Marcando {len(suspects):,} usuários como bot_suspect...")
    conn.query(Q_CLEAR_BOT_SUSPECTS)
    conn.query(Q_MARK_BOT_SUSPECTS, {"usernames": list(suspects)})
    print("  Concluído.")


# Entry point

if __name__ == "__main__":
    print("[ analysis_04 ] Qualidade dos Dados — Detecção de Bots")
    state = load_state()
    if is_done(state, "analysis_04"):
        print(
            "  Já executado, pulando. Apague output/_analysis_state.json para reexecutar."
        )
    else:
        with Neo4jConnection() as conn:
            ensure_user_interacted(conn)
            if not is_done(state, "04_volume"):
                suspects_volume, df_vol = detect_volume_outliers(conn)
                mark_done(state, "04_volume", suspects=len(suspects_volume))
            if not is_done(state, "04_neutral"):
                suspects_neutral = detect_neutral_sentiment(conn)
                mark_done(state, "04_neutral", suspects=len(suspects_neutral))
            suspects_ratio = detect_high_ratio(df_vol)
            all_suspects = suspects_volume | suspects_neutral | suspects_ratio
            print(f"\n  Total de suspeitos únicos : {len(all_suspects):,}")
            print(f"    Por volume              : {len(suspects_volume):,}")
            print(f"    Por sentimento neutro   : {len(suspects_neutral):,}")
            print(f"    Por ratio               : {len(suspects_ratio):,}")
            mark_suspects(conn, all_suspects)
        mark_done(state, "analysis_04")
    print("[ analysis_04 ] Concluído!")
