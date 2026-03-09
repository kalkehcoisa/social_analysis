"""
neo4j_base.py
=============
Módulo compartilhado pelos scripts de análise.
Fornece conexão com Neo4j, helpers de visualização e projeções GDS.
"""

import json
import os
import time

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from neo4j import GraphDatabase
from neo4j.exceptions import AuthError, ServiceUnavailable

# Configuração

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4j123")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

CONNECT_TIMEOUT = 120
CONNECT_INTERVAL = 5

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CHARTS_DIR = os.path.join(PROJECT_ROOT, "output", "charts")
os.makedirs(CHARTS_DIR, exist_ok=True)

# Estilo global dos gráficos

plt.rcParams.update(
    {
        "figure.figsize": (12, 6),
        "figure.dpi": 150,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.titlesize": 14,
        "axes.titleweight": "bold",
        "axes.labelsize": 11,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
    }
)

PALETTE = [
    "#4C72B0",
    "#DD8452",
    "#55A868",
    "#C44E52",
    "#8172B3",
    "#937860",
    "#DA8BC3",
    "#8C8C8C",
]

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


# Conexão


class Neo4jConnection:
    def __init__(self):
        self._driver = self._wait_for_neo4j()

    def _wait_for_neo4j(self):
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        deadline = time.time() + CONNECT_TIMEOUT
        attempt = 0
        while True:
            attempt += 1
            try:
                driver.verify_connectivity()
                if attempt > 1:
                    print(f"  Neo4j disponivel apos {attempt} tentativas.")
                return driver
            except AuthError:
                driver.close()
                raise
            except ServiceUnavailable:
                if time.time() >= deadline:
                    driver.close()
                    raise RuntimeError(
                        f"Neo4j nao respondeu em {CONNECT_TIMEOUT}s ({NEO4J_URI}). "
                        "Verifique se o container esta rodando."
                    )
                if attempt == 1:
                    print(f"  Aguardando Neo4j em {NEO4J_URI}...", flush=True)
                time.sleep(CONNECT_INTERVAL)

    def close(self):
        self._driver.close()

    def query(self, cypher, parameters=None):
        with self._driver.session(database=NEO4J_DATABASE) as session:
            result = session.run(cypher, parameters or {})
            return [record.data() for record in result]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# GDS — projeções

GDS_USER_INTERACTED = "graph-user-interacted"
GDS_USER_SUBREDDIT = "graph-user-subreddit"

Q_GDS_EXISTS = "CALL gds.graph.exists($name) YIELD exists RETURN exists"

Q_GDS_USER_INTERACTED_PROJECT = """
CALL gds.graph.project(
  $name,
  'User',
  {
    INTERACTED: {
      orientation: 'NATURAL',
      properties: ['interaction_count', 'sentiment_avg', 'sentiment_sum']
    }
  }
)
"""

Q_GDS_USER_SUBREDDIT_PROJECT = """
CALL gds.graph.project(
  $name,
  ['User', 'Subreddit'],
  { ACTIVE_IN: { orientation: 'UNDIRECTED' } }
)
"""


def ensure_projection(conn, name, q_project):
    exists = conn.query(Q_GDS_EXISTS, {"name": name})[0]["exists"]
    if not exists:
        print(f"  Projetando '{name}' no GDS...")
        conn.query(q_project, {"name": name})
    else:
        print(f"  Reusando projecao '{name}'.")


def ensure_user_interacted(conn):
    ensure_projection(conn, GDS_USER_INTERACTED, Q_GDS_USER_INTERACTED_PROJECT)


def drop_projection(conn, name):
    exists = conn.query(Q_GDS_EXISTS, {"name": name})[0]["exists"]
    if exists:
        conn.query("CALL gds.graph.drop($name)", {"name": name})
        print(f"  Projecao '{name}' removida da memoria.")


def ensure_user_subreddit(conn):
    ensure_projection(conn, GDS_USER_SUBREDDIT, Q_GDS_USER_SUBREDDIT_PROJECT)


# Helpers de visualização


def save_chart(fig, filename):
    path = os.path.join(CHARTS_DIR, filename)
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"  Salvo: {path}")
    return path


def bar_chart(
    labels, values, title, xlabel, ylabel, filename, color=None, horizontal=False
):
    fig, ax = plt.subplots()
    color = color or PALETTE[0]
    if horizontal:
        ax.barh(labels, values, color=color)
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
        ax.set_xlabel(ylabel)
        ax.set_ylabel(xlabel)
        ax.invert_yaxis()
    else:
        ax.bar(labels, values, color=color)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        plt.xticks(rotation=45, ha="right")
    ax.set_title(title)
    fig.tight_layout()
    return save_chart(fig, filename)
