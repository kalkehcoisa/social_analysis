#!/bin/bash
# Comando gerado automaticamente por prepare_neo4j_import.py
#
# Pré-requisitos:
#   1. Os CSVs devem estar em $DATASETS
#
# Uso:
#   bash import_command.sh

set -e

# Diretório deste script bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

DATASETS="$SCRIPT_DIR/../dataset/neo4j_import/"


neo4j stop
echo ">>> Iniciando importação..."

neo4j-admin database import full \
  --nodes=User="$DATASETS/neo4j_users.csv" \
  --nodes=Submission="$DATASETS/neo4j_submissions.csv" \
  --nodes=Subreddit="$DATASETS/neo4j_subreddits.csv" \
  --relationships=INTERACTED="$DATASETS/neo4j_interacted.csv" \
  --relationships=POSTED="$DATASETS/neo4j_posted.csv" \
  --relationships=BELONGS_TO="$DATASETS/neo4j_belongs_to.csv" \
  --relationships=ACTIVE_IN="$DATASETS/neo4j_active_in.csv" \
  --delimiter="," \
  --ignore-empty-strings=true \
  --bad-tolerance=0 \
  --overwrite-destination=true \
  --verbose \
  neo4j

echo ">>> Importação concluída! Reiniciando o Neo4j"
neo4j start
