#!/bin/bash
# Comando gerado automaticamente por prepare_neo4j_import.py
#
# Pré-requisitos:
#   1. Os CSVs devem estar em /Users/jaymetosineto/neo4j-data/import
#   2. O container Neo4j deve estar parado
#
# Uso:
#   bash import_command.sh

set -e

echo ">>> Iniciando importação..."

docker run --rm \
  -v "/Users/jaymetosineto/neo4j-data/import:/import" \
  -v "/Users/jaymetosineto/neo4j-data/data:/data" \
  neo4j:5 \
  neo4j-admin database import full \
    --nodes=User="/import/neo4j_users.csv" \
    --nodes=Submission="/import/neo4j_submissions.csv" \
    --nodes=Subreddit="/import/neo4j_subreddits.csv" \
    --relationships=INTERACTED="/import/neo4j_interacted.csv" \
    --relationships=POSTED="/import/neo4j_posted.csv" \
    --relationships=BELONGS_TO="/import/neo4j_belongs_to.csv" \
    --relationships=ACTIVE_IN="/import/neo4j_active_in.csv" \
    --delimiter="," \
    --ignore-empty-strings=true \
    --bad-tolerance=0 \
    --overwrite-destination \
    --verbose \
    neo4j

echo ">>> Importação concluída! Inicie o Neo4j com neo4j-docker.sh"
