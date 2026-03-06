#!/usr/bin/env bash

set -e

NEO4J_VERSION=5
NEO4J_PASSWORD=neo4j123
CONTAINER_NAME=neo4j
DATA_DIR=$HOME/neo4j-data

mkdir -p "$DATA_DIR"/{data,logs,import,plugins,conf}

cat > $DATA_DIR/conf/apoc.conf <<EOL
apoc.export.file.enabled=true
apoc.import.file.enabled=true
EOL

# Verifica se o container já existe
if docker ps -a --format '{{.Names}}' | grep -w "$CONTAINER_NAME" > /dev/null; then
  echo "Container já existe."

  # Verifica se está rodando
  if docker ps --format '{{.Names}}' | grep -w "$CONTAINER_NAME" > /dev/null; then
    echo "Container já está em execução."
  else
    echo "Iniciando container existente..."
    docker start "$CONTAINER_NAME"
  fi
else
  echo "Criando novo container..."
    # -e NEO4J_AUTH=neo4j/$NEO4J_PASSWORD \
    # -e NEO4JLABS_PLUGINS='["graph-data-science"]' \
    # -e NEO4J_dbms_security_procedures_unrestricted=gds.* \
    # -e NEO4J_dbms_security_procedures_allowlist=gds.* \
  docker run -d \
    --name $CONTAINER_NAME \
    -p 7474:7474 \
    -p 7687:7687 \
    -v "$DATA_DIR/data":/data \
    -v "$DATA_DIR/logs":/logs \
    -v "$DATA_DIR/import":/import \
    -v "$DATA_DIR/plugins":/plugins \
    -v "$DATA_DIR/conf":/conf \
    -e NEO4J_AUTH=none \
    -e NEO4JLABS_PLUGINS='["apoc","graph-data-science"]' \
    -e NEO4J_dbms_security_procedures_unrestricted=apoc.*,gds.* \
    -e NEO4J_dbms_security_procedures_allowlist=apoc.*,gds.* \
    -e NEO4J_server_memory_heap_initial__size=2G \
    -e NEO4J_server_memory_heap_max__size=2G \
    -e NEO4J_server_memory_pagecache_size=1G \
    --restart unless-stopped \
    neo4j:$NEO4J_VERSION
fi

echo "Neo4j disponível em:"
echo "Web: http://localhost:7474"
echo "User: neo4j"
echo "Password: $NEO4J_PASSWORD"