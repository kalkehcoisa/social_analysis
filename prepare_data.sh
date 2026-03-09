#!/bin/bash

# Diretório deste script bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$SCRIPT_DIR/scripts"
DATASET_DIR="$SCRIPT_DIR/dataset"
DATASET_IMPORT_DIR="$DATASET_DIR/neo4j_import"
NEO4J_DATA_DIR="$HOME/neo4j-data/import"

mkdir -p "$NEO4J_DATA_DIR"
$(poetry env activate)

echo "Limpando os arquivos tsv:"
python "$SCRIPTS_DIR/prepare_data_01.py"

echo "Limpando os arquivos csv:"
python "$SCRIPTS_DIR/prepare_data_02.py"

echo "Preparando os dados para importá-los com o neo4j-admin:"
python "$SCRIPTS_DIR/prepare_data_03.py"

COUNT=$(cypher-shell -d neo4j -u neo4j -p neo4j123 --format plain "MATCH (n) RETURN count(n);")

if [ "$COUNT" = "0" ]; then
    echo "Copiando os arquivos csv para o import do neo4j..."
    bash "import_command.sh"
else
    echo "Banco já importado, pulando..."
fi

echo "Criando índices no neo4j..."
python "$SCRIPTS_DIR/prepare_data_04.py"
