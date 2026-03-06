#!/bin/bash

# Diretório deste script bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$SCRIPT_DIR/scripts"
DATASET_DIR="$SCRIPT_DIR/dataset"
DATASET_IMPORT_DIR="$DATASET_DIR/neo4j_import"
NEO4J_DATA_DIR="$HOME/neo4j-data/import"
mkdir -p "$NEO4J_DATA_DIR"


$(poetry env activate)

# echo "Limpando os arquivos tsv:"
# python "$SCRIPTS_DIR/prepare_data_01.py"

# echo "Limpando os arquivos csv:"
# python "$SCRIPTS_DIR/prepare_data_02.py"

echo "Preparando os dados para importá-los com o neo4j-admin:"
python "$SCRIPTS_DIR/prepare_data_03.py"

echo "Copiando os arquivos csv para o import do neo4j..."
cp -n $DATASET_IMPORT_DIR/*.csv "$NEO4J_DATA_DIR"
bash "$DATASET_IMPORT_DIR/import_command.sh"

# echo "Importando dados..."
# docker exec -i neo4j cypher-shell \
#   -u neo4j -p neo4j123 \
#   --param 'file_path_root => "file:///"' \
#   --param 'file_0 => "users_clean.csv"' \
#   --param 'file_1 => "comments_clean.csv"' \
#   --param 'file_1 => "posts_clean.csv"' \
#   < ./cypher/import_initial_data.cypher
# echo "Importação finalizada."

# exit 0


# echo "Importando dados..."
# docker exec -i neo4j cypher-shell \
#   -u neo4j -p neo4j123 \
#   --param 'file_path_root => "file:///"' \
#   --param 'file_0 => "User.Listening.History.filtrado.csv"' \
#   --param 'file_1 => "Music.Info.filtrado.csv"' \
#   < ./cypher/00_import_data.cypher
# echo "Importação finalizada."

# echo "Extraindo Generos..."
# docker exec -i neo4j cypher-shell \
#   -u neo4j -p neo4j123 \
#   < ./cypher/01_genres_tags.cypher
# echo "Generos extraidos."

# echo "Limpeza e normalização dos dados..."
# docker exec -i neo4j cypher-shell \
#   -u neo4j -p neo4j123 \
#   < ./cypher/02_clean_normalize.cypher
# echo "Dados normalizados."

# echo "Calculando similaridades..."
# docker exec -i neo4j cypher-shell \
#   -u neo4j -p neo4j123 \
#   < ./cypher/03_calc_similarities.cypher
# echo "Similaridades calculadas."

# echo "Obter sugestões de músicas..."
# docker exec -i neo4j cypher-shell \
#   -u neo4j -p neo4j123 \
#   < ./cypher/04_return_similar_songs.cypher
