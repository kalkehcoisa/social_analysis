#!/bin/bash
# run_analysis.sh
# Executa todos os scripts de análise em sequência.
#
# Uso:
#   bash run_analysis.sh
#
# Variáveis de ambiente opcionais:
#   NEO4J_URI      (padrão: bolt://localhost:7687)
#   NEO4J_USER     (padrão: neo4j)
#   NEO4J_PASSWORD (padrão: neo4j123)
#   NEO4J_DATABASE (padrão: neo4j)

set -e

SCRIPTS_DIR="$(cd "$(dirname "$0")/scripts" && pwd)"
OUTPUT_DIR="$(cd "$(dirname "$0")" && pwd)/output/charts"
mkdir -p "$OUTPUT_DIR"

echo "========================================"
echo " Reddit Social Graph — Análises"
echo "========================================"
echo ""

run_script() {
    echo "----------------------------------------"
    echo " Executando: $1"
    echo "----------------------------------------"
    python "$SCRIPTS_DIR/$1"
    echo ""
}

# Bots primeiro para não distorcer as demais análises
run_script "analysis_04_bots.py"
run_script "analysis_01_engagement.py"
run_script "analysis_02_content.py"
run_script "analysis_03_communities.py"

echo "========================================"
echo " Análises concluídas!"
echo " Gráficos em: $OUTPUT_DIR"
echo "========================================"
