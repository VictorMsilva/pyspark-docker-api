#!/bin/bash

ARQUIVO="../data/raw/2018_Yellow_Taxi_Trip_Data_20250727.csv"  # Altere para o nome real do seu arquivo

# Verificar se o arquivo existe
if [ ! -f "$ARQUIVO" ]; then
    echo "Erro: Arquivo $ARQUIVO não encontrado!"
    exit 1
fi

# Criar diretório de destino se não existir
mkdir -p "../data/split_raw"

# 1. Contar número total de linhas (sem cabeçalho)
TOTAL=$(tail -n +2 "$ARQUIVO" | wc -l)

# 2. Calcular número de linhas por parte
LINES_PER_PART=$(( (TOTAL + 19) / 20 ))  # arredonda pra cima

# 3. Salvar cabeçalho
head -n 1 "$ARQUIVO" > header.csv

# 4. Dividir corpo do CSV
tail -n +2 "$ARQUIVO" | split -l $LINES_PER_PART - parte_

# 5. Adicionar cabeçalho a cada parte
i=1
for f in parte_*; do
    printf -v idx "%02d" "$i"
    cat header.csv "$f" > "../data/split_raw/split_$idx.csv"
    rm "$f"
    i=$((i + 1))
done

# 6. Limpar
rm header.csv

echo "Divisão completa. Gerados $(($i - 1)) arquivos split_XX.csv"