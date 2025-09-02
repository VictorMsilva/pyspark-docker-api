# PySpark Docker API

🚀 **Sistema completo de processamento de dados de táxi NYC com PySpark e FastAPI**

## 📋 Funcionalidades

- ✅ **Processamento distribuído** com Apache Spark
- ✅ **API REST** com FastAPI para consulta de dados
- ✅ **Suporte Docker** para fácil deploy
- ✅ **Otimizações avançadas** de performance
- ✅ **Filtros dinâmicos** e estatísticas
- ✅ **Dados em formato Parquet** comprimido

## 🏗️ Arquitetura
```text
[CSV Raw Data] → [PySpark Job] → [Parquet Files] → [FastAPI] → [User/API Client]

```
├── api/                 # FastAPI application
│   ├── main.py         # Endpoints REST
│   └── requirements.txt
├── scripts/            # Scripts de processamento
│   ├── spark_job.py    # Job principal Spark
│   ├── split_file.sh   # Divisão de arquivos
│   └── consolidate_chunks.py
├── data/               # Dados (excluído do Git)
│   ├── raw/           # Dados originais CSV
│   ├── split_raw/     # Arquivos divididos
│   └── processed/     # Dados processados Parquet
└── Dockerfile.spark    # Container Spark
```

## 🚀 Como Usar

### 1. Preparação dos Dados
```bash
# Dividir arquivo CSV grande em chunks menores
cd scripts
./split_file.sh
```

### 2. Processamento com Spark
```bash
# Executar job de processamento
python scripts/spark_job.py
```

### 3. API REST
```bash
# Instalar dependências
pip install -r api/requirements.txt

# Executar API
cd api
uvicorn main:app --host 0.0.0.0 --port 8001
```

### 4. Docker (Alternativo)
```bash
# Build da imagem Spark
docker build -f Dockerfile.spark -t taxi-spark-job .

# Executar processamento
docker run -v $(pwd)/data:/opt/data taxi-spark-job
```

## 📊 Endpoints da API

- `GET /` - Health check
- `GET /info` - Informações do dataset
- `POST /filter` - Filtros dinâmicos
- `POST /stats` - Estatísticas por colunas
- `GET /samples` - Amostras de dados

## ⚙️ Configuração

### Variáveis de Ambiente (.env)
```bash
CSV_PATH=./data/raw/2018_Yellow_Taxi_Trip_Data.csv
PARQUET_PATH=./data/processed/taxi_clean.parquet
API_PORT=8001
```

### Otimizações Spark
- **Adaptive Query Execution** habilitado
- **Column pruning** automático
- **Predicate pushdown** otimizado
- **Compressão Snappy** para Parquet
- **Particionamento** inteligente

## 📈 Performance

### Dados Processados
- **~113 milhões** de registros
- **20 arquivos** CSV divididos
- **Processamento paralelo** em 20 partições
- **Formato Parquet** otimizado

### Features Calculadas
- Duração da viagem (minutos)
- Velocidade média (mph)
- Hora do pickup
- Dia da semana
- Porcentagem de gorjeta

## 🛠️ Tecnologias

- **Apache Spark 3.2+** - Processamento distribuído
- **Python 3.11+** - Linguagem principal
- **FastAPI** - API REST moderna
- **Pandas** - Manipulação de dados
- **PyArrow** - Leitura Parquet otimizada
- **Docker** - Containerização
- **Bitnami Spark** - Imagem Docker otimizada

## 📝 Licença

MIT License - Veja [LICENSE](LICENSE) para detalhes.

---

💡 **Desenvolvido para análise de dados de táxi de NYC com foco em performance e escalabilidade.**