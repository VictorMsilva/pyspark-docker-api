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
│   ├── docker-control.sh # Controle Docker
│   └── consolidate_chunks.py
├── data/               # Dados (exemplos incluídos)
│   ├── raw/           # Dados originais CSV
│   ├── split_raw/     # Arquivos divididos
│   └── processed/     # Dados processados Parquet
├── Dockerfile.spark    # Container Spark
├── Dockerfile.api      # Container API
└── docker-compose.yml  # Orquestração completa
```

## 🚀 Como Usar

### 1. Docker Compose (Recomendado)
```bash
# Iniciar cluster Spark + API
./scripts/docker-control.sh start

# Executar apenas o job Spark
./scripts/docker-control.sh job

# Iniciar apenas a API
./scripts/docker-control.sh api

# Parar todos os serviços
./scripts/docker-control.sh stop

# Ver logs
./scripts/docker-control.sh logs

# Limpeza completa
./scripts/docker-control.sh clean
```

### 2. Execução Local

#### Preparação dos Dados
```bash
# Dividir arquivo CSV grande em chunks menores
cd scripts
./split_file.sh
```

#### Processamento com Spark
```bash
# Executar job de processamento
python scripts/spark_job.py
```

#### API REST
```bash
# Instalar dependências
pip install -r api/requirements.txt

# Executar API
cd api
uvicorn main:app --host 0.0.0.0 --port 8001
```

### 3. Docker Manual
```bash
# Build das imagens
docker build -f Dockerfile.spark -t taxi-spark-job .
docker build -f Dockerfile.api -t taxi-api .

# Executar containers
docker run -v $(pwd)/data:/opt/data taxi-spark-job
docker run -p 8001:8001 -v $(pwd)/data:/app/data taxi-api
```

## 📊 Endpoints da API

Após iniciar os containers, a API estará disponível em `http://localhost:8001`

- `GET /` - Health check
- `GET /info` - Informações do dataset
- `POST /filter` - Filtros dinâmicos
- `POST /stats` - Estatísticas por colunas
- `GET /samples` - Amostras de dados

### Interfaces Web
- **API Documentation**: http://localhost:8001/docs
- **Spark Master UI**: http://localhost:8080
- **Spark Worker UI**: http://localhost:8081

## ⚙️ Configuração

### Variáveis de Ambiente (.env)
```bash
# Dados
CSV_PATH=./data/raw/2018_Yellow_Taxi_Trip_Data.csv
PARQUET_PATH=./data/processed/taxi_clean.parquet

# API
API_PORT=8001

# Spark (para Docker)
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_WORKER_MEMORY=4g
SPARK_WORKER_CORES=2
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

- **Apache Spark 3.5+** - Processamento distribuído
- **Python 3.11+** - Linguagem principal
- **FastAPI** - API REST moderna
- **Pandas** - Manipulação de dados
- **PyArrow** - Leitura Parquet otimizada
- **Docker & Docker Compose** - Containerização
- **Bitnami Spark** - Imagem Docker otimizada

## 🐳 Containers

### Spark Cluster
- **spark-master**: Coordenador do cluster (porta 8080)
- **spark-worker**: Worker node (porta 8081)
- **spark-job**: Executor de jobs (profile: job)

### API
- **taxi-api**: FastAPI service (porta 8001)

### Volumes
- `./data`: Dados compartilhados entre containers
- `./scripts`: Scripts de processamento

## 🎮 Script de Controle

O arquivo `scripts/docker-control.sh` fornece comandos simples para gerenciar o ambiente:

```bash
# Ver todas as opções disponíveis
./scripts/docker-control.sh help

# Cenários de uso comum
./scripts/docker-control.sh start  # Ambiente completo
./scripts/docker-control.sh job    # Apenas processamento
./scripts/docker-control.sh api    # Apenas API
./scripts/docker-control.sh logs   # Ver logs em tempo real
./scripts/docker-control.sh clean  # Limpeza completa
```

## 📝 Licença

MIT License - Veja [LICENSE](LICENSE) para detalhes.

---

💡 **Desenvolvido para análise de dados de táxi de NYC com foco em performance e escalabilidade.**