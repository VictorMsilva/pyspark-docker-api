# PySpark Docker API

🚀 **Complete NYC taxi data processing system with PySpark and FastAPI**

## 📋 Features

- ✅ **Distributed processing** with Apache Spark
- ✅ **REST API** with FastAPI for data querying
- ✅ **Docker support** for easy deployment
- ✅ **Advanced performance** optimizations
- ✅ **Dynamic filters** and statistics
- ✅ **Compressed Parquet** data format

## 🏗️ Architecture
```
[CSV Raw Data] → [PySpark Job] → [Parquet Files] → [FastAPI] → [User/API Client]

├── api/                 # FastAPI application
│   ├── main.py         # REST endpoints
│   └── requirements.txt
├── scripts/            # Processing scripts
│   ├── spark_job.py    # Main Spark job
│   ├── split_file.sh   # File splitting
│   ├── docker-control.sh # Docker control
│   └── consolidate_chunks.py
├── data/               # Data (examples included)
│   ├── raw/           # Original CSV data
│   ├── split_raw/     # Split files
│   └── processed/     # Processed Parquet data
├── Dockerfile.spark    # Spark container
├── Dockerfile.api      # API container
└── docker-compose.yml  # Complete orchestration
```

## 🚀 How to Use

### 1. Docker Compose (Recommended)
```bash
# Start Spark cluster + API
./scripts/docker-control.sh start

# Run Spark job only
./scripts/docker-control.sh job

# Start API only
./scripts/docker-control.sh api

# Stop all services
./scripts/docker-control.sh stop

# View logs
./scripts/docker-control.sh logs

# Complete cleanup
./scripts/docker-control.sh clean
```

### 2. Local Execution

#### Data Preparation
```bash
# Split large CSV file into smaller chunks
cd scripts
./split_file.sh
```

#### Spark Processing
```bash
# Run processing job
python scripts/spark_job.py
```

#### REST API
```bash
# Install dependencies
pip install -r api/requirements.txt

# Run API
cd api
uvicorn main:app --host 0.0.0.0 --port 8001
```

### 3. Manual Docker
```bash
# Build images
docker build -f Dockerfile.spark -t taxi-spark-job .
docker build -f Dockerfile.api -t taxi-api .

# Run containers
docker run -v $(pwd)/data:/opt/data taxi-spark-job
docker run -p 8001:8001 -v $(pwd)/data:/app/data taxi-api
```

## 📊 API Endpoints

After starting the containers, the API will be available at `http://localhost:8001`

- `GET /` - Health check
- `GET /info` - Dataset information
- `POST /filter` - Dynamic filters
- `POST /stats` - Statistics by columns
- `GET /samples` - Data samples

### Web Interfaces
- **API Documentation**: http://localhost:8001/docs
- **Spark Master UI**: http://localhost:8080
- **Spark Worker UI**: http://localhost:8081

## ⚙️ Configuration

### Environment Variables (.env)
```bash
# Data
CSV_PATH=./data/raw/2018_Yellow_Taxi_Trip_Data.csv
PARQUET_PATH=./data/processed/taxi_clean.parquet

# API
API_PORT=8001

# Spark (for Docker)
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_WORKER_MEMORY=4g
SPARK_WORKER_CORES=2
```

### Spark Optimizations
- **Adaptive Query Execution** enabled
- **Column pruning** automatic
- **Predicate pushdown** optimized
- **Snappy compression** for Parquet
- **Intelligent partitioning**

## 📈 Performance

### Processed Data
- **~113 million** records
- **20 split** CSV files
- **Parallel processing** in 20 partitions
- **Optimized Parquet** format

### Calculated Features
- Trip duration (minutes)
- Average speed (mph)
- Pickup hour
- Day of week
- Tip percentage

## 🛠️ Technologies

- **Apache Spark 3.5+** - Distributed processing
- **Python 3.11+** - Main language
- **FastAPI** - Modern REST API
- **Pandas** - Data manipulation
- **PyArrow** - Optimized Parquet reading
- **Docker & Docker Compose** - Containerization
- **Bitnami Spark** - Optimized Docker image

## 🐳 Containers

### Spark Cluster
- **spark-master**: Cluster coordinator (port 8080)
- **spark-worker**: Worker node (port 8081)
- **spark-job**: Job executor (profile: job)

### API
- **taxi-api**: FastAPI service (port 8001)

### Volumes
- `./data`: Shared data between containers
- `./scripts`: Processing scripts

## 🎮 Control Script

The `scripts/docker-control.sh` file provides simple commands to manage the environment:

```bash
# View all available options
./scripts/docker-control.sh help

# Common usage scenarios
./scripts/docker-control.sh start  # Complete environment
./scripts/docker-control.sh job    # Processing only
./scripts/docker-control.sh api    # API only
./scripts/docker-control.sh logs   # View real-time logs
./scripts/docker-control.sh clean  # Complete cleanup
```

## 📝 License

MIT License - See [LICENSE](LICENSE) for details.

---

💡 **Developed for NYC taxi data analysis with focus on performance and scalability.**
