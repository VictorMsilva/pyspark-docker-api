from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import glob
from dotenv import load_dotenv

load_dotenv()
CSV_PATH = os.getenv("CSV_PATH")
PARQUET_PATH = os.getenv("PARQUET_PATH")

# Configuração da sessão Spark otimizada
spark = SparkSession.builder \
    .appName("TaxiDataProcessing") \
    .config("spark.driver.memory", "8g") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.memoryFraction", "0.8") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema definido
schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

# Determina o caminho base do projeto (scripts está um nível abaixo da raiz)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
csv_pattern = os.path.join(project_root, "data", "split_raw", "*.csv")
csv_files = glob.glob(csv_pattern)

print(f"Tentando ler arquivos de: {csv_pattern}")
print(f"Diretório atual: {os.getcwd()}")
print(f"Arquivos encontrados: {len(csv_files)}")
print(f"Primeiros arquivos: {csv_files[:3]}")

# Lê o dataset com schema aplicado - usando arquivos divididos para processamento paralelo
df = spark.read.option("header", True).schema(schema).csv(csv_files).repartition(20)

#1: Filtra dados inválidos ANTES das transformações pesadas
df_filtered_early = df.filter(
    col("tpep_pickup_datetime").isNotNull() &
    col("tpep_dropoff_datetime").isNotNull() &
    col("passenger_count").isNotNull() &
    col("trip_distance").isNotNull() &
    col("fare_amount").isNotNull() &
    (col("passenger_count") >= 1) & (col("passenger_count") <= 8) &
    (col("trip_distance") > 0) & (col("trip_distance") < 500) &
    (col("fare_amount") > 0) & (col("fare_amount") < 1000) &
    (col("total_amount") > 0) & (col("total_amount") < 1000) &
    (col("tip_amount") >= 0) &
    col("RatecodeID").isin([1, 2, 3, 4, 5, 6]) &
    col("payment_type").isin([1, 2, 3, 4, 5, 6])
)


#2: Transformações de timestamp com verificação
df_with_timestamps = df_filtered_early.withColumn(
    "tpep_pickup_datetime", 
    to_timestamp(col("tpep_pickup_datetime"), "MM/dd/yyyy hh:mm:ss a")
).withColumn(
    "tpep_dropoff_datetime", 
    to_timestamp(col("tpep_dropoff_datetime"), "MM/dd/yyyy hh:mm:ss a")
)

#3: Filtros de data logo após conversão
df_date_filtered = df_with_timestamps.filter(
    col("tpep_pickup_datetime").isNotNull() &
    col("tpep_dropoff_datetime").isNotNull() &
    (col("tpep_pickup_datetime") < col("tpep_dropoff_datetime")) &
    (year(col("tpep_pickup_datetime")) == 2018) &
    (year(col("tpep_dropoff_datetime")) == 2018)
)

#4: Todas as transformações em uma única operação
df_enriched = df_date_filtered.withColumn(
    "trip_duration_minutes", 
    (unix_timestamp(col("tpep_dropoff_datetime")) - 
     unix_timestamp(col("tpep_pickup_datetime"))) / 60
).withColumn(
    "pickup_hour", hour(col("tpep_pickup_datetime"))
).withColumn(
    "pickup_day_of_week", dayofweek(col("tpep_pickup_datetime"))
).withColumn(
    "pickup_month", month(col("tpep_pickup_datetime"))
).withColumn(
    "speed_mph", 
    when(col("trip_duration_minutes") > 0, 
         col("trip_distance") / (col("trip_duration_minutes") / 60))
    .otherwise(0)
).withColumn(
    "tip_percentage",
    when(col("fare_amount") > 0, (col("tip_amount") / col("fare_amount")) * 100)
    .otherwise(0)
)

# Filtros finais
df_final = df_enriched.filter(
    (col("trip_duration_minutes") >= 1) & (col("trip_duration_minutes") <= 480) &
    (col("speed_mph") >= 0) & (col("speed_mph") <= 80) &
    (col("tip_percentage") >= 0) & (col("tip_percentage") <= 50)
)

#5: Usa coalesce para gerar arquivo único
df_final.coalesce(1).write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .option("maxRecordsPerFile", "1000000") \
    .parquet(PARQUET_PATH)

#6: Limpa cache antes de finalizar
df_filtered_early.unpersist()

spark.stop()