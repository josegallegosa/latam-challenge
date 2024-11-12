from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from typing import List, Tuple
import datetime
import re
import emoji

def create_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "10g") \
        .getOrCreate()


# Enfoque optimizado para memoria - Pregunta 1
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    spark = create_spark_session("q1_memory")
    
    # Procesar los datos en chunks
    df = spark.read.json(file_path)
    
    # Usar ventanas para optimizar memoria
    from pyspark.sql.window import Window
    
    # Analizar por fecha sin cache
    date_counts = df.groupBy(F.to_date(F.col("date")).alias("date")) \
        .count() \
        .orderBy(F.col("count").desc()) \
        .limit(10)
    
    # Usar window functions para encontrar top usuarios por fecha
    window_spec = Window.partitionBy(F.to_date(F.col("date")).alias("date")) \
        .orderBy(F.col("user_count").desc())
    
    user_counts = df.groupBy(
        F.to_date(F.col("date")).alias("date"),
        "username"
    ).count().alias("user_count")
    
    result = user_counts.withColumn(
        "rank",
        F.row_number().over(window_spec)
    ).filter(
        F.col("rank") == 1
    ).join(
        date_counts,
        "date"
    ).select(
        "date",
        "username"
    ).orderBy(
        F.col("count").desc()
    ).collect()
    
    spark.stop()
    return [(row.date, row.username) for row in result]