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

# Enfoque optimizado para tiempo - Pregunta 3
def q3_time(file_path: str) -> List[Tuple[str, int]]:
    spark = create_spark_session("q3_time")
    
    df = spark.read.json(file_path)
    df.cache()
    
    # Extraer menciones usando regex
    mentions_udf = F.udf(
        lambda text: re.findall(r'@(\w+)', text),
        ArrayType(StringType())
    )
    
    result = df.select(F.explode(mentions_udf(F.col("content"))).alias("mention")) \
        .groupBy("mention") \
        .count() \
        .orderBy(F.col("count").desc()) \
        .limit(10) \
        .collect()
    
    spark.stop()
    return [(row.mention, row.count) for row in result]