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

# Enfoque optimizado para tiempo - Pregunta 2
def q2_time(file_path: str) -> List[Tuple[str, int]]:
    spark = create_spark_session("q2_time")
    
    df = spark.read.json(file_path)
    df.cache()
    
    # Extraer todos los emojis del texto
    emoji_udf = F.udf(lambda text: [c for c in text if c in emoji.UNICODE_EMOJI['en']], ArrayType(StringType()))
    
    result = df.select(F.explode(emoji_udf(F.col("content"))).alias("emoji")) \
        .groupBy("emoji") \
        .count() \
        .orderBy(F.col("count").desc()) \
        .limit(10) \
        .collect()
    
    spark.stop()
    return [(row.emoji, row.count) for row in result]