from typing import List, Tuple
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import re
import emoji

def create_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "10g") \
        .getOrCreate()


def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    spark = create_spark_session("q1_time")
    
    # Leer los datos y crear un DataFrame
    df = spark.read.json(file_path)
    
    # Cache del DataFrame para múltiples operaciones
    df.cache()
    
    # Encontrar las top 10 fechas con más tweets
    top_dates = df.groupBy(F.to_date(F.col("date")).alias("date")) \
        .count() \
        .orderBy(F.col("count").desc()) \
        .limit(10)
    
    # Para cada fecha top, encontrar el usuario con más tweets
    result = []
    top_dates_list = [row.date for row in top_dates.collect()]
    
    for date in top_dates_list:
        top_user = df.filter(F.to_date(F.col("date")) == date) \
            .groupBy("username") \
            .count() \
            .orderBy(F.col("count").desc()) \
            .first()
        
        result.append((date, top_user.username))
    
    spark.stop()
    return result