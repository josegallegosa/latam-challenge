from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from typing import List, Tuple
import datetime
import re
import emoji
# Enfoque optimizado para memoria - Pregunta 3
def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    spark = create_spark_session("q3_memory")
    
    df = spark.read.json(file_path)
    
    # Extraer menciones usando regex
    mentions_udf = F.udf(
        lambda text: re.findall(r'@(\w+)', text),
        ArrayType(StringType())
    )
    
    result = df.select(F.explode(mentions_udf(F.col("content"))).alias("mention")) \
        .repartition(100) \
        .groupBy("mention") \
        .count() \
        .orderBy(F.col("count").desc()) \
        .limit(10) \
        .collect()
    
    spark.stop()
    return [(row.mention, row.count) for row in result]