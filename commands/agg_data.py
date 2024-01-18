from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


def main(**kwargs):
    current_date = kwargs['ds']
    
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName('ratings_task')\
        .getOrCreate()

    df = spark.read.parquet(f"/user/tdkozachkin/project/DATA/DT={current_date}")

    df_agg = df\
        .groupBy("agency")\
        .agg(
            count(col("name")).alias("num_ratings")
        )

    df_agg\
        .write\
        .mode("overwrite")\
        .parquet(f"/user/tdkozachkin/project/AGGDATA/DT={current_date}")

    df_agg_stats = df\
        .groupBy("agency", "rating")\
        .agg(
            count(col("name")).alias("num_ratings"),
            max(col("rat_date")).alias("max_date"),
            min(col("rat_date")).alias("max_date")
        )

    df_agg_stats\
        .write\
        .mode("overwrite")\
        .parquet(f"/user/tdkozachkin/project/AGGDATASTATS/DT={current_date}")

    df_agg_date = df\
        .groupBy("rat_date")\
        .agg(
            count(col("name")).alias("num_ratings")
        )

    df_agg_date\
        .write\
        .mode("overwrite")\
        .parquet(f"/user/tdkozachkin/project/AGGDATADATE/DT={current_date}")

    spark.stop()