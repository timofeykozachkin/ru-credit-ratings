from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


def main(**kwargs):
    current_date = kwargs["ds"]
    current_date = "2023-12-27"
    spark = SparkSession.builder.master("yarn").appName("ratings_task").getOrCreate()

    df = spark.read.parquet(f"/user/tdkozachkin/project/DATA/DT={current_date}")

    df_agg = df.groupBy("agency").agg(count(col("name")).alias("num_ratings"))

    df_agg.write.mode("overwrite").parquet(
        f"/user/tdkozachkin/project/AGGDATA/DT={current_date}"
    )

    spark.stop()
