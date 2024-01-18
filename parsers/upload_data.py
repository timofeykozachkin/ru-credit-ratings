from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import DateType


def main(**kwargs):
    current_date = kwargs["ds"]
    current_date = "2023-12-27"

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("ratings_task")
        .config("spark.jars", "/usr/share/java/mysql-connector-java-8.2.0.jar")
        .getOrCreate()
    )

    df = spark.read.parquet(f"/user/tdkozachkin/project/DATA/DT={current_date}")

    df.write.mode("overwrite").format("jdbc").option(
        "driver", "com.mysql.cj.jdbc.Driver"
    ).option("url", "jdbc:mysql://localhost:3306/hse").option(
        "dbtable", "credit_ratings"
    ).option(
        "user", "arhimag"
    ).option(
        "password", "password57"
    ).save()

    df_agg = spark.read.parquet(f"/user/tdkozachkin/project/AGGDATA/DT={current_date}")

    df_agg.withColumn("record_date", lit(current_date).cast(DateType())).write.mode(
        "overwrite"
    ).format("jdbc").option("driver", "com.mysql.cj.jdbc.Driver").option(
        "url", "jdbc:mysql://localhost:3306/hse"
    ).option(
        "dbtable", "agencies_stats"
    ).option(
        "user", "arhimag"
    ).option(
        "password", "password57"
    ).save()

    spark.stop()
