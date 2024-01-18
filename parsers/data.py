import pandas as pd
from pyspark.sql import SparkSession

from parsers.expert_ra import expert_ra_archive, expert_ra_ratings
from parsers.nkr import nkr_ratings
from parsers.nra import nra_ratings


def main(**kwargs):
    current_date = kwargs["ds"]
    current_date = "2023-12-27"
    df1 = expert_ra_ratings()
    # df2 = df1.copy().iloc[0:0]
    df2 = expert_ra_archive()
    df3 = nkr_ratings()
    df4 = nra_ratings()
    pd_df = pd.concat([df1, df3], ignore_index=True)
    pd_df = pd.concat([pd_df, df4], ignore_index=True)
    pd_df = pd.concat([pd_df, df2], ignore_index=True)

    pd_df["rat_date"] = pd_df["rat_date"].str.replace(".", "-", regex=False)
    pd_df["rat_date"] = pd.to_datetime(pd_df["rat_date"], format="%d-%m-%Y")

    pd_current_date = pd.to_datetime(current_date, format="%Y-%m-%d")

    # pd_df = pd_df[pd_df['rat_date'] == pd_current_date]
    pd_df = pd_df[pd_df["rat_date"] <= pd_current_date]

    spark = SparkSession.builder.master("yarn").appName("ratings_task").getOrCreate()

    df = spark.createDataFrame(pd_df)

    df.repartition(1).write.mode("overwrite").parquet(
        f"/user/tdkozachkin/project/DATA/DT={current_date}"
    )

    spark.stop()
