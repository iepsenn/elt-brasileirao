import os

import pyspark.sql.functions as sf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType

from scraper.workflows.src.utils import BUCKET_NAME


def read_raw_file(spark: SparkSession, season_year: int, schema: list, stats_name: str):
    df = spark.read.csv(
        path=f"s3a://{BUCKET_NAME}/raw/{season_year}/*/*/*/{stats_name}*.csv",
        header=True,
        inferSchema=True
    )
    return df


def load_dataframe_to_database(df: DataFrame, season_year: int, stats_name: str):
    if stats_name != "SeasonSchedule":
        df = (
            df
            .withColumn("position", sf.regexp_replace(sf.col("position"), ',', '/'))
            .withColumn("age", sf.split(sf.col("age"), "-")[0].cast(IntegerType()))
            .withColumn("nationality", sf.split(sf.col("nationality"), " ")[1])
        )
    (
        df
        .na.fill(-1)
        .withColumn("season", season_year)
        .drop("dummy1", "dummy2", "dummy3", "dummy4", "dummy5")
        .repartition(1)
        .write.format("jdbc")
        .option("url", "jdbc:clickhouse://host.docker.internal:9090/stats")
        .option("driver", "com.github.housepower.jdbc.ClickHouseDriver")
        .option("dbtable", f"{os.environ.get('CLICKHOUSE_DB')}.{stats_name}")
        .option("createTableOptions", "engine=MergeTree() order by match_id")
        .option("user", os.environ.get("CLICKHOUSE_USER"))
        .option("password", os.environ.get("CLICKHOUSE_PASSWORD"))
        .option("truncate", "true")
        .option("batchsize", 1000)
        .option("isolationLevel", "NONE")
        .mode('append').save()
    )


def load_refined_data(spark: SparkSession, stats_name: str, season_year: int, schema: list):
    df = read_raw_file(spark, season_year, stats_name)
    load_dataframe_to_database(df.toDF(*schema), season_year, stats_name)
