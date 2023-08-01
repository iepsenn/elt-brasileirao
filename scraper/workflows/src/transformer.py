from datetime import timedelta

import pyspark.sql.functions as sf
import src.schemas as s
from prefect_aws import AwsCredentials
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from src.utils import BUCKET_NAME

from prefect import flow, task
from prefect.tasks import task_input_hash


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def transform_summary_data(spark: SparkSession, season_year: int):
    (
        spark.read.schema(s.SummarySchema)
        .csv(
            path=f"s3a://{BUCKET_NAME}/raw/{season_year}/*/*/*/Summary*.csv",
            header=True,
        )
        .withColumn("position", sf.split(sf.col("position"), ","))
        .withColumn("age", sf.split(sf.col("age"), "-")[0].cast(IntegerType()))
        .drop("dummy1", "dummy2", "dummy3", "dummy4", "dummy5")
        .repartition(1)
        .write.mode("overwrite")
        .parquet(f"s3a://{BUCKET_NAME}/refined/{season_year}/Summary.parquet")
    )


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def transform_passing_data(spark: SparkSession, season_year: int):
    (
        spark.read.schema(s.PassingSchema)
        .csv(
            path=f"s3a://{BUCKET_NAME}/raw/{season_year}/*/*/*/Passing*.csv",
            header=True,
        )
        .withColumn("position", sf.split(sf.col("position"), ","))
        .withColumn("age", sf.split(sf.col("age"), "-")[0].cast(IntegerType()))
        .drop("dummy1", "dummy2")
        .repartition(1)
        .write.mode("overwrite")
        .parquet(f"s3a://{BUCKET_NAME}/refined/{season_year}/Passing.parquet")
    )


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def transform_passing_types_data(spark: SparkSession, season_year: int):
    (
        spark.read.schema(s.PassTypesSchema)
        .csv(
            path=f"s3a://{BUCKET_NAME}/raw/{season_year}/*/*/*/PassTypes*.csv",
            header=True,
        )
        .withColumn("position", sf.split(sf.col("position"), ","))
        .withColumn("age", sf.split(sf.col("age"), "-")[0].cast(IntegerType()))
        .repartition(1)
        .write.mode("overwrite")
        .parquet(f"s3a://{BUCKET_NAME}/refined/{season_year}/PassTypes.parquet")
    )


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def transform_defensive_actions_data(spark: SparkSession, season_year: int):
    (
        spark.read.schema(s.DefensiveActionsSchema)
        .csv(
            path=f"s3a://{BUCKET_NAME}/raw/{season_year}/*/*/*/DefensiveActions*.csv",
            header=True,
        )
        .withColumn("position", sf.split(sf.col("position"), ","))
        .withColumn("age", sf.split(sf.col("age"), "-")[0].cast(IntegerType()))
        .repartition(1)
        .write.mode("overwrite")
        .parquet(f"s3a://{BUCKET_NAME}/refined/{season_year}/DefensiveActions.parquet")
    )


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def transform_possession_data(spark: SparkSession, season_year: int):
    (
        spark.read.schema(s.PossessionSchema)
        .csv(
            path=f"s3a://{BUCKET_NAME}/raw/{season_year}/*/*/*/Possession*.csv",
            header=True,
        )
        .withColumn("position", sf.split(sf.col("position"), ","))
        .withColumn("age", sf.split(sf.col("age"), "-")[0].cast(IntegerType()))
        .repartition(1)
        .write.mode("overwrite")
        .parquet(f"s3a://{BUCKET_NAME}/refined/{season_year}/Possession.parquet")
    )


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def transform_miscellaneous_stats_data(spark: SparkSession, season_year: int):
    (
        spark.read.schema(s.MiscellaneousStatsSchema)
        .csv(
            path=f"s3a://{BUCKET_NAME}/raw/{season_year}/*/*/*/MiscellaneousStats*.csv",
            header=True,
        )
        .withColumn("position", sf.split(sf.col("position"), ","))
        .withColumn("age", sf.split(sf.col("age"), "-")[0].cast(IntegerType()))
        .repartition(1)
        .write.mode("overwrite")
        .parquet(f"s3a://{BUCKET_NAME}/refined/{season_year}/MiscellaneousStats.parquet")
    )


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def transform_goalkeeper_data(spark: SparkSession, season_year: int):
    (
        spark.read.schema(s.GoalkeeperSchema)
        .csv(
            path=f"s3a://{BUCKET_NAME}/raw/{season_year}/*/*/*/Goalkeeper*.csv",
            header=True,
        )
        .withColumn("age", sf.split(sf.col("age"), "-")[0].cast(IntegerType()))
        .drop("dummy1")
        .repartition(1)
        .write.mode("overwrite")
        .parquet(f"s3a://{BUCKET_NAME}/refined/{season_year}/Goalkeeper.parquet")
    )


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def transform_season_schedule_data(spark: SparkSession, season_year: int):
    (
        spark.read.schema(s.SeasonScheduleSchema)
        .csv(
            path=f"s3a://{BUCKET_NAME}/raw/{season_year}/SeasonSchedule*.csv",
            header=True,
        )
        .drop("dummy1", "dummy2", "dummy3", "dummy4")
        .repartition(1)
        .write.mode("overwrite")
        .parquet(f"s3a://{BUCKET_NAME}/refined/{season_year}/SeasonSchedule.parquet")
    )


@flow(retries=5, retry_delay_seconds=5, log_prints=True)
def transform_raw_data(season_year: int):
    # credentials to storage
    credentials = AwsCredentials.load("s3-credentials")
    access_key = credentials.aws_access_key_id
    secret_key = credentials.aws_secret_access_key.get_secret_value()

    # configuration to access s3 storage
    conf = SparkConf()
    conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2")
    conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    conf.set("spark.hadoop.fs.s3a.access.key", access_key)
    conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)
    conf.set("spark.hadoop.fs.s3a.proxy.host", "minio")
    conf.set("spark.hadoop.fs.s3a.endpoint", "minio")
    conf.set("spark.hadoop.fs.s3a.proxy.port", "9000")
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    spark = (
        SparkSession.builder.config(conf=conf)
        .master("local[*]")
        .appName("TransformData")
        .getOrCreate()
    )

    transform_summary_data(spark, season_year)
    transform_passing_data(spark, season_year)
    transform_passing_types_data(spark, season_year)
    transform_defensive_actions_data(spark, season_year)
    transform_possession_data(spark, season_year)
    transform_miscellaneous_stats_data(spark, season_year)
    transform_goalkeeper_data(spark, season_year)
    transform_season_schedule_data(spark, season_year)

    spark.stop()


if __name__ == "__main__":
    # example
    transform_raw_data(2023)
