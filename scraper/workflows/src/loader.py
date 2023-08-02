from datetime import timedelta

from pyspark.sql import SparkSession

from prefect import flow, task
from prefect.tasks import task_input_hash
from scraper.workflows.src.utils import (build_spark_session,
                                         load_refined_data, read_schemas_file)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def load_summary_data(spark: SparkSession, season_year: int, schema: list):
    load_refined_data(spark, "Summary", season_year, schema)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def load_passing_data(spark: SparkSession, season_year: int, schema: list):
    load_refined_data(spark, "Passing", season_year, schema)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def load_pass_types_data(spark: SparkSession, season_year: int, schema: list):
    load_refined_data(spark, "PassTypes", season_year, schema)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def load_defensive_actions_data(spark: SparkSession, season_year: int, schema: list):
    load_refined_data(spark, "DefensiveActions", season_year, schema)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def load_possession_data(spark: SparkSession, season_year: int, schema: list):
    load_refined_data(spark, "Possession", season_year, schema)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def load_miscellaneous_stats_data(spark: SparkSession, season_year: int, schema: list):
    load_refined_data(spark, "MiscellaneousStats", season_year, schema)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def load_goalkeeper_data(spark: SparkSession, season_year: int, schema: list):
    load_refined_data(spark, "Goalkeeper", season_year, schema)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def load_season_schedule_data(spark: SparkSession, season_year: int, schema: list):
    load_refined_data(spark, "SeasonSchedule", season_year, schema)


@flow(retries=5, retry_delay_seconds=5, log_prints=True)
def load_data(season_year: int):
    spark = build_spark_session("LoadData")

    schemas = read_schemas_file()
    load_summary_data(spark, season_year, schemas.get("sumary", []))
    load_passing_data(spark, season_year, schemas.get("passing", []))
    load_pass_types_data(spark, season_year, schemas.get("pass_types", []))
    load_defensive_actions_data(spark, season_year, schemas.get("defensive_actions", []))
    load_possession_data(spark, season_year, schemas.get("possession", []))
    load_miscellaneous_stats_data(spark, season_year, schemas.get("miscellaneous_stats", []))
    load_goalkeeper_data(spark, season_year, schemas.get("goalkeeper", []))
    load_season_schedule_data(spark, season_year, schemas.get("season_schedule", []))

    spark.stop()
