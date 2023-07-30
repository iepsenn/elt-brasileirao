from source.schemas import GoalkeeperSchema
from source.schemas import MiscellaneousStatsSchema
from source.schemas import PossessionSchema
from source.schemas import DefensiveActionsSchema
from source.schemas import PassTypesSchema
from source.schemas import PassingSchema
from source.schemas import SummarySchema

import os
from prefect_aws import AwsCredentials
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import pyspark.sql.functions as sf
from pyspark.sql.types import IntegerType

from source.utils import BUCKET_NAME


# os.environ["PYSPARK_SUBMIT_ARGS"] = "--master local[2] pyspark-shell"

credentials = AwsCredentials.load("s3-credentials")
access_key = credentials.aws_access_key_id
secret_key = credentials.aws_secret_access_key.get_secret_value()

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
conf.set(
    'spark.hadoop.fs.s3a.aws.credentials.provider',
    'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider'
)

conf.set("spark.hadoop.fs.s3a.access.key", access_key)
conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)
conf.set("spark.hadoop.fs.s3a.proxy.host", "minio")
conf.set("spark.hadoop.fs.s3a.endpoint", "minio")
conf.set("spark.hadoop.fs.s3a.proxy.port", "9000")
conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark = SparkSession.builder \
    .config(conf=conf) \
    .master('local[*]') \
    .appName('TransformData') \
    .getOrCreate()

# summary
summary = spark.read.schema(SummarySchema).csv(
    f's3a://{BUCKET_NAME}/raw/2023/1/*/*/Summary*.csv',
    header=True)

summary.withColumn("age", sf.split(sf.col("age"), "-")[0].cast(IntegerType()))
summary.select("age", "position").show(5)

# # passing
# passing = spark.read.schema(PassingSchema).csv(
#     f's3a://{BUCKET_NAME}/raw/2023/1/*/*/Passing*.csv',
#     header=True)


# # pass types
# pass_types = spark.read.schema(PassTypesSchema).csv(
#     f's3a://{BUCKET_NAME}/raw/2023/1/*/*/PassTypes*.csv',
#     header=True)


# # defensive actions
# defensive_actions = spark.read.schema(DefensiveActionsSchema).csv(
#     f's3a://{BUCKET_NAME}/raw/2023/1/*/*/DefensiveActions*.csv',
#     header=True)


# # possesion
# possesion = spark.read.schema(PossessionSchema).csv(
#     f's3a://{BUCKET_NAME}/raw/2023/1/*/*/Possession*.csv',
#     header=True)


# # miscellaneous stats
# miscellaneous_stats = spark.read.schema(MiscellaneousStatsSchema).csv(
#     f's3a://{BUCKET_NAME}/raw/2023/1/*/*/MiscellaneousStats*.csv',
#     header=True)


# # goalkeeper
# goalkeeper = spark.read.schema(GoalkeeperSchema).csv(
#     f's3a://{BUCKET_NAME}/raw/2023/1/*/*/Goalkeeper*.csv',
#     header=True)


spark.stop()
