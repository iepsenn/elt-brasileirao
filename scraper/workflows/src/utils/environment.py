from prefect_aws import AwsCredentials
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

BUCKET_NAME = "brasileirao-data"


def build_spark_session(name: str):
    # credentials to storage
    credentials = AwsCredentials.load("s3-credentials")
    access_key = credentials.aws_access_key_id
    secret_key = credentials.aws_secret_access_key.get_secret_value()

    # configuration to access s3 storage
    conf = SparkConf()
    conf.set("spark.jars.packages", (
        "org.apache.hadoop:hadoop-aws:3.2.2,"
        "com.github.housepower:clickhouse-native-jdbc:2.6.5"
    ))
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
        .appName(name)
        .getOrCreate()
    )

    return spark
