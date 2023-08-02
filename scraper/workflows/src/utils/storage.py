import pandas as pd
import s3fs
from prefect_aws import AwsCredentials


def get_s3_client():
    credentials = AwsCredentials.load("s3-credentials")
    s3 = s3fs.S3FileSystem(
        endpoint_url="http://minio:9000",
        key=credentials.aws_access_key_id,
        secret=credentials.aws_secret_access_key.get_secret_value(),
        anon=False,
        use_ssl=False,
    )
    return s3


def load_to_bucket(
    s3_client: s3fs.S3FileSystem, dataframe: pd.DataFrame, storage_path: str
):
    # TODO: verify if bucket exists
    with s3_client.open(f"s3://{storage_path}", "w") as f:
        dataframe.to_csv(f, index=False)
