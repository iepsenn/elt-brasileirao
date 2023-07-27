import s3fs
import pandas as pd


def get_s3_client():
    from prefect.blocks.system import JSON

    credentials = JSON.load("s3-credentials")
    s3 = s3fs.S3FileSystem(
        endpoint_url=credentials.value["endpoint_url"],
        key=credentials.value["aws_access_key_id"],
        secret=credentials.value["aws_secret_access_key"],
        anon=False,
        use_ssl=False,
    )
    return s3


def load_to_bucket(
    s3_client: s3fs.S3FileSystem,
    dataframe: pd.DataFrame,
    storage_path: str
):
    # TODO: verify if bucket exists
    with s3_client.open(f"s3://{storage_path}", "w") as f:
        dataframe.to_csv(f)
