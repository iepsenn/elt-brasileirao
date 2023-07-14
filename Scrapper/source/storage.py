import s3fs
import pandas as pd


def get_s3_client():
    import os
    from dotenv import load_dotenv

    load_dotenv()

    ACCESS_KEY = os.environ.get("ACCESS_KEY")
    SECRET_KEY = os.environ.get("SECRET_KEY")
    S3_HOST = "https://host.docker.internal:9000"

    s3 = s3fs.S3FileSystem(
        endpoint_url=S3_HOST,
        key=ACCESS_KEY,
        secret=SECRET_KEY,
        anon=False,
        use_ssl=False,
    )
    return s3


def load_to_bucket(
    s3_client: s3fs.S3FileSystem,
    dataframe: pd.DataFrame,
    storage_path: str
):
    with s3_client.open(f"s3://{storage_path}", "w") as f:
        dataframe.to_csv(f)
