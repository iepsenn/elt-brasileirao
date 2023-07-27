import os
from typing import Optional

from prefect.blocks.core import Block
from pydantic import SecretStr


class S3Credentials(Block):
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[SecretStr] = None
    endpoint_url: Optional[str] = None,
    anon: bool = False,
    use_ssl: bool = False,


def create_blocks():
    credentials = S3Credentials(
        endpoint_url="https://host.docker.internal:9000",
        aws_access_key_id=os.environ.get("ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("SECRET_KEY"),
    )
    credentials.save(name="s3-credentials")


if __name__ == "__main__":
    create_blocks()
