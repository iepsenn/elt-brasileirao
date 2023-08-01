import os


def create_blocks():
    from prefect_aws.credentials import AwsCredentials

    AwsCredentials(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    ).save("s3-credentials", overwrite=True)


if __name__ == "__main__":
    create_blocks()
