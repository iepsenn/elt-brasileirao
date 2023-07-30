FROM prefecthq/prefect:2-python3.10-conda

WORKDIR /opt/prefect

# install requirements
COPY requirements.txt .
RUN pip install --upgrade pip --no-cache-dir
RUN pip install -r requirements.txt
RUN pip install prefect-aws

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean;

# apt install -y software-properties-common && \
# add-apt-repository -y ppa:webupd8team/java && \

# install aws cli and nano editor
RUN apt update && \
    apt install -y nano curl unzip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    aws/install --update

EXPOSE 4200

# copy script to register credentials blocks in prefect
COPY prefect/create_blocks.py .

# crate bucket on minio storage
ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
RUN aws --endpoint-url http://host.docker.internal:9000 \
    s3 mb --acl public-read-write \
    --bucket s3://brasileirao-data 

# start prefect server
CMD prefect config set PREFECT_UI_API_URL="http://localhost:4200/api" && \ 
    prefect server start --host 0.0.0.0
