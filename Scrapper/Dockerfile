FROM python:3.9.17-slim-bullseye as base

WORKDIR /scrapper

COPY requirements.txt .
RUN pip install --upgrade pip --no-cache-dir
RUN pip install -r requirements.txt

CMD [ "tail", "-f", "/dev/null"]