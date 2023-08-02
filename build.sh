docker compose up -d --build && \
sleep 5 && \
docker exec -it minio sh bucket_setup.sh && \
docker exec -it prefect /opt/conda/envs/prefect/bin/python create_blocks.py && \
docker exec -it prefect /opt/conda/envs/prefect/bin/prefect work-pool create --type process etl-pool && \
docker exec -it prefect /opt/conda/envs/prefect/bin/prefect deploy -n elt  && \
docker exec -it prefect tmux new-session -d -s worker_start '/opt/conda/envs/prefect/bin/prefect worker start --pool elt-pool'
