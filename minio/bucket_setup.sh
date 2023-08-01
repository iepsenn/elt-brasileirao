mc alias set minio http://localhost:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD} && \
mc admin user svcacct add --access-key ${AWS_ACCESS_KEY_ID} --secret-key ${AWS_SECRET_ACCESS_KEY} minio lukas && \
mc mb minio/brasileirao-data && \
mc anonymous set public minio/brasileirao-data
