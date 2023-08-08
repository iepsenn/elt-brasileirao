# Scraper Brasileirao

<p>
<img alt="Docker" src="https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white"/>
<img alt="Prefect" src="https://img.shields.io/badge/Prefect-%23ffffff.svg?style=for-the-badge&logo=prefect&logoColor=blue"/>
<img alt="Clickhouse" src="https://img.shields.io/badge/ClickHouse-E9C46A?style=for-the-badge&logo=Clickhouse&logoColor=black"/>
<img alt="PySpark" src="https://img.shields.io/badge/spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white" />
<img alt="Pandas" src="https://img.shields.io/badge/Pandas-2C2D72?style=for-the-badge&logo=pandas&logoColor=white" />

---

## Project Organization

```markdown
├── README.md
├── docker                  <- The top-level README for developers using this project
├── clickhouse              <- Clickhouse artifacts and DB
├── minio
|   ├── bucket_setup.sh     <- Script to setup storage instance
│   └── data                <- MinIO artifacts and DB
├── prefect
|   ├── create_blocks.py    <- Script to store credentials in prefect blocks
|   ├── prefect.yaml        <- Config file to register prefect workflow
│   └── database            <- Prefect artifacts and DB
├── scraper                 <- Workflow code
|      ├── workflows
│           ├── src         <- Source code modules
│           └── elt.py      <- Main reference file for the workflow
│
└── build.sh                <- Script to build containers and setup services
```

---

## Launch the Project

```bash
sh build.sh
```

- `prefect` - Prefect UI, available on [http://localhost:4200](http://localhost:4200)
- `minio` - MinIO Storage UI, available on [http://localhost:9001](http://localhost:9001)
