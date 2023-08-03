# Scraper Brasileirao

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

## :rocket: Launch the Project

```bash
docker compose up -d
```

- `prefect` - Prefect UI, available on [http://localhost:4200](http://localhost:4200)
- `minio` - MinIO Storage UI, available on [http://localhost:9001](http://localhost:9001)
