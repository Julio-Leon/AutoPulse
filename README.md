# AutoPulse — Traffic & Accident Analytics

AutoPulse is an end-to-end data engineering project designed to analyze historical vehicle collision data and integrate near-real-time traffic events into a unified analytics pipeline. The project aims to demonstrate how batch processing, stream processing, and data orchestration can be combined to transform raw transportation data into structured, analytics-ready datasets.

The platform processes historical collision records through a batch pipeline while developing a streaming pipeline to ingest and process incoming traffic events. By integrating these data sources, AutoPulse aims to support analysis of collision patterns, traffic conditions, accident frequency, and transportation safety trends.

The project follows a layered data processing approach, organizing data into **Bronze, Silver, and Gold layers** to separate raw ingestion, cleaned and transformed records, and aggregated datasets prepared for analytics.

## Technology Stack

- **Python** — Data ingestion, transformation logic, validation, and streaming producers.
- **Apache Kafka (KRaft)** — Event streaming and message management without requiring ZooKeeper.
- **Apache Spark / PySpark** — Distributed data processing, batch transformations, and Structured Streaming.
- **Apache Airflow** — Workflow orchestration, scheduling, and pipeline dependency management.
- **Apache Parquet** — Columnar storage format for efficient analytical processing.
- **AWS S3** — Data lake for the Bronze, Silver, and Gold layers.
- **AWS Lambda** — Serverless ingestion of the NYC collisions API into S3 Bronze.
- **AWS Glue** — Serverless Spark jobs for Bronze → Silver (same PySpark code as local runs).
- **AWS EC2** — Host running Kafka, the vehicle simulator, Spark Structured Streaming, and Airflow via Docker Compose.
- **boto3 deploy script** — Lambda, Glue jobs and IAM roles deployed as code (`infra/aws/deploy.py`, policies in `infra/aws/iam/`).
- **Snowflake** — Analytics warehouse: Snowpipe auto-ingestion from S3 and a star schema (facts and dimensions) built in SQL.
- **Docker & Docker Compose** — Containerized services and reproducible local development environments.
- **Git & GitHub** — Version control, source code management, and collaboration.
- **VS Code & WSL 2 (Ubuntu)** — Linux-based development environment on Windows.

## Data Pipeline Architecture

1. **Data Ingestion:** Load historical vehicle collision datasets and introduce traffic events through a streaming producer.
2. **Batch Processing:** Use Python and PySpark to validate, clean, transform, and enrich historical records.
3. **Stream Processing:** Use Kafka to transport traffic events and Spark Structured Streaming to process incoming data.
4. **Data Lake Organization:** Store data in Bronze, Silver, and Gold layers, using Parquet for analytical datasets where appropriate.
5. **Batch-Stream Integration:** Combine historical records with processed streaming data to support unified transportation analytics.
6. **Orchestration:** Use Airflow to schedule workflows, manage dependencies, and monitor pipeline execution.
7. **Warehouse:** Snowpipe loads Silver data from S3 into Snowflake, where fact and dimension tables are modeled.
8. **Analytics & Visualization:** A dashboard on top of Snowflake for fleet status, collision trends, and hotspots.
9. **Cloud Deployment:** Lambda and Glue run the batch steps, and EC2 runs the always-on streaming services. The same code also runs locally against disk or S3.

```
                    ┌──────────── Airflow (EC2) schedules and monitors ────────────┐
                    ▼                                                              ▼
NYC Collisions API ─► Lambda ──────────────┐                                  Glue jobs
                                           ├─► S3 Bronze ──────────────────► (Spark) ─► S3 Silver
10-vehicle simulator ─► Kafka ─► Spark SS ─┘   (EC2: Kafka, simulator, streaming)            │
                                                                                             ▼
                                                     Snowflake: Snowpipe ─► star schema ─► views
                                                                                             │
                                                                                             ▼
                                                                                         Dashboard
```

## Configuration

All paths and connection settings live in `src/common/config.py` and are read from environment variables or a `.env` file. Copy `.env.example` to `.env` to get started.

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATA_ROOT` | `./data` | Where Bronze/Silver/Gold live. Set to `s3://<bucket>` to use S3. |
| `CHECKPOINT_ROOT` | `./checkpoints` | Spark streaming checkpoints. |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` | Kafka broker address. |
| `STREAM_TRIGGER_INTERVAL` | `1 minute` | How often streaming writes a Bronze file. |
| `MAINTENANCE_INTERVAL_MILES` / `MAINTENANCE_DUE_WINDOW_MILES` | `5000` / `250` | Fleet service rule. |

## Running the pipelines

Run from the project root (either `python src/...py` or `python -m src....` works):

```bash
docker compose up -d                                   # Kafka + Kafka UI

# Collisions (batch)
python src/ingestion/nyc_collisions_api.py --days-back 30
python src/batch/transform_collisions.py
python src/batch/transform_gold.py

# Vehicle telemetry (streaming), each in its own terminal
python src/streaming/vehicle_telemetry_producer.py
python src/streaming/vehicle_telemetry_stream.py
# then
python src/batch/transform_vehicle_telemetry.py
python src/batch/transform_vehicle_gold.py

pytest                                                 # data-quality checks
```

AWS setup steps are in [`infra/aws/README.md`](infra/aws/README.md). In the cloud everything runs on its own:

```bash
python infra/aws/deploy.py           # IAM roles, Lambda, Glue jobs, code bundle (step 9)
python infra/aws/ec2_host.py launch  # EC2 host: Kafka, simulator, streaming, Airflow (step 10)
```

Airflow then runs collisions daily (Lambda → Glue) and telemetry every 15 minutes (Glue).
Snowflake setup (Snowpipe, star schema, views) is in [`snowflake/README.md`](snowflake/README.md).

## Development Environment

Development takes place in VS Code connected to Ubuntu through WSL 2. Docker Compose is used to manage local infrastructure services, while Git and GitHub provide version control and enable the project to be developed across multiple machines.

The environment is designed to keep application code, service configuration, and dependencies reproducible across development machines.

## Project Status

- [x] Development environment verified
- [x] Local Kafka infrastructure
- [x] Historical batch pipeline
- [x] Streaming pipeline (Kafka → Spark → Bronze/Silver/Gold)
- [x] Environment-driven config (local or S3)
- [x] AWS S3 data lake
- [x] Lambda ingestion + Glue Silver jobs (deployed)
- [ ] EC2 host (Kafka, simulator, streaming, Airflow)
- [ ] Snowflake ingestion (Snowpipe) and star schema
- [ ] Batch-stream integration (fleet vs. collision hotspots)
- [ ] Airflow orchestration (DAGs written; runs on the EC2 host)
- [ ] Dashboard development

## Project Goals

- Build a reproducible, end-to-end data engineering pipeline.
- Demonstrate batch and streaming data processing using Apache Spark and Kafka.
- Apply data validation, transformation, and layered data lake design.
- Automate pipeline execution and dependencies with Airflow.
- Develop practical analytics around vehicle collisions and traffic events.
- Establish a portable development environment that can transition from local development to cloud infrastructure.