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
- **PostgreSQL** — Relational storage for metadata, pipeline tracking, and supporting application data.
- **AWS S3** — Cloud object storage for data lake layers and processed datasets.
- **AWS services** — Cloud infrastructure and deployment components to be incorporated as the project evolves.
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
7. **Analytics & Visualization:** Develop a dashboard for exploring collision trends, traffic patterns, and other relevant metrics.
8. **Cloud Deployment:** Extend the local architecture to AWS for cloud-based storage and processing.

## Development Environment

Development takes place in VS Code connected to Ubuntu through WSL 2. Docker Compose is used to manage local infrastructure services, while Git and GitHub provide version control and enable the project to be developed across multiple machines.

The environment is designed to keep application code, service configuration, and dependencies reproducible across development machines.

## Project Status

- [x] Development environment verified
- [x] Local Kafka infrastructure
- [x] Historical batch pipeline
- [ ] Streaming pipeline
- [ ] Batch-stream integration
- [ ] Airflow orchestration
- [ ] Dashboard development
- [ ] AWS deployment

## Project Goals

- Build a reproducible, end-to-end data engineering pipeline.
- Demonstrate batch and streaming data processing using Apache Spark and Kafka.
- Apply data validation, transformation, and layered data lake design.
- Automate pipeline execution and dependencies with Airflow.
- Develop practical analytics around vehicle collisions and traffic events.
- Establish a portable development environment that can transition from local development to cloud infrastructure.