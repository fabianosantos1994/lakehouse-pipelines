# Lakehouse Pipelines

## Overview

This repository contains the PySpark pipeline application for a local Lakehouse platform.

The current implementation follows the Medallion Architecture:

- Bronze: raw ingestion with minimal transformation
- Silver: cleaned and standardized data
- Gold: analytical, aggregated datasets

The application writes Apache Iceberg tables backed by object storage and a Hive Metastore catalog.

## Runtime Dependency

This repository is not self-sufficient. It depends on the `lakehouse-infra` repository for runtime.

`lakehouse-infra` provides:

- Docker Compose runtime
- Spark container
- MinIO object storage
- Hive Metastore
- PostgreSQL
- Airflow orchestration
- Trino query engine

At runtime, this repository is mounted into the Spark and Airflow containers at:

```text
/opt/project
```

The mount is configured in `lakehouse-infra/docker/.env`:

```env
PIPELINES_HOST_PATH=/path/to/lakehouse-pipelines
PIPELINES_CONTAINER_PATH=/opt/project
SPARK_CONTAINER_NAME=spark
```

## Repository Structure

```text
src/
  common/
    config.py      # Spark, catalog, Hive Metastore, and S3/MinIO config
    spark.py       # SparkSession builder
    logger.py      # Application logging setup
    job.py         # Shared job lifecycle

  configs/
    municipios.py  # Dataset-specific paths and Iceberg table names

  ingestion/
    bronze/
      ingest_municipios.py
    silver/
      silver_municipios.py

  processing/
    gold/
      build_municipios_by_state.py

datasets/
  raw/
    municipios.csv
```

## Pipeline Layers

### Bronze

Source:

```text
/opt/project/datasets/raw/municipios.csv
```

Job:

```text
src/ingestion/bronze/ingest_municipios.py
```

Output:

```text
lakehouse.bronze.municipios
```

### Silver

Job:

```text
src/ingestion/silver/silver_municipios.py
```

Output:

```text
lakehouse.silver.municipios
```

### Gold

Job:

```text
src/processing/gold/build_municipios_by_state.py
```

Output:

```text
lakehouse.gold.municipios_by_state
```

Columns:

- `state`
- `municipality_count`
- `reference_ingestion_date`

## Environment Configuration

The pipeline application reads Spark, Iceberg, Hive Metastore, S3/MinIO, and logging settings from environment variables.

| Variable | Default | Purpose |
| --- | --- | --- |
| `LAKEHOUSE_CATALOG` | `lakehouse` | Spark/Iceberg catalog name |
| `LAKEHOUSE_WAREHOUSE` | `s3a://lakehouse/warehouse` | Iceberg warehouse path |
| `HIVE_METASTORE_URI` | `thrift://hive-metastore:9083` | Hive Metastore URI |
| `S3_ENDPOINT` | `http://minio:9000` | S3-compatible object storage endpoint |
| `S3_ACCESS_KEY` | `minio` | S3/MinIO access key |
| `S3_SECRET_KEY` | `minio123` | S3/MinIO secret key |
| `S3_PATH_STYLE_ACCESS` | `true` | Enables path-style S3 access |
| `S3_SSL_ENABLED` | `false` | Enables or disables SSL for S3 access |
| `LOG_LEVEL` | `INFO` | Application log level |

These values are normally provided by the runtime environment from `lakehouse-infra`.

## Pipeline Configuration

Dataset-specific paths and table names are defined in:

```text
src/configs/municipios.py
```

Current values:

```text
INPUT_PATH=/opt/project/datasets/raw/municipios.csv
BRONZE_TABLE=lakehouse.bronze.municipios
SILVER_TABLE=lakehouse.silver.municipios
GOLD_TABLE=lakehouse.gold.municipios_by_state
```

## Prerequisites

- The `lakehouse-infra` repository is available locally.
- The Docker Compose stack from `lakehouse-infra/docker` is running.
- This repository is mounted into the runtime using `PIPELINES_HOST_PATH`.
- The Spark container is named `spark`.

## Running Pipelines

Run jobs from the `lakehouse-infra` runtime after the Docker Compose stack is running.

Bronze:

```bash
docker exec spark python /opt/project/src/ingestion/bronze/ingest_municipios.py
```

Silver:

```bash
docker exec spark python /opt/project/src/ingestion/silver/silver_municipios.py
```

Gold:

```bash
docker exec spark python /opt/project/src/processing/gold/build_municipios_by_state.py
```

## Orchestration

Pipelines are orchestrated by Airflow DAGs in the `lakehouse-infra` repository.

Airflow executes the same scripts inside the Spark container using:

```bash
docker exec spark python /opt/project/...
```

## Current Implemented Example

- Bronze municipios ingestion
- Silver municipios transformation
- Gold municipios aggregation by state
- Shared Spark session builder
- Shared job lifecycle wrapper
- Shared logger
- Environment-based Spark configuration

## Known Limitations

The current dataset ingestion may contain schema/header inconsistencies.

Data correctness validation, schema checks, and broader data quality rules are planned for a later phase.

## Future Improvements

- CDC ingestion
- Schema evolution automation
- Data quality checks
- Multiple datasets
