# ⚙️ Lakehouse Pipelines (V1)

## 📌 Overview

This repository contains data pipelines for a local Lakehouse platform.

Pipelines are implemented using PySpark and follow the Medallion Architecture:

* Bronze
* Silver
* Gold

## 🧱 Pipelines

### Bronze

* Source: public dataset (municipios)
* Minimal transformation
* Adds ingestion metadata

Output:

```
lakehouse.bronze.municipios
```

---

### Silver

* Data cleaning and normalization
* Schema standardization

Output:

```
lakehouse.silver.municipios
```

---

### Gold

* Aggregated dataset
* Ready for analytical consumption

Example:

```
lakehouse.gold.municipios_by_state
```

Columns:

* state
* municipality_count
* reference_ingestion_date

---

## 🧠 Processing Engine

All pipelines are executed using **Apache Spark**.

* Iceberg tables
* Parquet storage
* Hive Metastore catalog

---

## ▶️ Running Pipelines

Example execution inside Spark container:

```bash
docker exec -it docker-spark-1 python /opt/project/src/gold/build_municipios_by_state.py
```

---

## 🔁 Orchestration

Pipelines are orchestrated using Airflow from the `lakehouse-infra` repository.

---

## 📁 Structure

```
src/
  bronze/
  silver/
  gold/
```

---

## 🎯 V1 Coverage

* Bronze ingestion ✔
* Silver transformation ✔
* Gold aggregation ✔

---

## 🚀 Future Improvements

* CDC ingestion
* Schema evolution automation
* Data quality checks
* Multiple datasets
