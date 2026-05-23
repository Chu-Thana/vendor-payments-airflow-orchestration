# 🛠 Airflow Data Pipeline Orchestration

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Orchestration](https://img.shields.io/badge/Orchestration-Airflow-orange)
![Streaming](https://img.shields.io/badge/Streaming-Kafka-purple)
![Cloud](https://img.shields.io/badge/Cloud-AWS-yellow)
![Data Lake](https://img.shields.io/badge/Data_Lake-S3-lightgrey)
![Warehouse](https://img.shields.io/badge/Warehouse-Redshift-red)
![Query](https://img.shields.io/badge/Query-Athena-blue)
![Format](https://img.shields.io/badge/Format-JSONL-lightgrey)
![Container](https://img.shields.io/badge/Container-Docker-blue)

---

## 📌 Summary

Production-style data orchestration system using Apache Airflow to unify batch and streaming pipelines.

- Orchestrates end-to-end workflows across ingestion → transformation → serving  
- Ensures reliability with retries, monitoring, and alerting  
- Handles at-least-once streaming with downstream deduplication  

👉 Airflow acts as the **central control layer of a modern data platform**

---

## 📊 Orchestration Metrics

- Orchestrated **4 production-style DAGs (11+ tasks)** across batch and streaming pipelines  
- Achieved **100% successful pipeline runs** with retry and alerting mechanisms  
- Implemented **automatic retry recovery (3 tasks, 2 retries)** for fault tolerance  
- Enabled **real-time alerting (success + failure)** for full observability  
- Reduced manual pipeline execution by **~64% (11 → 4 steps)** via automation  

👉 Metrics collected from controlled validation runs simulating production scenarios

---

## 🔗 Integration with Data Platform

This project sits at the **center of the data platform**:

- Project 1 → batch ETL and data modeling (analytics-ready datasets)
- Project 2 → API serving layer for data consumption
- Project 3 → real-time streaming ingestion (Kafka)
- Project 4 → orchestration, transformation, and deduplication (this project)
- Project 5 → cloud storage and warehouse (S3 / Redshift / Athena)

👉 Airflow acts as the **central orchestration layer connecting all components**

---

## 🔄 Data Flow

Kafka → Staging → Airflow Orchestration → Transform / Dedup → S3 (Silver/Gold) → Redshift / Athena → API / BI

👉 End-to-end **data pipeline orchestration with unified batch + streaming architecture**

---

## 🧭 Architecture Overview

This project demonstrates a **unified data pipeline orchestration layer** where both batch and streaming workflows are centrally managed using Apache Airflow.

Airflow acts as the **control layer** for coordinating batch input, Kafka streaming staging, validation, cleansing, downstream deduplication, retry handling, monitoring, and publishing to analytics-ready layers.

![Airflow Batch and Streaming Orchestration](assets/00_airflow-batch-streaming-orchestration.png)

**Design principle:** Airflow orchestrates batch and streaming workflows with validation, deduplication, retry handling, and monitoring before publishing data to the Silver and Gold layers.

### Key Responsibilities of Airflow in This Project

- Orchestrates both **batch** and **streaming** workflows
- Manages DAG-based task dependencies
- Performs validation and cleansing before publishing downstream data
- Applies downstream deduplication to improve analytics consistency
- Supports scheduling, retry handling, logging, and monitoring
- Publishes validated and deduplicated data into Silver and Gold layers
- Enables downstream analytics through Athena on the S3 Gold Layer

👉 **Batch and streaming pipelines are unified into a single orchestration workflow for downstream analytics.**

---

## ⚙️ Pipeline Flow

### 1️⃣ Extract (Staging Layer)
- Airflow reads streaming output from Kafka staging (JSONL / S3)
- Batch data is ingested from raw layer
- Schema is validated and normalized

### 2️⃣ Transform (Processing Layer)
- Airflow executes modular DAG tasks with dependency management  
- Data is cleaned and validated
- Deduplication is applied (downstream of Kafka at-least-once delivery)
- Business logic and aggregations are applied

### 3️⃣ Load (Storage & Serving Layer)
- Cleaned data is written to S3 Silver layer
- Aggregated data is promoted to S3 Gold layer
- Final datasets are loaded into Redshift for analytics

---

## 🧩 DAG Structure

This project contains multiple Airflow DAGs for different orchestration use cases:

- `project1_etl_runner.py` → orchestrates the original batch ETL workflow
- `sales_etl_pipeline.py` → runs sales ETL processing tasks
- `streaming_staging_pipeline.py` → processes Kafka staging data with downstream transformation and deduplication
- `redshift_mart_pipeline.py` → builds / updates Redshift mart tables for analytics

👉 These DAGs show that Airflow is used as a central orchestration layer, not just a single-task scheduler.

---

## 🔁 Deduplication Strategy

This system follows an **at-least-once delivery model**:

- Kafka ensures no data loss
- Duplicate events may occur due to reprocessing or consumer retries

### Design Decision

Deduplication is intentionally handled **downstream in Airflow**, not in the consumer layer.

👉 Reason:

- Avoids data loss in case of consumer failure
- Keeps the streaming layer lightweight and stateless
- Ensures correctness is enforced in a controlled batch processing environment

### Approach

- Use `event_id` as a unique identifier
- Deduplicate records during transformation (Airflow DAG)

### Guarantees

- No data loss (streaming ingestion layer)
- Data correctness (processing / warehouse layer)

👉 This reflects a real-world trade-off:  
**reliability first → correctness enforced downstream**
👉 This design prioritizes **data reliability over processing simplicity**, a common pattern in real-world data platforms

---

## ⚡ Scalability Design

- Airflow breaks workflows into modular DAG tasks, enabling parallel execution  
- Batch and streaming pipelines scale independently without coupling  
- S3 acts as a decoupled storage layer (compute vs storage separation)  
- Redshift scales analytical workloads independently from ingestion  

👉 This architecture supports **horizontal scaling across ingestion, processing, and serving layers**

---

## 🚨 Reliability & Failure Handling

- Kafka ensures **at-least-once delivery** (no data loss)  
- Airflow manages **task dependencies, retries, and execution monitoring**  
- Downstream deduplication guarantees data correctness despite duplicate events  
- Pipelines are **fully recoverable** from raw → silver → gold layers  

👉 Designed with **production-grade reliability, fault tolerance, and observability principles**

---

## 📸 Execution Proof

### 1️⃣ Orchestration Overview
![DAG](assets/01_orchestration_dag_overview.png)

### 2️⃣ DAG Execution Flow
![Execution](assets/02_pipeline_dag_execution.png)

### 3️⃣ Task Execution Logs
![Logs](assets/03_task_execution_logs.png)

### 4️⃣ Real-time Alert Monitoring
![Alert](assets/04_real_time_alert_monitoring.png)

### 5️⃣ Data Lake Output (S3 Silver Layer)
![S3](assets/05_data_lake_silver_output.png)

### 6️⃣ Pipeline Metrics Summary
![Metrics](assets/06_pipeline_metrics_summary.png)

Pipeline reliability and automation metrics collected from Airflow validation runs  
Demonstrates system stability, retry handling, and monitoring capabilities
- 4 DAGs, 11+ tasks orchestrated across batch and streaming pipelines  
- 100% validation success rate with retry and alerting  
- ~64% reduction in manual execution steps via automation  

---

## 🧠 What This Project Demonstrates

- Designing **production-ready orchestration systems** using Airflow  
- Integrating **batch and real-time streaming pipelines** into a unified architecture  
- Building systems with **fault tolerance, observability, and automated recovery**  
- Applying **real-world data engineering patterns (at-least-once + downstream correction)**  

👉 Demonstrates **end-to-end system thinking**, not just tool usage

---

## 💡 Key Takeaway

This project demonstrates how to design a **production-grade orchestration system**:

- Centralized control using Airflow  
- Decoupled, scalable data architecture  
- Reliability-first design with downstream correction  
- Measurable impact through automation and monitoring  

👉 Shows the ability to build **real-world data platforms**, not just pipelines
