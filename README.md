# Real-Time Fraud Detection Pipeline

This repository contains a complete end-to-end **real-time fraud detection system** using Apache Kafka, Apache Spark, MLflow, MinIO, and Airflow. The system simulates financial transactions, detects potential fraud, and logs the results in real time.

---

## 🚀 Architecture Overview

<img width="1800" height="1000" alt="Screenshot From 2025-11-29 18-07-09" src="https://github.com/user-attachments/assets/1370cb15-204c-423b-9657-fb1d3bb75dfd" />


The project is divided into several components:

### 1. **Transaction Producer**
- Simulates realistic financial transactions with a small percentage flagged as fraudulent.
- Implements multiple fraud patterns (account takeover, card testing, merchant collusion, geo anomalies).
- Produces transactions to a **Kafka topic**.
- Built with Python and `confluent_kafka`.

### 2. **Kafka**
- Apache Kafka is used as the message broker.
- Producer sends transaction messages to the topic `VishwaSimulatedTransactions`.
- Spark reads these messages for inference.

### 3. **Inference Pipeline (Spark Streaming)**
- Reads transactions in real-time from Kafka.
- Performs feature engineering (e.g., transaction hour, weekend, merchant risk, rolling averages).
- Loads the best fraud detection model from **MLflow**.
- Runs inference using a PySpark **pandas UDF**.
- Writes predicted fraud transactions to the Kafka topic `predicted`.

### 4. **MLflow + MinIO**
- **MLflow** is used to track experiments, register models, and manage versions.
- **MinIO** serves as the S3-compatible artifact storage for MLflow.
- Models are versioned and the best-performing version is loaded for inference.

### 5. **Airflow**
- Orchestrates ETL, model training, and inference workflows.
- CeleryExecutor with Redis as the broker and PostgreSQL as the metadata DB.
- DAGs can trigger producer or inference tasks automatically.

---
