# Real-Time Fraud Detection Pipeline

This repository contains a complete end-to-end **real-time fraud detection system** using Apache Kafka, Apache Spark, MLflow, MinIO, and Airflow. The system simulates financial transactions, detects potential fraud, and logs the results in real time.

---

## 🚀 Architecture Overview

<img src="https://github.com/user-attachments/assets/306e7e0f-2797-4c72-a717-030abecdb539" width="800" />


## Project Flow

### 1. Transaction Simulation (Producer)
- I created a **Python producer** that simulates realistic financial transactions.
- The producer introduces multiple **fraud patterns** (account takeover, card testing, merchant collusion, geo anomalies).
- Transactions are validated using a **JSON schema** before sending.
- All transactions are streamed to **Kafka topic**: `VishwaSimulatedTransactions`.

<img src="https://github.com/user-attachments/assets/33ee0b4d-bf37-40f6-93a9-b6a8414295de" width="400" />


### 2. Model Training (Airflow + MLflow)
- Using **Airflow**, I created a DAG that triggers training whenever needed.
- The DAG reads all data currently in the first Kafka topic (`VishwaSimulatedTransactions`) for training.
- The trained model artifacts are stored in **MinIO**, and MLflow tracks all experiments and keeps **versioned models**.
- MLflow allows us to automatically select the **best-performing model** based on validation metrics.
<img src="https://github.com/user-attachments/assets/0c4f13af-a0ee-4b5b-bac7-1af427ba5ae4" width="400" />

<img src="https://github.com/user-attachments/assets/1c5193b1-4dd4-4092-995e-613aa3b058b1" width="400" />



### 3. Inference Pipeline (Spark Streaming)
- The inference pipeline reads **real-time transactions** from the first Kafka topic.
- Features are engineered in Spark (e.g., transaction hour, weekend/night flags, merchant risk, rolling averages).
- The pipeline loads the **best model** from MinIO/MLflow.
- Predictions are made using a **PySpark pandas UDF**.
- Transactions flagged as fraud are sent to a **second Kafka topic**: `predicted`.

<img src="https://github.com/user-attachments/assets/a10d356d-4188-497d-9445-8f7cb73ae5b4" width="400" />


### 4. Overall Architecture
- **Producer** streams transaction data → **Kafka** topic 1.
- **Airflow DAG** triggers model training using topic 1 data → stores model artifacts in MinIO → logs experiment in MLflow.
- **Inference pipeline** reads topic 1 → uses best model from MinIO → predicts fraud → sends results to topic 2 (`predicted`).

### 5. Containerization
- All components are **dockerized**:
  - Producer, Spark streaming inference, MLflow, MinIO, Kafka, Redis, PostgreSQL, Airflow.
- **Docker Compose** orchestrates the full stack with proper environment variables, volumes, and health checks.
- This setup makes the project **reproducible and scalable**.

---

## How to Run

1. Clone the repository:
   ```bash
   git clone https://github.com/<your-username>/<repo-name>.git
   cd <repo-name>

---
