"""
Real-time Fraud Detection Inference Pipeline
"""

import logging
import os
import joblib
import yaml
import mlflow.pyfunc
from dotenv import load_dotenv
import numpy as np

import mlflow
from mlflow.tracking import MlflowClient
# PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, hour, dayofmonth, dayofweek, when, lit, coalesce
)
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


class FraudDetectionInference:

    def __init__(self, config_path="/app/config.yaml"):
        load_dotenv("/app/.env")
        self.config = self._load_config(config_path)
        self.spark = self._init_spark_session()

        # self.model = self._load_model(self.config["model"]["path"])
        self.model = self._load_best_model()
        self.broadcast_model = self.spark.sparkContext.broadcast(self.model)

        logger.info("Inference service initialized.")

    # --------------------------------------------------------------
    # CONFIG + SPARK
    # --------------------------------------------------------------
    @staticmethod
    def _load_config(path):
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def _init_spark_session(self):
        packages = self.config.get("spark", {}).get("packages", "")
        builder = SparkSession.builder.appName("FraudDetectionInferenceStreaming")
        if packages:
            builder = builder.config("spark.jars.packages", packages)

        spark = builder.getOrCreate()
        logger.info("Spark session initialized.")
        return spark

    def _load_model(self, path):
        model = joblib.load(path)
        logger.info("Model loaded from %s", path)
        return model
    


    def _load_best_model(self, metric_name="auc_pr"):
        mlflow_config = self.config['mlflow']
        model_name = mlflow_config['registered_model_name']
        mlflow.set_tracking_uri(mlflow_config['tracking_uri'])
        os.environ['MLFLOW_S3_ENDPOINT_URL'] = mlflow_config['s3_endpoint_url']
        os.environ['AWS_ACCESS_KEY_ID'] = os.getenv("AWS_ACCESS_KEY_ID")
        os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        client = MlflowClient()
        versions = client.get_latest_versions(name=model_name)
        
        # Filter only "READY" stages if you like (optional)
        ready_versions = [v for v in versions if v.current_stage == "Production" or v.current_stage == "None"]
        
        if not ready_versions:
            raise ValueError(f"No versions found for model {model_name}")
        
        # Get version with highest metric
        best_version = None
        best_score = -float("inf")
        
        for v in ready_versions:
            run_id = v.run_id
            metrics = client.get_run(run_id).data.metrics
            score = metrics.get(metric_name, -float("inf"))
            if score > best_score:
                best_score = score
                best_version = v
        
        if not best_version:
            raise ValueError(f"No model found with metric {metric_name}")
        
        logger.info("Loading best model version: %s with %s = %.4f", 
                    best_version.version, metric_name, best_score)
        
        model_uri = f"models:/{model_name}/{best_version.version}"
        model = mlflow.pyfunc.load_model(model_uri)
        
        logger.info("Successfully loaded model: %s", model_uri)
        return model


    # --------------------------------------------------------------
    # KAFKA INPUT
    # --------------------------------------------------------------
    def read_from_kafka(self):
        kafka = self.config["kafka"]

        # SAFE CONFIG ACCESS (NO KEYERROR)
        bootstrap = kafka.get("bootstrap_servers", "localhost:9092")
        topic = kafka["topic"]
        protocol = kafka.get("security_protocol", "PLAINTEXT")
        mechanism = kafka.get("sasl_mechanism", "PLAIN")
        username = kafka.get("username", "")
        password = kafka.get("password", "")

        jaas = (
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{username}" password="{password}";'
        )

        # STORE THESE FOR OUTPUT TOPIC WRITER
        self.bootstrap = bootstrap
        self.protocol = protocol
        self.mechanism = mechanism
        self.jaas = jaas

        schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("merchant", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
        ])

        df = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("kafka.security.protocol", protocol)
            .option("kafka.sasl.mechanism", mechanism)
            .option("kafka.sasl.jaas.config", jaas)
            .load()
        )

        parsed = (
            df.selectExpr("CAST(value AS STRING)")
              .select(from_json(col("value"), schema).alias("data"))
              .select("data.*")
        )

        return parsed

    # --------------------------------------------------------------
    # FEATURE ENGINEERING
    # --------------------------------------------------------------
    def add_features(self, df):

        df = df.withColumn("transaction_hour", hour("timestamp"))
        df = df.withColumn("is_night",
                           when((col("transaction_hour") >= 22) |
                                (col("transaction_hour") < 5), 1).otherwise(0))

        df = df.withColumn(
            "is_weekend",
            when((dayofweek(col("timestamp")) == 1) |
                 (dayofweek(col("timestamp")) == 7), 1).otherwise(0)
        )

        df = df.withColumn("transaction_day", dayofmonth("timestamp"))

        # Same placeholder features you used previously
        df = df.withColumn("time_since_last_txn", lit(0.0))
        df = df.withColumn("user_activity_24h", lit(1000))
        df = df.withColumn("rolling_avg_7d", lit(1000.0))

        df = df.withColumn("amount_to_avg_ratio", col("amount") / col("rolling_avg_7d"))
        df = df.withColumn("amount_to_avg_ratio", coalesce("amount_to_avg_ratio", lit(1.0)))

        risky = self.config.get("high_risk_merchants",
                                ["QuickCash", "GlobalDigital", "FastMoneyX"])

        df = df.withColumn("merchant_risk", col("merchant").isin(risky).cast("int"))

        df.printSchema()
        return df

    # --------------------------------------------------------------
    # INFERENCE PIPELINE
    # --------------------------------------------------------------
    def run_inference(self):
        import pandas as pd

        df = self.read_from_kafka()
        df = df.withWatermark("timestamp", "24 hours")
        df = self.add_features(df)

        model = self.broadcast_model
        
        @pandas_udf("int")
        def predict_udf(
                user_id: pd.Series,
                amount: pd.Series,
                currency: pd.Series,
                transaction_hour: pd.Series,
                is_weekend: pd.Series,
                time_since_last_txn: pd.Series,
                merchant_risk: pd.Series,
                amount_to_avg_ratio: pd.Series,
                is_night: pd.Series,
                transaction_day: pd.Series,
                user_activity_24h: pd.Series,
                merchant: pd.Series
        ) -> pd.Series:

            batch = pd.DataFrame({
                "user_id": user_id,
                "amount": amount,
                "currency": currency,
                "transaction_hour": transaction_hour,
                "is_weekend": is_weekend,
                "time_since_last_txn": time_since_last_txn,
                "merchant_risk": merchant_risk,
                "amount_to_avg_ratio": amount_to_avg_ratio,
                "is_night": is_night,
                "transaction_day": transaction_day,
                "user_activity_24h": user_activity_24h,
                "merchant": merchant
            })

            # probs = model.value.predict_proba(batch)[:, 1]
            # preds = model.value.predict(batch)  # returns whatever the pyfunc returns
            # if your model returns probabilities directly, you can use:
            # probs = np.array(preds)[:, 1]
            # preds = (probs >= 0.60).astype(int)
            # return pd.Series(preds)
        
            preds = model.value.predict(batch)
            preds = np.array(preds)

            # Case 1: model returns probability of positive class → 1D array
            if preds.ndim == 1:
                probs = preds

            # Case 2: model returns 2D array → probability matrix
            elif preds.ndim == 2:
                probs = preds[:, 1]

            else:
                raise ValueError(f"Unexpected prediction shape: {preds.shape}")

            # Threshold at 0.60
            labels = (probs >= 0.60).astype(int)
            return pd.Series(labels)




        prediction_df = df.withColumn(
            "prediction",
            predict_udf(
                *[col(f) for f in [
                    "user_id", "amount", "currency", "transaction_hour",
                    "is_weekend", "time_since_last_txn", "merchant_risk",
                    "amount_to_avg_ratio", "is_night", "transaction_day",
                    "user_activity_24h", "merchant"
                ]]
            )
        )



        fraud_df = prediction_df.filter(col("prediction") == 1)
        # fraud_df = prediction_df

        (
            fraud_df.selectExpr(
                "CAST(transaction_id AS STRING) AS key",
                "to_json(struct(*)) AS value"
            )
            .writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap)
            .option("kafka.security.protocol", self.protocol)
            .option("kafka.sasl.mechanism", self.mechanism)
            .option("kafka.sasl.jaas.config", self.jaas)
            .option("topic", "predicted")
            .option("checkpointLocation", "checkpoints/checkpoint")
            .outputMode("update")
            .start()
            .awaitTermination()
        )


# --------------------------------------------------------------
# MAIN
# --------------------------------------------------------------
if __name__ == "__main__":
    pipeline = FraudDetectionInference("/app/config.yaml")
    pipeline.run_inference()
