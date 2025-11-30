

# Libraries needed to produce the sumulated transactions
from random import randint

from typing import Dict, Any, Optional

from datetime import datetime, timezone, timedelta
import time
import random
import json
import os
import signal
import logging

from confluent_kafka import Producer
from faker import Faker
from jsonschema import validate, ValidationError, FormatChecker
from dotenv import load_dotenv

# Configure logging using a dictionary for cleaner, maintainable setup 
# (sets log level and message format, passed into basicConfig).


log_config = {
    "level": logging.INFO,
    "format": "%(asctime)s - %(levelname)s - %(module)s - %(message)s"
}
logging.basicConfig(**log_config)
logger = logging.getLogger(__name__)


# Load environment variables
load_dotenv(dotenv_path="../.env")


# Faker instance for generating random data
fake = Faker()


# -----------------------------------------------------------
# Faker and JSON Schema for validation purposes
# -----------------------------------------------------------
fake = Faker()

TRANSACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "transaction_id": {"type": "string"},
        "user_id": {"type": "number", "minimum": 1000, "maximum": 9999},
        "amount": {"type": "number", "minimum": 0.01, "maximum": 100000},
        "currency": {"type": "string", "pattern": "^[A-Z]{3}$"},
        "merchant": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"},
        "location": {"type": "string", "pattern": "^[A-Z]{2}$"},
        "is_fraud": {"type": "integer", "minimum": 0, "maximum": 1},
    },
    "required": ["transaction_id", "user_id", "amount", "currency", "timestamp", "is_fraud"],
}


class SimulatedTransactionProducer:
    def __init__(self):
        self.kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.kafka_uname = os.getenv("KAFKA_USERNAME")
        self.kafka_pwd = os.getenv("KAFKA_PASSWORD")
        self.topic = os.getenv("KAFKA_TOPIC", "VishwaSimulatedTransactions")
        self.running = False

        # Producer configuration for Confluent Kafka
        self.producer_config = {
            "bootstrap.servers": self.kafka_servers,
            "client.id": "simulated-transaction-producer",
            "compression.type": "gzip",
            "linger.ms": 5,
            "batch.size": 15000,
        }

        if self.kafka_uname and self.kafka_pwd:
            self.producer_config.update({
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": self.kafka_uname,
                "sasl.password": self.kafka_pwd,
            })
        else:
            self.producer_config["security.protocol"] = "PLAINTEXT"

        try:
            self.producer = Producer(self.producer_config)
            logger.info("Confluent Kafka Producer initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Confluent Kafka Producer: {str(e)}")
            raise e

        self.compromised_users = set(random.sample(range(1000, 9999), 50))  # 0.5% of users
        self.high_risk_merchants = ['QuickCash', 'GlobalDigital', 'FastMoneyX']
        self.fraud_pattern_weights = {
            'account_takeover': 0.4,  # 40% of fraud cases comprise of account takeover
            'card_testing': 0.3,  # 30% of fraud cases comprise of card testing
            'merchant_collusion': 0.2,  # 20% of fraud cases comprise of merchant collusion
            'geo_anomaly': 0.1  # 10% of fraud cases comprise of geo anomaly
        }

        # Configure graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def validate_transaction(self, transaction: Dict[str, Any]) -> bool:
        """Validate transaction against JSON schema with date-time checking"""
        try:
            validate(
                instance=transaction,
                schema=TRANSACTION_SCHEMA,
                format_checker=FormatChecker()
            )
            return True
        except ValidationError as e:
            logger.error(f"Invalid transaction: {e.message}")
            return False

    def SimulateTransaction(self) -> Optional[Dict[str, Any]]:
        """Generate transaction with 1-2% fraud rate using realistic patterns"""
        transaction = {
            "transaction_id": fake.uuid4(),
            "user_id": randint(1000, 9999),
            "amount": round(fake.pyfloat(min_value=0.01, max_value=10000), 2),  # Reduced max to $10k
            "currency": "USD",
            "merchant": fake.company(),
            "timestamp": (datetime.now(timezone.utc) +
                         timedelta(seconds=random.randint(-300, 300))).isoformat(),
            "location": fake.country_code(),
            "is_fraud": 0 
        }

        # Realistic fraud patterns (1.5% base rate ±0.5%)
        is_fraud = 0
        amount = transaction['amount']
        user_id = transaction['user_id']
        merchant = transaction['merchant']


    # Pattern 1: Account takeover which comprise of 0.6% of transactions
    # Fraud if a compromised user performs a large transaction
        if user_id in self.compromised_users and amount > 500:
            if random.random() < 0.3:  
                is_fraud = 1
                transaction['amount'] = random.uniform(500, 5000)  # Typical takeover amounts
                transaction['merchant'] = random.choice(self.high_risk_merchants)


    # Pattern 2: Card testing
    # Small rapid transactions used to test stolen cards
        if not is_fraud and amount < 2.0:
            if user_id % 1000 == 0 and random.random() < 0.25:
                is_fraud = 1
                transaction['amount'] = round(random.uniform(0.01, 2.0), 2)
                transaction['location'] = 'US'  

    # Pattern 3: Merchant collusion
    # Transactions at known high-risk merchants with unusual amounts
        if not is_fraud and merchant in self.high_risk_merchants:
            if amount > 300 and random.random() < 0.15:
                is_fraud = 1
                transaction['amount'] = random.uniform(300, 1500)


    # Pattern 4: Geographic anomalies
    # Sudden foreign transactions inconsistent with user behavior
        if not is_fraud:
            if user_id % 500 == 0 and random.random() < 0.1:
                is_fraud = 1
                transaction['location'] = random.choice(['CN', 'RU', 'NG'])  

    
    # Pattern 5: Baseline random fraud
    # Low-probability fraud for background noise

        if not is_fraud and random.random() < 0.002:
            is_fraud = 1
            transaction['amount'] = random.uniform(100, 2000)

    # Final fraud assignment ensuring overall 1–2% rate

        transaction['is_fraud'] = is_fraud if random.random() < 0.985 else 0

    # Validate the final transaction before returning
        if self.validate_transaction(transaction):
            return transaction
        return None


    # -----------------------------------------------------------
    # Callback function triggered after message delivery attempt
    # Logs success or failure for each message sent to Kafka
    # -----------------------------------------------------------
    def delivery_report(self, err, msg):
        """Delivery callback for confirming message delivery"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    # -----------------------------------------------------------
    # Produce a single transaction to Kafka
    # Handles transaction simulation, sending, and error logging
    # Returns True if message was produced successfully, else False
    # -----------------------------------------------------------

    def ProduceTransactionToKafka(self) -> bool:
        """Send a single transaction to Kafka with error handling"""
        try:
            transaction = self.SimulateTransaction()
            if not transaction:
                return False

            self.producer.produce(
                self.topic,
                key=transaction["transaction_id"],
                value=json.dumps(transaction),
                callback=self.delivery_report
            )

            self.producer.poll(0)  # Trigger callbacks
            return True

        except Exception as e:
            logger.error(f"Error producing message: {str(e)}")
            return False
        
    # -----------------------------------------------------------
    # Continuously produce transactions at the given interval
    # Handles graceful shutdown on SIGINT or SIGTERM
    # -----------------------------------------------------------

    def run_continuous_production(self, interval: float = 0.0):
        """Run continuous message production with graceful shutdown"""
        self.running = True
        logger.info("Starting producer for topic %s...", self.topic)

        try:
            while self.running:
                if self.ProduceTransactionToKafka():
                    time.sleep(interval)
        finally:
            self.shutdown()

    # -----------------------------------------------------------
    # Gracefully shutdown the producer
    # Ensures all queued messages are flushed and producer is closed
    # -----------------------------------------------------------

    def shutdown(self, signum=None, frame=None):
        """Graceful shutdown procedure"""
        if self.running:
            logger.info("Initiating shutdown...")
            self.running = False
            if self.producer:
                self.producer.flush(timeout=30)  # <-- Ensure flush() is called
                self.producer.close()
            logger.info("Producer stopped")

# -----------------------------------------------------------
# Entry point for the script
# Creates a producer instance and starts continuous production
# -----------------------------------------------------------

if __name__ == "__main__":
    producer = SimulatedTransactionProducer()
    producer.run_continuous_production()