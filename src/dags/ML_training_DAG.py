from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'Vishwani',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 10),
    # 'execution_timeout': timedelta(minutes=120),
    'max_active_runs': 1
}

def _train_model(**context):
    """ Airflow wrapper for training task """
    # Example: replace with actual training code
    from model_training import FraudDetectionTraining
    try:
        logger.info('Initializing fraud detection training')
        trainer = FraudDetectionTraining()
        model, precision = trainer.train_model()

        return { 'status': 'success', 'precision':precision}
    except Exception as e:
        logger.error('Training failed: %s', str(e), exc_info=True)
        raise AirflowException(f'Model training failed: {str(e)}')


with DAG(
    dag_id='ML_training_DAG',
    default_args=default_args,
    description='Train fraud detection model on schedule',
    schedule_interval= None,
    catchup=False,
    tags=['fraud', 'ML']
) as dag:

    validate_environment = BashOperator(
    task_id="environment_validation",
    bash_command="""
        echo "Validating environment"
        ls -l /app
        test -f /app/config.yaml || echo "Missing config.yaml"
        test -f /app/.env || echo "Missing .env"
        test -f /app/config.yaml && test -f /app/.env && echo "Environment is valid!"
    """,
    dag=dag,)


    training_task = PythonOperator(
        task_id='execute_training',
        python_callable=_train_model
    )

    cleanup_task = BashOperator(
        task_id='cleanup',
        bash_command='rm -f /app/temp/*.pk1',
        trigger_rule='all_done'
    )

    # Define task order
    validate_environment >> training_task >> cleanup_task
