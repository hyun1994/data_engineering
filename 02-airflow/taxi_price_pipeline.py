from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime

default_args = {
    "start_date": datetime(2021, 1, 1)
}

with DAG(dag_id = "taxi_price_pipeline",
        schedule_interval="@daily",
        tags=["spark"],
        catchup=False) as dag:

    preprocess = SparkSubmitOperator(
        application="/Users/ji/data-engineering/02-airflow/preprocess.py",
        task_id = "preprocess",
        conn_id = "spark_default"
    )

    tune_hyperparameter = SparkSubmitOperator(
        application="/Users/ji/data-engineering/02-airflow/tune_hyperparameter.py",
        task_id = "tune_hyperparameter",
        conn_id = "spark_default"
    )

    train_model = SparkSubmitOperator(
        application="/Users/ji/data-engineering/02-airflow/train_model.py",
        task_id = "train_model",
        conn_id = "spark_default"
    )