from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

VENV = "source /home/ubuntu/data-lake-project/venv/bin/activate"
PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

with DAG(
    dag_id="data_lake_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    start_producer = BashOperator(
        task_id="run_producer",
        bash_command=f"{VENV} && python /home/ubuntu/data-lake-project/producers/orders_producer.py"
    )

    run_spark = BashOperator(
        task_id="run_spark_job",
        bash_command=(
            f"{VENV} && spark-submit "
            f"--packages {PACKAGES} "
            "/home/ubuntu/data-lake-project/streaming/spark_streaming.py"
        )
    )

    start_producer >> run_spark
