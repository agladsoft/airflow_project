from airflow import DAG
from loguru import logger
from pendulum import today
from datetime import timedelta
from dags.b24_fact_task.__init__ import *
from airflow.operators.python import PythonOperator, ShortCircuitOperator

# Default args
default_args = {
    "owner": "airflow",
    "start_date": today('UTC'),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="b24_fact_task_dag",
    default_args=default_args,
    description="DAG for processing B24 Fact Task",
    schedule_interval="@daily",
    catchup=False,
) as dag:

    def process_csv_task():
        json_output_path = os.path.join(dir_name, "json")
        done_path = os.path.join(dir_name, "done")
        os.makedirs(done_path, exist_ok=True)
        os.makedirs(json_output_path, exist_ok=True)

        logger.info(f"Checking files in directory: {dir_name}")
        csv_files = [f for f in os.listdir(dir_name) if f.endswith('.csv') and not f.startswith('error')]

        for csv_file in csv_files:
            logger.info(f"File {csv_file} found for processing")
            file_path = os.path.join(dir_name, csv_file)
            try:
                parse_csv(file_path, json_output_path)
                logger.info(f"Successfully processed {csv_file}")
                move_file(file_path, done_path)
                logger.info(f"Moved file {csv_file} to done folder")
            except Exception as ex:
                move_file(file_path, dir_name, error=True)
                logger.error(f"Error processing file {csv_file}: {ex}")
                raise Exception(f"Не удалось обработать файл {csv_file}: {ex}")

    def check_data():
        json_files = [f for f in os.listdir(os.path.join(dir_name, "json")) if f.endswith('.json')]
        return bool(json_files)

    def send_to_postgres():
        logger.info("Starting to send data to PostgreSQL")
        json_files = [
            os.path.join(dir_name, "json", f) for f in os.listdir(os.path.join(dir_name, "json")) if f.endswith('.json')
        ]
        insert_to_postgres(json_files)
        logger.info("Data from successfully inserted into PostgreSQL")

    def send_to_clickhouse():
        logger.info("Starting to send data to ClickHouse")
        json_files = [
            os.path.join(dir_name, "json", f) for f in os.listdir(os.path.join(dir_name, "json")) if f.endswith('.json')
        ]
        insert_to_clickhouse(json_files)
        logger.info("Data from successfully inserted into ClickHouse")

    def cleanup_json_files():
        json_files = [
            os.path.join(dir_name, "json", f) for f in os.listdir(os.path.join(dir_name, "json")) if f.endswith('.json')
        ]
        for json_file in json_files:
            file_path = os.path.join(dir_name, "json", json_file)
            os.remove(file_path)
        logger.info(f"Deleted JSON file: {json_files}")

    process_task = PythonOperator(task_id="process_csv_task", python_callable=process_csv_task)
    check_data_task = ShortCircuitOperator(task_id="check_data", python_callable=check_data)
    postgres_task = PythonOperator(task_id="send_to_postgres", python_callable=send_to_postgres)
    clickhouse_task = PythonOperator(task_id="send_to_clickhouse", python_callable=send_to_clickhouse)
    cleanup_task = PythonOperator(task_id="cleanup_json_files", python_callable=cleanup_json_files)

    process_task >> check_data_task >> [postgres_task, clickhouse_task] >> cleanup_task
