# import os
# import json
# import shutil
# import pendulum
# import psycopg2
# import contextlib
# import pandas as pd
# from airflow import DAG
# from loguru import logger
# from datetime import timedelta, datetime
# from clickhouse_connect import get_client
# from clickhouse_connect.driver import Client
# from airflow.operators.python import PythonOperator, ShortCircuitOperator
#
# current_path = os.path.dirname(os.path.abspath(__file__))
# os.environ["CURRENT_PATH"] = current_path
#
# # Получаем переменные из окружения
# dir_name = os.getenv('DOCKER_PATH_B24_FACT_FILES')
#
# logger.info(f"Current files path is {dir_name}")
# logger.info(f"Current scripts path is {os.path.dirname(os.path.abspath(__file__))}")
#
# date_formats: list = [
#     "%Y-%m-%d %H:%M:%S",
#     "%Y-%m-%d %H:%M:%S.%f"
# ]
#
#
# # Чтение данных из файлов
# def parse_csv(file_path, json_output_path):
#     def convert_format_date(date):
#         """
#         Convert to a date type.
#         """
#         for date_format in date_formats:
#             with contextlib.suppress(ValueError):
#                 return datetime.strptime(date, date_format).strftime("%Y-%m-%d %H:%M:%S")
#         return date
#
#     # Словарь для замены названий столбцов с русского на английский
#     column_translation = {
#         'number': 'number',
#         'postanovshik': 'initiator',
#         'otvetstvennij': 'responsible',
#         'soispolniteli': 'co_executors',
#         'date_sozdaniya': 'creation_date',
#         'date_zakritiya': 'closing_date',
#         'podrazdelenie_postanovshika': 'initiators_department',
#         'sostoyanie': 'status',
#         'gruppa': 'group_',
#         'tip_obrasheniya': 'request_type',
#         'proekt_avtomatizacii': 'automation_project',
#         'podrazdelenie_otvetstvennogo': 'responsibles_department',
#         'teg': 'tag',
#         'priznak_prosrochki': 'overdue_indicator',
#         'Недель назад закрыто': 'weeks_closed_ago'
#     }
#
#     # Читаем Excel файл
#     df = pd.read_csv(file_path)
#
#     # Переименовываем столбцы на английские
#     df.rename(columns=column_translation, inplace=True)
#
#     df['creation_date'] = df['creation_date'].apply(lambda x: convert_format_date(x))
#     df['closing_date'] = df['closing_date'].apply(lambda x: convert_format_date(x))
#
#     # Сохраняем в JSON
#     df.to_json(
#         f"{json_output_path}/{os.path.basename(file_path).replace('.csv', '.json')}",
#         orient='records',
#         force_ascii=False,
#         indent=4
#     )
#
#
# # Аргументы по умолчанию для DAG
# default_args = {
#     "owner": "airflow",
#     "start_date": pendulum.today('UTC'),
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }
#
# # Определяем DAG для project1
# dag = DAG(
#     "b24_fact_task_dag",
#     default_args=default_args,
#     description="DAG для выполнения скриптов по export_nw",
#     schedule_interval="@daily",
#     catchup=False
# )
#
#
# # Функция для обработки Excel файлов и конвертации в JSON
# def process_csv_task():
#     done_path = os.path.join(dir_name, "done")
#     json_output_path = os.path.join(dir_name, "json")
#
#     # Проверяем и создаем необходимые папки
#     os.makedirs(done_path, exist_ok=True)
#     os.makedirs(json_output_path, exist_ok=True)
#
#     logger.info(f"Checking Excel files in directory: {dir_name}")
#
#     # Получаем все файлы из директории xls_path
#     csv_files = [
#         f for f in os.listdir(dir_name)
#         if f.endswith('.csv') and not f.startswith('error')
#     ]
#
#     if not csv_files:
#         logger.info("No Excel files found for processing")
#         return
#
#     # Итерируем по каждому файлу
#     for csv_file in csv_files:
#         logger.info(f"File {csv_file} found for processing")
#         file_path = os.path.join(dir_name, csv_file)
#         json_output_path = os.path.join(dir_name, "json")
#
#         logger.info(f"Starting process for {file_path}")
#         try:
#             parse_csv(file_path, json_output_path)
#             logger.info(f"Successfully processed {csv_file}")
#             # Перемещаем файл в папку done после успешной обработки
#             shutil.move(file_path, os.path.join(done_path, csv_file))
#             logger.info(f"Moved file {csv_file} to done folder")
#         except Exception as ex:
#             new_file_name = f"error_{csv_file}"
#             shutil.move(file_path, os.path.join(dir_name, new_file_name))
#             logger.error(f"Error processing file {csv_file}: {ex}")
#             raise Exception(f"Не удалось обработать файл {csv_file}: {ex}")
#
#
# # Функция для проверки наличия данных
# def check_data():
#     json_files = [
#         f for f in os.listdir(os.path.join(dir_name, "json"))
#         if f.endswith('.json') and not f.startswith('error')
#     ]
#     if json_files:
#         logger.info(f"Found {len(json_files)} files for processing.")
#         return True  # Продолжаем выполнение DAG
#     else:
#         logger.info("No data found. Skipping the next steps.")
#         return False  # Прекращаем выполнение последующих задач
#
#
# # Функция для вставки JSON данных в PostgreSQL
# def send_to_postgres():
#     logger.info("Starting to send data to PostgreSQL")
#
#     json_files = [
#         f for f in os.listdir(os.path.join(dir_name, "json"))
#         if f.endswith('.json') and not f.startswith('error')
#     ]
#
#     logger.info(f"Found JSON files: {json_files}")
#
#     conn = None
#     cursor = None
#     try:
#         conn = psycopg2.connect(
#             host="postgres",
#             database="airflow",
#             user="airflow",
#             password="airflow"
#         )
#         cursor = conn.cursor()
#         logger.info("Connected to PostgreSQL")
#
#         for json_file in json_files:
#             file_path = os.path.join(dir_name, "json", json_file)
#             logger.info(f"Processing JSON file: {file_path}")
#
#             with open(file_path, 'r') as f:
#                 data_list = json.load(f)
#                 logger.info(f"Loaded JSON data from {json_file}")
#
#                 for data in data_list:
#                     # Extract columns from data keys
#                     columns = [
#                         'number',
#                         'initiator',
#                         'responsible',
#                         'co_executors',
#                         'creation_date',
#                         'closing_date',
#                         'initiators_department',
#                         'status',
#                         'group_',
#                         'request_type',
#                         'automation_project',
#                         'responsibles_department',
#                         'tag',
#                         'overdue_indicator',
#                         'weeks_closed_ago'
#                     ]
#
#                     # Create dynamic list of values, using None for missing keys
#                     values = [data.get(col) for col in columns]
#
#                     # Create dynamic query
#                     query = (
#                         f"INSERT INTO b24_fact ({', '.join(columns)}) "
#                         f"VALUES ({', '.join(['%s'] * len(values))})"
#                     )
#
#                     cursor.execute(query, values)
#                     conn.commit()
#                 logger.info(f"Data from {json_file} successfully inserted into PostgreSQL")
#
#     except Exception as ex:
#         logger.error(f"Error while sending data to PostgreSQL: {ex}")
#         raise Exception("Не удалось загрузить данные в PostgreSQL")
#     finally:
#         if conn:
#             cursor.close()
#             conn.close()
#             logger.info("Closed PostgreSQL connection")
#
#
# # Функция для вставки JSON данных в ClickHouse
# def send_to_clickhouse():
#     logger.info("Starting to send data to ClickHouse")
#
#     json_files = [
#         f for f in os.listdir(os.path.join(dir_name, "json"))
#         if f.endswith('.json') and not f.startswith('error')
#     ]
#
#     logger.info(f"Found JSON files: {json_files}")
#
#     try:
#         client: Client = get_client(host="clickhouse", database="default", username="default", password="6QVnYsC4iSzz")
#         logger.info("Connected to ClickHouse")
#
#         for json_file in json_files:
#             file_path = os.path.join(dir_name, "json", json_file)
#             logger.info(f"Processing JSON file: {file_path}")
#
#             with open(file_path, 'r') as f:
#                 data_list = json.load(f)
#                 logger.info(f"Loaded JSON data from {json_file}")
#
#                 rows = []  # Список списков для всех строк текущего JSON файла
#
#                 for record in data_list:
#                     # Каждая строка должна быть списком значений колонок
#                     row = [
#                         record.get('number'),
#                         record.get('initiator'),
#                         record.get('responsible'),
#                         record.get('co_executors'),
#                         datetime.strptime(record.get('creation_date'), "%Y-%m-%d %H:%M:%S"),
#                         datetime.strptime(record.get('closing_date'), "%Y-%m-%d %H:%M:%S"),
#                         record.get('initiators_department'),
#                         record.get('status'),
#                         record.get('group_'),
#                         record.get('request_type'),
#                         record.get('automation_project'),
#                         record.get('responsibles_department'),
#                         record.get('tag'),
#                         record.get('overdue_indicator'),
#                         record.get('weeks_closed_ago')
#                     ]
#
#                     rows.append(row)  # Добавляем строку в список строк
#
#                 # Вставляем все строки в таблицу ClickHouse
#                 client.insert(
#                     table="b24_fact",
#                     data=rows,
#                     column_names=[
#                         'number', 'initiator', 'responsible', 'co_executors', 'creation_date', 'closing_date',
#                         'initiators_department', 'status', 'group_', 'request_type', 'automation_project',
#                         'responsibles_department', 'tag', 'overdue_indicator', 'weeks_closed_ago'
#                     ]
#                 )
#                 logger.info(f"Data from {json_file} successfully inserted into ClickHouse")
#     except Exception as ex:
#         logger.error(f"Error while sending data to Clickhouse: {ex}")
#         raise Exception(f"Не удалось загрузить данные в Clickhouse. Данные {rows}")
#
#
# # Функция для удаления JSON файлов
# def cleanup_json_files():
#     json_files = [
#         f for f in os.listdir(os.path.join(dir_name, "json"))
#         if f.endswith('.json') and not f.startswith('error')
#     ]
#
#     if not json_files:
#         logger.info("No JSON files to delete")
#         return
#
#     logger.info(f"Deleting JSON files: {json_files}")
#
#     for json_file in json_files:
#         file_path = os.path.join(dir_name, "json", json_file)
#         try:
#             os.remove(file_path)
#             logger.info(f"Deleted JSON file: {file_path}")
#         except Exception as ex:
#             logger.error(f"Error deleting JSON file {file_path}: {ex}")
#
#
# # Оператор для обработки файлов
# python_task = PythonOperator(
#     task_id="b24_fact_task",
#     python_callable=process_csv_task,
#     dag=dag
# )
#
# # Оператор для проверки наличия данных
# check_data_task = ShortCircuitOperator(
#     task_id='check_data',
#     python_callable=check_data,
#     provide_context=True,
#     dag=dag
# )
#
# # Оператор для отправки данных в PostgreSQL
# postgres_task = PythonOperator(
#     task_id="send_to_postgres",
#     python_callable=send_to_postgres,
#     dag=dag
# )
#
# # Оператор для отправки данных в ClickHouse
# clickhouse_task = PythonOperator(
#     task_id="send_to_clickhouse",
#     python_callable=send_to_clickhouse,
#     dag=dag
# )
#
# # Оператор для удаления JSON файлов
# cleanup_task = PythonOperator(
#     task_id="cleanup_json_files",
#     python_callable=cleanup_json_files,
#     dag=dag
# )
#
# # Определяем последовательность выполнения задач
# python_task >> check_data_task >> [postgres_task, clickhouse_task]
