import os
import json
import shutil
import psycopg2
import numpy as np
import pandas as pd
import psycopg2.extras
from datetime import datetime
from contextlib import suppress
from clickhouse_connect import get_client

# General settings
current_path = os.path.dirname(os.path.abspath(__file__))
os.environ["CURRENT_PATH"] = current_path
dir_name = os.getenv('DOCKER_PATH_B24_FACT_FILES')

# Date formats
date_formats = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]

# Column translation mapping
column_translation = {
    'number': 'number',
    'postanovshik': 'initiator',
    'otvetstvennij': 'responsible',
    'soispolniteli': 'co_executors',
    'date_sozdaniya': 'creation_date',
    'date_zakritiya': 'closing_date',
    'podrazdelenie_postanovshika': 'initiators_department',
    'sostoyanie': 'status',
    'gruppa': 'group_',
    'tip_obrasheniya': 'request_type',
    'proekt_avtomatizacii': 'automation_project',
    'podrazdelenie_otvetstvennogo': 'responsibles_department',
    'teg': 'tag',
    'priznak_prosrochki': 'overdue_indicator',
    'Недель назад закрыто': 'weeks_closed_ago'
}


def convert_format_date(date):
    for date_format in date_formats:
        with suppress(Exception):
            return datetime.strptime(date, date_format).strftime("%Y-%m-%d %H:%M:%S")
    return date


def change_type_and_values(df) -> None:
    """
    Change data types or changing values.
    """
    df.replace({np.nan: None, "NaT": None})
    df['number'] = df['number'].astype('Int64')
    df['creation_date'] = df['creation_date'].apply(lambda x: convert_format_date(x))
    df['closing_date'] = df['closing_date'].apply(lambda x: convert_format_date(x))
    df['overdue_indicator'] = df['overdue_indicator'].astype(bool)
    df['weeks_closed_ago'] = df['weeks_closed_ago'].astype('Int64')


def parse_csv(file_path, json_output_path):
    df = pd.read_csv(file_path)
    df.rename(columns=column_translation, inplace=True)
    change_type_and_values(df)
    df.to_json(
        f"{json_output_path}/{os.path.basename(file_path).replace('.csv', '.json')}",
        orient='records', force_ascii=False, indent=4
    )


def move_file(src, dest, error=False):
    new_name = f"error_{os.path.basename(src)}" if error else os.path.basename(src)
    shutil.move(src, os.path.join(dest, new_name))


def insert_to_postgres(json_files):
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()

    for json_file in json_files:
        with open(json_file, 'r') as f:
            data_list = json.load(f)

            rows = [
                [
                    record.get('number'),
                    record.get('initiator'),
                    record.get('responsible'),
                    record.get('co_executors'),
                    datetime.strptime(
                        record.get('creation_date'), "%Y-%m-%d %H:%M:%S"
                    ) if record.get('creation_date') else None,
                    datetime.strptime(
                        record.get('closing_date'), "%Y-%m-%d %H:%M:%S"
                    ) if record.get('closing_date') else None,
                    record.get('initiators_department'),
                    record.get('status'),
                    record.get('group_'),
                    record.get('request_type'),
                    record.get('automation_project'),
                    record.get('responsibles_department'),
                    record.get('tag'),
                    record.get('overdue_indicator'),
                    record.get('weeks_closed_ago')
                ]
                for record in data_list
            ]

            insert_query = """
            INSERT INTO b24_fact (
                number, initiator, responsible, co_executors, creation_date, closing_date,
                initiators_department, status, group_, request_type, automation_project,
                responsibles_department, tag, overdue_indicator, weeks_closed_ago
            ) VALUES %s
            """
            psycopg2.extras.execute_values(cursor, insert_query, rows)
            conn.commit()

    cursor.close()
    conn.close()


def insert_to_clickhouse(json_files):
    client = get_client(host="clickhouse", database="default", username="default", password="6QVnYsC4iSzz")

    for json_file in json_files:
        with open(json_file, 'r') as f:
            data_list = json.load(f)

            rows = [
                [
                    record.get('number'),
                    record.get('initiator'),
                    record.get('responsible'),
                    record.get('co_executors'),
                    datetime.strptime(
                        record.get('creation_date'), "%Y-%m-%d %H:%M:%S"
                    ) if record.get('creation_date') else None,
                    datetime.strptime(
                        record.get('closing_date'), "%Y-%m-%d %H:%M:%S"
                    ) if record.get('closing_date') else None,
                    record.get('initiators_department'),
                    record.get('status'),
                    record.get('group_'),
                    record.get('request_type'),
                    record.get('automation_project'),
                    record.get('responsibles_department'),
                    record.get('tag'),
                    record.get('overdue_indicator'),
                    record.get('weeks_closed_ago')
                ]
                for record in data_list
            ]

            client.insert(
                table="b24_fact",
                data=rows,
                column_names=[
                    'number', 'initiator', 'responsible', 'co_executors', 'creation_date', 'closing_date',
                    'initiators_department', 'status', 'group_', 'request_type', 'automation_project',
                    'responsibles_department', 'tag', 'overdue_indicator', 'weeks_closed_ago'
                ]
            )
