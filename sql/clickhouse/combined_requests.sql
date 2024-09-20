CREATE TABLE IF NOT EXISTS b24.combined_requests
(
    request_number Int32,
    request_type Nullable(String),
    automation_project Nullable(String),
    creation_date DateTime32,
    closing_date Nullable(DateTime32),
    status_name Nullable(String),
    group_name Nullable(String),
    tag_name Nullable(String),
    initiator String,
    initiators_department Nullable(String),
    responsible String,
    responsibles_department Nullable(String),
    co_executors Nullable(String)
) ENGINE = PostgreSQL('postgres:5432', 'airflow', 'combined_requests', 'airflow', 'airflow', 'b24');