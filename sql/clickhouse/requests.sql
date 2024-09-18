CREATE TABLE IF NOT EXISTS b24.requests
(
    request_number Int32,
    request_type Nullable(String),
    automation_project Nullable(String),
    creation_date Nullable(DateTime),
    closing_date Nullable(DateTime),
    status Nullable(String),
    group_ Nullable(String),
    tag Nullable(String),
    initiator_id UUID,
    responsible_id UUID
) ENGINE = MergeTree()
ORDER BY request_number;
