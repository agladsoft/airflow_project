CREATE TABLE IF NOT EXISTS b24.request_co_executors
(
    request_number Int32,
    co_executor_id UUID
) ENGINE = MergeTree()
ORDER BY (request_number, co_executor_id);
