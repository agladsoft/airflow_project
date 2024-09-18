CREATE TABLE IF NOT EXISTS b24.co_executors
(
    co_executor_id BIGINT PRIMARY KEY DEFAULT floor(random() * 1000000000),
    co_executor TEXT NOT NULL
);
