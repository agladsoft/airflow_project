CREATE TABLE IF NOT EXISTS b24.co_executors
(
    co_executor_id UUID DEFAULT generateUUIDv4(), -- генерировать UUID при вставке
    co_executor String
) ENGINE = MergeTree()
ORDER BY co_executor;
