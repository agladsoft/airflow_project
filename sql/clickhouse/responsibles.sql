CREATE TABLE IF NOT EXISTS b24.responsibles
(
    responsible_id UUID DEFAULT generateUUIDv4(), -- генерировать UUID при вставке
    responsible String,
    responsibles_department Nullable(String)
) ENGINE = MergeTree()
ORDER BY responsible;