CREATE TABLE IF NOT EXISTS b24.initiators
(
    initiator_id UUID DEFAULT generateUUIDv4(), -- генерировать UUID при вставке
    initiator String,
    initiators_department Nullable(String)
) ENGINE = MergeTree()
ORDER BY initiator;
