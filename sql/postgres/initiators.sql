CREATE TABLE IF NOT EXISTS b24.initiators
(
    initiator_id BIGINT PRIMARY KEY DEFAULT floor(random() * 1000000000),
    initiator TEXT NOT NULL,
    initiators_department TEXT NULL
);
