CREATE TABLE IF NOT EXISTS b24.requests
(
    request_number BIGINT PRIMARY KEY NOT NULL, -- уникальный номер запроса
    request_type TEXT NULL,
    automation_project TEXT NULL,
    creation_date TIMESTAMP NULL,
    closing_date TIMESTAMP NULL,
    status TEXT NULL,
    group_ TEXT NULL,
    tag TEXT NULL,
    initiator_id BIGINT REFERENCES initiators(initiator_id), -- внешний ключ на инициаторов
    responsible_id BIGINT REFERENCES responsibles(responsible_id) -- внешний ключ на ответственных
);
