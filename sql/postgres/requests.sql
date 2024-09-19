CREATE TABLE IF NOT EXISTS b24.requests
(
    request_number BIGINT PRIMARY KEY NOT NULL, -- уникальный номер запроса
    request_type TEXT NULL,
    automation_project TEXT NULL,
    creation_date TIMESTAMP NULL,
    closing_date TIMESTAMP NULL,
    initiator_id BIGINT REFERENCES b24.initiators(initiator_id), -- внешний ключ на инициаторов
    responsible_id BIGINT REFERENCES b24.responsibles(responsible_id), -- внешний ключ на ответственных,
    status_id BIGINT REFERENCES b24.status(status_id), -- внешний ключ на статусы
    group_id BIGINT REFERENCES b24.group(group_id), -- внешний ключ на группу,
    tag_id BIGINT REFERENCES b24.tag(tag_id) -- внешний ключ на теги
);