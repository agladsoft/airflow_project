CREATE TABLE IF NOT EXISTS b24.initiators
(
    initiator_id BIGSERIAL PRIMARY KEY,
    creation_date TIMESTAMP NULL,
    closing_date TIMESTAMP NULL,
    people_id BIGINT REFERENCES b24.people(people_id), -- внешний ключ на людей
    department_id BIGINT REFERENCES b24.department(department_id) -- внешний ключ на подразделения
);