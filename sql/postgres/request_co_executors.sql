CREATE TABLE IF NOT EXISTS b24.request_co_executors
(
    request_number BIGINT REFERENCES b24.requests(request_number),
    people_id BIGINT REFERENCES b24.people(people_id),
    PRIMARY KEY (request_number, people_id)
);