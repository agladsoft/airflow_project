CREATE TABLE IF NOT EXISTS b24.request_co_executors
(
    request_number BIGINT REFERENCES requests(request_number),
    co_executor_id BIGINT REFERENCES co_executors(co_executor_id),
    PRIMARY KEY (request_number, co_executor_id)
);
