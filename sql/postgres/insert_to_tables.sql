INSERT INTO b24.initiators (initiator, initiators_department)
SELECT DISTINCT initiator, initiators_department
FROM b24_fact
WHERE initiator IS NOT NULL;



INSERT INTO b24.responsibles (responsible, responsibles_department)
SELECT DISTINCT responsible, responsibles_department
FROM b24_fact
WHERE responsible IS NOT NULL;



INSERT INTO b24.co_executors (co_executor)
SELECT DISTINCT UNNEST(STRING_TO_ARRAY(co_executors, ', '))
FROM b24_fact
WHERE co_executors IS NOT NULL;



INSERT INTO b24.requests (
    request_number, request_type, automation_project, creation_date, closing_date,
    status, group_, tag, initiator_id, responsible_id
)
SELECT
    b.number AS request_number,
    b.request_type,
    b.automation_project,
    b.creation_date,
    b.closing_date,
    b.status,
    b.group_,
    b.tag,
    i.initiator_id,
    r.responsible_id
FROM b24_fact b
JOIN b24.initiators i ON b.initiator = i.initiator
JOIN b24.responsibles r ON b.responsible = r.responsible
ON CONFLICT (request_number) DO NOTHING;



-- Создание временной таблицы с развернутыми ко-исполнителями
CREATE TEMP TABLE temp_co_executors AS
SELECT
    b.number AS request_number,
    co_exec_table.co_executor_id AS co_executor_id,
    TRIM(co_exec) AS co_executor
FROM b24_fact b
JOIN UNNEST(string_to_array(b.co_executors, ', ')) AS co_exec ON true
JOIN b24.co_executors co_exec_table ON TRIM(co_exec) = co_exec_table.co_executor;

DROP TABLE temp_co_executors



-- Вставка данных в request_co_executors из временной таблицы
INSERT INTO b24.request_co_executors (request_number, co_executor_id)
SELECT
    req.request_number,
    temp.co_executor_id
FROM b24.requests req
JOIN temp_co_executors temp ON req.request_number = temp.request_number;
