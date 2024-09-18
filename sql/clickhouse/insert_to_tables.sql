INSERT INTO b24.initiators (initiator, initiators_department)
SELECT DISTINCT initiator, initiators_department
FROM b24_fact
WHERE initiator IS NOT NULL;



INSERT INTO b24.responsibles (responsible, responsibles_department)
SELECT DISTINCT responsible, responsibles_department
FROM b24_fact
WHERE responsible IS NOT NULL;



INSERT INTO b24.co_executors (co_executor)
SELECT DISTINCT arrayJoin(splitByString(', ', assumeNotNull(co_executors)))
FROM b24_fact
WHERE co_executors IS NOT NULL;



INSERT INTO b24.requests (
    request_number, request_type, automation_project, creation_date, closing_date,
    status, group_, tag, initiator_id, responsible_id
)
SELECT
    b.number AS request_number,
    b.request_type AS request_type,
    b.automation_project AS automation_project,
    b.creation_date AS creation_date,
    b.closing_date AS closing_date,
    b.status AS status,
    b.group_ AS group_,
    b.tag AS tag,
    i.initiator_id AS initiator_id,
    r.responsible_id AS responsible_id
FROM b24_fact b
JOIN b24.initiators i ON b.initiator = i.initiator
JOIN b24.responsibles r ON b.responsible = r.responsible



INSERT INTO b24.request_co_executors (request_number, co_executor_id)
SELECT
    r.request_number AS request_number,
    co.co_executor_id AS co_executor_id
FROM
    (SELECT number, arrayJoin(splitByString(', ', assumeNotNull(co_executors))) AS co_executor
     FROM b24_fact
     WHERE co_executors IS NOT NULL) AS b
JOIN b24.requests r ON b.number = r.request_number
JOIN b24.co_executors co ON b.co_executor = co.co_executor;
