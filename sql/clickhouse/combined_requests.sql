CREATE OR REPLACE VIEW b24.combined_requests AS
SELECT
    req.request_number AS request_number,
    req.request_type AS request_type,
    req.automation_project AS automation_project,
    req.creation_date AS creation_date,
    req.closing_date AS closing_date,
    req.status AS status,
    req.group_ AS group_,
    req.tag AS tag,
    init.initiator AS initiator,
    init.initiators_department AS initiators_department,
    resp.responsible AS responsible,
    resp.responsibles_department AS responsibles_department,
    CASE
        WHEN co_exec.co_executors_names = '' THEN NULL
        ELSE co_exec.co_executors_names
    END AS co_executors
FROM
    b24.requests req
JOIN b24.initiators init ON req.initiator_id = init.initiator_id
JOIN b24.responsibles resp ON req.responsible_id = resp.responsible_id
LEFT JOIN (
    SELECT
        request_number,
        arrayStringConcat(groupArray(co_executor), ', ') AS co_executors_names
    FROM
        b24.request_co_executors rce
    JOIN b24.co_executors co_exec ON rce.co_executor_id = co_exec.co_executor_id
    GROUP BY request_number
) co_exec ON req.request_number = co_exec.request_number
GROUP BY
    request_number, request_type, automation_project, creation_date,
    closing_date, status, group_, tag, initiator, initiators_department, responsible, responsibles_department, co_executors;