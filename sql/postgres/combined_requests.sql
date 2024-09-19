CREATE OR REPLACE VIEW b24.combined_requests AS
SELECT
    req.request_number AS request_number,
    req.request_type AS request_type,
    req.automation_project AS automation_project,
    req.creation_date AS creation_date,
    req.closing_date AS closing_date,
    s.status_name AS status_name, -- Присоединение статуса
    g.group_name AS group_name, -- Присоединение группы
    t.tag_name AS tag_name, -- Присоединение тега
    p_init.people_name AS initiator, -- Имя инициатора
    d_init.department_name AS initiators_department, -- Подразделение инициатора
    p_resp.people_name AS responsible, -- Имя ответственного
    d_resp.department_name AS responsibles_department, -- Подразделение ответственного
    co_exec.co_executors_names AS co_executors -- Исполнители (через запятую)
FROM
    b24.requests req
LEFT JOIN b24.initiators init ON req.initiator_id = init.initiator_id
LEFT JOIN b24.people p_init ON init.people_id = p_init.people_id -- Присоединение таблицы people для инициаторов
LEFT JOIN b24.department d_init ON init.department_id = d_init.department_id -- Присоединение таблицы department для инициаторов
LEFT JOIN b24.responsibles resp ON req.responsible_id = resp.responsible_id
LEFT JOIN b24.people p_resp ON resp.people_id = p_resp.people_id -- Присоединение таблицы people для ответственных
LEFT JOIN b24.department d_resp ON resp.department_id = d_resp.department_id -- Присоединение таблицы department для ответственных
LEFT JOIN (
    SELECT
        rce.request_number,
        STRING_AGG(p.people_name, ', ') AS co_executors_names -- Агрегация соисполнителей в строку
    FROM
        b24.request_co_executors rce
    LEFT JOIN b24.people p ON rce.people_id = p.people_id -- Присоединение соисполнителей к таблице people
    GROUP BY rce.request_number
) co_exec ON req.request_number = co_exec.request_number -- Присоединение соисполнителей
LEFT JOIN b24.status s ON req.status_id = s.status_id -- Присоединение таблицы status
LEFT JOIN b24."group" g ON req.group_id = g.group_id -- Присоединение таблицы group
LEFT JOIN b24.tag t ON req.tag_id = t.tag_id; -- Присоединение таблицы tag