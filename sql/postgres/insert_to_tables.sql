INSERT INTO b24.initiators (creation_date, closing_date, people_id, department_id)
SELECT DISTINCT
  b24_fact.creation_date,
  b24_fact.closing_date,
  people.people_id,
  department.department_id
FROM public.b24_fact
LEFT JOIN b24.people ON b24_fact.initiator = people.people_name
LEFT JOIN b24.department ON b24_fact.initiators_department = department.department_name;



INSERT INTO b24.responsibles (creation_date, closing_date, people_id, department_id)
SELECT DISTINCT
  b24_fact.creation_date,
  b24_fact.closing_date,
  people.people_id,
  department.department_id
FROM public.b24_fact
LEFT JOIN b24.people ON b24_fact.responsible = people.people_name
LEFT JOIN b24.department ON b24_fact.responsibles_department = department.department_name;



INSERT INTO b24.requests (
    request_number, request_type, automation_project, creation_date, closing_date,
    status_id, group_id, tag_id, initiator_id, responsible_id
)
SELECT
    f.number AS request_number,
    f.request_type,
    f.automation_project,
    f.creation_date,
    f.closing_date,
    s.status_id,
    g.group_id,
    t.tag_id,
    i.initiator_id,
    r.responsible_id
FROM public.b24_fact f
LEFT JOIN b24.people p_initiator ON f.initiator = p_initiator.people_name
LEFT JOIN b24.initiators i ON
    p_initiator.people_id = i.people_id AND
    f.creation_date = i.creation_date AND
    (f.closing_date = i.closing_date OR (f.closing_date IS NULL AND i.closing_date IS NULL))
LEFT JOIN b24.people p_responsible ON f.responsible = p_responsible.people_name
LEFT JOIN b24.responsibles r ON
    p_responsible.people_id = r.people_id AND
    f.creation_date = r.creation_date AND
    (f.closing_date = r.closing_date OR (f.closing_date IS NULL AND r.closing_date IS NULL))
LEFT JOIN b24.status s ON f.status = s.status_name
LEFT JOIN b24."group" g ON f.group_ = g.group_name
LEFT JOIN b24.tag t ON f.tag = t.tag_name;



INSERT INTO b24.request_co_executors (request_number, people_id)
SELECT
  r.request_number,
  p.people_id
FROM public.b24_fact
JOIN LATERAL unnest(string_to_array(b24_fact.co_executors, ',')) AS executor_name ON TRUE -- Разделяем строку на части
JOIN b24.people p ON trim(executor_name) = p.people_name -- Сопоставляем с полем people_name
JOIN b24.requests r ON b24_fact.number = r.request_number;










select tag_name, count(*)
from tag
group by tag_name
having count(tag_name) > 1

DELETE FROM tag
    WHERE tag_id NOT IN
    (
        SELECT MAX(tag_id) AS MaxRecordID
        FROM tag
        GROUP BY tag_name
    );