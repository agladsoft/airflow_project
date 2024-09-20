CREATE OR REPLACE VIEW b24.enhanced_request_summary
AS SELECT
    request_number,
    request_type,
    automation_project,
    creation_date,
    closing_date,
    status_name,
    group_name,
    tag_name,
    initiator,
    CASE
        WHEN initiators_department IN (
            'Управление эксплуатации',
            'Отдел запуска и сопровождения удаленных объектов',
            'Отдел поддержки пользователей'
        ) THEN 'Управление эксплуатации'
        WHEN initiators_department LIKE 'Отдел сети передачи данных%' THEN 'УССИ'
        WHEN initiators_department IN ('УССИ', 'Отдел системного администрирования', 'Отдел КСБ') THEN 'УССИ'
        ELSE initiators_department
    END AS initiators_department,
    responsible,
    CASE
        WHEN responsibles_department IN (
            'Управление эксплуатации',
            'Отдел запуска и сопровождения удаленных объектов',
            'Отдел поддержки пользователей'
        ) THEN 'Управление эксплуатации'
        WHEN responsibles_department LIKE 'Отдел сети передачи данных%' THEN 'УССИ'
        ELSE responsibles_department
    END AS responsibles_department,
    co_executors,
    CASE
        WHEN status_name IN ('Завершена', 'Условно завершена') THEN TRUE
        ELSE FALSE
    END AS is_completed,
    CASE
        WHEN responsibles_department IN (
            'Управление сетевой и серверной инфраструктурой',
            'Отдел системного администрирования',
            'Отдел КСБ',
            'Отдел СПД'
        ) THEN 1
        ELSE NULL
    END AS net_architecture_count,
    CASE
        WHEN responsibles_department IN (
            'Отдел управления данными',
            'Отдел разработки',
            'Проектный офис'
        ) THEN 1
        ELSE NULL
    END AS dcr_count,
    CASE
        WHEN closing_date IS NULL THEN
            (DATE_TRUNC('day', current_timestamp) - DATE_TRUNC('day', creation_date)) > interval '1 day'
        ELSE
            (DATE_TRUNC('day', closing_date) - DATE_TRUNC('day', creation_date)) > interval '1 day'
    END AS is_overdue,
    EXTRACT(HOUR FROM creation_date)::int AS creation_hour,
    EXTRACT(HOUR FROM closing_date)::int AS closing_hour,
    creation_date::date AS creation_day,
    closing_date::date AS closing_day,
    EXTRACT(WEEK FROM current_date)::int - EXTRACT(WEEK FROM creation_date)::int AS weeks_since_creation,
    EXTRACT(WEEK FROM current_date)::int - EXTRACT(WEEK FROM closing_date)::int AS weeks_since_closing,
    EXTRACT(DOW FROM creation_date)::int AS creation_weekday,
    EXTRACT(WEEK FROM creation_date)::int AS creation_week,
    EXTRACT(WEEK FROM closing_date)::int AS closing_week,
    CASE
        WHEN EXTRACT(ISODOW FROM creation_date)::int = 1 THEN 'Понедельник'
        WHEN EXTRACT(ISODOW FROM creation_date)::int = 2 THEN 'Вторник'
        WHEN EXTRACT(ISODOW FROM creation_date)::int = 3 THEN 'Среда'
        WHEN EXTRACT(ISODOW FROM creation_date)::int = 4 THEN 'Четверг'
        WHEN EXTRACT(ISODOW FROM creation_date)::int = 5 THEN 'Пятница'
        WHEN EXTRACT(ISODOW FROM creation_date)::int = 6 THEN 'Суббота'
        WHEN EXTRACT(ISODOW FROM creation_date)::int = 7 THEN 'Воскресенье'
    END AS day_name,
    CONCAT(TO_CHAR(creation_date - (EXTRACT(ISODOW FROM creation_date)::int - 1) * INTERVAL '1 day', 'DD.MM'), '-', TO_CHAR(creation_date + (7 - EXTRACT(ISODOW FROM creation_date)::int) * INTERVAL '1 day', 'DD.MM')) AS week_period,
    CASE
        WHEN EXTRACT(WEEK FROM current_date) = EXTRACT(WEEK FROM closing_date) THEN FALSE
        ELSE TRUE
    END AS is_current_week
FROM
    b24.combined_requests
WHERE
    group_name IN ('ПОЛИТИКА ИНФОБЕЗА', '#HelpDesk')
    AND responsible NOT LIKE '%esk%'
    AND responsible NOT LIKE '%Филип%'
    AND responsible NOT LIKE '%Прист%'
    AND responsibles_department NOT LIKE '%генераль%'
    AND EXTRACT(WEEK FROM current_date)::int - EXTRACT(WEEK FROM creation_date)::int BETWEEN 1 AND 8
    AND EXTRACT(YEAR FROM creation_date)::int = EXTRACT(YEAR FROM current_date)::int;