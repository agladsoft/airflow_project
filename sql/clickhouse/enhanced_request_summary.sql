CREATE OR REPLACE VIEW b24.enhanced_request_summary
AS SELECT
    request_number,
    request_type,
    automation_project,
    creation_date,
    closing_date,
    status,
    group_ AS group,
    tag,
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
    IF(status IN ('Завершена', 'Условно завершена'), TRUE, FALSE) AS is_completed,
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
    toBool(
        IF(
            closing_date IS NULL,
            dateDiff('day', creation_date, now()) > 1,
            dateDiff('day', creation_date, closing_date) > 1
        )
    ) AS overdue_indicator,
    toHour(creation_date) AS creation_hour,
    toHour(closing_date) AS closing_hour,
    toDate(creation_date) AS creation_day,
    toDate(closing_date) AS closing_day,
    dateDiff('week', creation_date, now()) AS weeks_since_creation,
    dateDiff('week', closing_date, now()) AS weeks_since_closing,
    toDayOfWeek(creation_date) AS creation_weekday,
    dateDiff('week', toStartOfYear(creation_date), creation_date) + 1 AS creation_week,
    dateDiff('week', toStartOfYear(closing_date), closing_date) + 1 AS closing_week,
    CASE
        WHEN toDayOfWeek(creation_date) = 1 THEN 'Понедельник'
        WHEN toDayOfWeek(creation_date) = 2 THEN 'Вторник'
        WHEN toDayOfWeek(creation_date) = 3 THEN 'Среда'
        WHEN toDayOfWeek(creation_date) = 4 THEN 'Четверг'
        WHEN toDayOfWeek(creation_date) = 5 THEN 'Пятница'
        WHEN toDayOfWeek(creation_date) = 6 THEN 'Суббота'
        WHEN toDayOfWeek(creation_date) = 7 THEN 'Воскресенье'
    END AS day_name,
    CONCAT(
        formatDateTime(creation_date - INTERVAL (toDayOfWeek(creation_date) - 1) DAY, '%d.%m'), '-',
        formatDateTime(creation_date + INTERVAL (7 - toDayOfWeek(creation_date)) DAY, '%d.%m')
    ) AS week_period,
    IF(toWeek(now()) = toWeek(closing_date), FALSE, TRUE) AS is_current_week
FROM
    b24.combined_requests
WHERE
    group_ IN ('ПОЛИТИКА ИНФОБЕЗА', '#HelpDesk')
    AND responsible NOT LIKE '%esk%'
    AND responsible NOT LIKE '%Филип%'
    AND responsible NOT LIKE '%Прист%'
    AND responsibles_department NOT LIKE '%генераль%'
    AND dateDiff('week', creation_date, now()) BETWEEN 1 AND 8
    AND toYear(creation_date) = toYear(now());