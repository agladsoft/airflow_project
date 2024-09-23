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
    rename_department(initiators_department) AS initiators_department,
    responsible,
    rename_department(responsibles_department) AS responsibles_department,
    co_executors,
    IF(status_name IN ('Завершена', 'Условно завершена'), TRUE, FALSE) AS is_completed,
    toBool(
        IF(
            closing_date IS NULL,
            dateDiff('day', creation_date, now()) > 1,
            dateDiff('day', creation_date, closing_date) > 1
        )
    ) AS is_overdue,
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
    group_name IN ('ПОЛИТИКА ИНФОБЕЗА', '#HelpDesk')
    AND responsible NOT LIKE '%esk%'
    AND responsible NOT LIKE '%Филип%'
    AND responsible NOT LIKE '%Прист%'
    AND responsibles_department NOT LIKE '%генераль%'
    AND dateDiff('week', creation_date, now()) BETWEEN 1 AND 8
    AND toYear(creation_date) = toYear(now());