CREATE TABLE IF NOT EXISTS b24.responsibles
(
    responsible_id BIGINT PRIMARY KEY DEFAULT floor(random() * 1000000000),
    responsible TEXT NOT NULL,
    responsibles_department TEXT NULL
);
