
CREATE TABLE IF NOT EXISTS de_log.tasks_logger (
    task_uuid TEXT PRIMARY KEY,
    task_name TEXT,
    task_id TEXT,
    state TEXT,
    runtime FLOAT,
    logical_date TIMESTAMP
)