WITH trainings AS (
    SELECT 
        ws.worksession_id,
        ws.module_id,
        ws.Trainer_Name,
        ws.start_timeinfo AS training_start_time,
        ws.end_timeinfo AS training_end_time,
        DATEDIFF(SECOND, ws.end_timeinfo, ws.start_timeinfo) / 1000 AS duration_seconds,
        'Training' AS worktype
    FROM 
        {{ ref("stg_worksessions") }} ws
    WHERE 
        ws.worktype = 'Training'
),
training_completion_status AS (
    SELECT
        t.worksession_id,
        t.module_id,
        t.Trainer_Name,
        t.training_start_time,
        t.training_end_time,
        ABS(t.duration_seconds) as duration_seconds,
        ROUND(ABS(t.duration_seconds) / 60, 3) AS duration_minutes, -- Duration in minutes rounded to 3 decimal places
        CONCAT(CAST(duration_minutes AS STRING), ' mins') AS duration_minutes_display, -- Duration in minutes rounded to 3 decimal places
        m.usertype,
        m.training_name,
        CASE WHEN t.training_end_time < CURRENT_TIMESTAMP THEN 'Completed' ELSE 'Not Completed' END AS status
    FROM
        trainings t
    INNER JOIN
        {{ ref("stg_modules") }} m ON t.module_id = m.module_id
),
final AS (
    SELECT
        worksession_id,
        training_name,
        Trainer_Name,
        training_start_time,
        training_end_time,
        duration_minutes_display AS duration,
        usertype,
        status
    FROM
        training_completion_status
)

SELECT * FROM final
