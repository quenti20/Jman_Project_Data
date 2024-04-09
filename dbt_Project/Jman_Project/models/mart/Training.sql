WITH trainings AS (
    SELECT 
        ws.worksession_id,
        ws.module_id,
        ws.Trainer_Name,
        ws.start_timeinfo AS training_start_time,
        ws.end_timeinfo AS training_end_time,
        DATEDIFF(SECOND,ws.end_timeinfo, ws.start_timeinfo) AS duration_seconds,
        'Training' AS worktype
    FROM 
        {{ ref("stg_worksessions") }} ws
    WHERE 
        ws.worktype = 'Training'
),
module_dates AS (
    SELECT
        module_id,
        moduledateinfo AS module_date
    FROM
        {{ ref("stg_modules") }}
),
training_completion_status AS (
    SELECT
        t.worksession_id,
        t.module_id,
        t.Trainer_Name,
        t.training_start_time,
        t.training_end_time,
        t.duration_seconds,
        t.worktype,
        md.module_date,
        CASE WHEN t.training_end_time < CURRENT_TIMESTAMP THEN 'Completed' ELSE 'Not Completed' END AS status
    FROM
        trainings t
    JOIN
        module_dates md ON t.module_id = md.module_id
),
final AS (
    SELECT
        worksession_id,
        module_id,
        Trainer_Name,
        training_start_time,
        training_end_time,
        duration_seconds,
        module_date,
        worktype,
        status
    FROM
        training_completion_status
)

SELECT * FROM final
