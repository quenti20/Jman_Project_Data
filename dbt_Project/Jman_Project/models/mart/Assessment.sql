WITH worksessions AS (
    SELECT * FROM {{ ref("stg_worksessions") }}
),
performance AS (
    SELECT * FROM {{ ref("stg_performance") }}
),
users AS (
    SELECT * FROM {{ ref("stg_users") }}
),
workSession_training_time AS (
    SELECT
        worksession_id,
        worktype,
        TIMESTAMPADD(SECOND, DATEDIFF('SECOND', start_timeinfo, end_timeinfo), start_timeinfo) AS training_time,
        marks_obtained,
        email AS user_email
    FROM
        worksessions ws
    JOIN
        performance p ON ws.worksession_id = p.assessment_id
),
workSessions_status AS (
    SELECT
        worksession_id,
        worktype,
        CASE WHEN training_time < CURRENT_TIMESTAMP THEN 'Completed' ELSE 'Not Completed' END AS status,
        COUNT(*) AS total_count
    FROM
        workSession_training_time
    GROUP BY
        worksession_id, worktype, status
),
total_users_per_assignment AS (
    SELECT
        assessment_id AS worksession_id,
        COUNT(email) AS user_count, 
        SUM(marks_obtained) AS total_marks_per_assessment
    FROM
        performance
    GROUP BY
        assessment_id
),
avg_marks_obtained AS (
    SELECT
        ws.worksession_id,
        SUM(total_marks_per_assessment) / SUM(user_count) AS avg_marks_obtained
    FROM
        workSession_training_time ws
    JOIN
        total_users_per_assignment tua ON ws.worksession_id = tua.worksession_id
    -- WHERE
    --     ws.worktype = 'Assessment'
    GROUP BY
        ws.worksession_id
),
final AS (
    SELECT
        ws.worksession_id,
        ws.worktype,
        ws.status,
        ws.total_count,
        amo.avg_marks_obtained
    FROM
        workSessions_status ws
    LEFT JOIN
        avg_marks_obtained amo ON ws.worksession_id = amo.worksession_id
    
)

SELECT * FROM final
