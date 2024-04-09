WITH modules AS (
    SELECT 
        * 
    FROM {{ ref("stg_modules") }}
),
worksessions AS (
    SELECT 
        * 
    FROM {{ ref("stg_worksessions") }}
),
performance AS (
    SELECT
        * 
    FROM {{ ref("stg_performance") }}
),
users AS (
    SELECT
        *
    FROM {{ ref("stg_users") }}
),
modules_with_worksessions AS (
    SELECT
        m.module_id,
        COUNT(ws.workSession_id) AS worksession_count
    FROM
        modules m
    INNER JOIN
        worksessions ws ON m.module_id = ws.module_id
    GROUP BY
        m.module_id
),
module_completion_time AS (
    SELECT
        module_id,
        MIN(start_timeinfo) AS earliest_startime,
        MAX(end_timeinfo) AS latest_endtime
    FROM
        worksessions
    GROUP BY
        module_id
),
avg_assessment_score AS (
    SELECT
        m.module_id,
        AVG(p.Marks_obtained) AS avg_assessment_score
    FROM
        modules m
    INNER JOIN
        worksessions ws ON m.module_id = ws.module_id
    INNER JOIN
        performance p ON ws.workSession_id = p.assessment_id
    WHERE
        ws.workType = 'Assessment'
    GROUP BY
        m.module_id
),
avg_performance_interns AS (
    SELECT
        m.module_id,
        AVG(p.Marks_obtained) AS avg_performance_intern
    FROM
        modules m
    INNER JOIN
        worksessions ws ON m.module_id = ws.module_id
    INNER JOIN
        performance p ON ws.workSession_id = p.assessment_id
    INNER JOIN
        users u ON p.email = u.email
    WHERE
        u.userType = 'Intern'
    GROUP BY
        m.module_id
),
avg_performance_employees AS (
    SELECT
        m.module_id,
        AVG(p.Marks_obtained) AS avg_performance_employee
    FROM
        modules m
    INNER JOIN
        worksessions ws ON m.module_id = ws.module_id
    INNER JOIN
        performance p ON ws.workSession_id = p.assessment_id
    INNER JOIN
        users u ON p.email = u.email
    WHERE
        u.userType = 'Employee'
    GROUP BY
        m.module_id
),
trainings_count AS (
    SELECT
        m.module_id,
        COUNT( ws.workType) AS training_count
    FROM
        modules m
    INNER JOIN
        worksessions ws ON m.module_id = ws.module_id
    GROUP BY
        m.module_id,ws.worktype having ws.worktype = 'Training'
),

-- CTE to calculate the count of assessments
assessments_count AS (
    SELECT
        m.module_id,
        COUNT(CASE WHEN ws.workType = 'Assessment' THEN 1 END) AS assessment_count
    FROM
        modules m
    INNER JOIN
        worksessions ws ON m.module_id = ws.module_id
    GROUP BY
        m.module_id
),
final AS (
    SELECT
        mws.module_id,
        mws.worksession_count AS WorkSessions_Count,
        DATEDIFF('SECOND', mct.earliest_startime, mct.latest_endtime) AS Completion_time,
        aas.avg_assessment_score AS Avg_Assessment_Score,
        api.avg_performance_intern AS Avg_Intern_Score,
        ape.avg_performance_employee AS Avg_Employee_Score,
        tc.training_count AS No_of_Trainings,
        ac.assessment_count AS No_of_Assessments
    FROM
        modules_with_worksessions mws
    INNER JOIN
        module_completion_time mct ON mws.module_id = mct.module_id
    INNER JOIN
        avg_assessment_score aas ON mws.module_id = aas.module_id
    INNER JOIN
        avg_performance_interns api ON mws.module_id = api.module_id
    INNEr JOIN
        avg_performance_employees ape ON mws.module_id = ape.module_id
    INNER JOIN
        trainings_count tc ON mws.module_id = tc.module_id
    INNER JOIN
        assessments_count ac ON mws.module_id = ac.module_id
)

-- Selecting from the final CTE to get the result
SELECT * FROM final
