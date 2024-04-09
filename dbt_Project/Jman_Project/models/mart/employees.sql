WITH interns AS (
    SELECT * 
    FROM {{ref("stg_users")}} 
    WHERE usertype = 'Employee'
),
modules AS (
    SELECT 
        *
    FROM {{ref("stg_modules")}}
),
worksessions AS (
    SELECT 
        * 
    FROM {{ref("stg_worksessions")}}
),
performance AS (
    SELECT 
        * 
    FROM {{ref("stg_performance")}}
),
intern_performance AS (
    SELECT 
        i.email,
        i.full_name,
        m.module_id,
        AVG(p.marks_obtained) AS avg_assessment_score
    FROM 
        interns i
    JOIN 
        performance p ON i.email = p.email
    JOIN 
        worksessions ws ON p.assessment_id = ws.worksession_id
    JOIN 
        modules m ON ws.module_id = m.module_id
    WHERE 
        ws.worktype = 'Assessment'
    GROUP BY 
        i.email, m.module_id,i.full_name
)

SELECT * FROM intern_performance
