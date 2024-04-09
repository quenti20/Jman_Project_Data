WITH required_fields AS (
    SELECT 
        _ID as worksession_id,
        worktype as worktype,
        module_id as module_id,
        trainer_name as trainer_name,

        TO_TIMESTAMP_TZ(DATENOW, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS dateinfo,
        TO_TIMESTAMP_TZ(START_TIME, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS start_timeinfo,
        TO_TIMESTAMP_TZ(END_TIME, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS end_timeinfo,
        testName

    FROM {{source('Jman_Project', 'worksessions')}}
)
SELECT * FROM required_fields


