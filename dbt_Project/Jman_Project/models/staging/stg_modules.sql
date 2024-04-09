
with required_fields as(
    select 
    _id as module_id,
    trainingname as training_name,
    coe_name as coe_name,
    usertype as usertype,
    TO_TIMESTAMP_TZ(DATE, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS moduledateinfo,
    worksessions as worksession_id

    from {{source('Jman_Project','modules')}}

)

select * from required_fields