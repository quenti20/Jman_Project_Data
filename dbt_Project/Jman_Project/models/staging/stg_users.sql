with required_fields as (

    select 
    _id as user_id,
    email as email,
    name as full_name,
    usertype as usertype,
    haschanged as haschanged    
    from {{source('Jman_Project','users')}}
)
select * from required_fields 