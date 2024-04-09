with required_fields as (

    select 

    * 
    from {{source('Jman_Project','performance')}}
)
select * from required_fields 