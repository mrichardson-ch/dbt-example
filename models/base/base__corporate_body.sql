with source as (
    select * from {{ ref('corporate_body')}}
),
renamed as (
    select
        corporate_body_id
        ,incorporation_number
        ,cast(incorporation_date as DATE) as incorporation_date
        ,action_code_type_id
        ,action_code_date as action_code_updated_at
        ,corporate_body_name
    from source
)

select * from renamed