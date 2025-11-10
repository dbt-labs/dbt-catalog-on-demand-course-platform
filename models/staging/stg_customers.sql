{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge'
    )
}}

with

source as (

    select * from {{ source('jaffle_shop', 'customers') }}

),

renamed as (

    select

        ----------  ids
        id as customer_id,

        ---------- text
        name as customer_name

    from source

)

select * from renamed
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where customer_id > (select max(customer_id) from {{ this }}) 
{% endif %}