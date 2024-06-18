with 

source as (

    select orders_id,shipping_fee,logCost,ship_cost from {{ source('raw', 'ship') }}

),

renamed as (

    select *

    from source

)

select * from renamed
