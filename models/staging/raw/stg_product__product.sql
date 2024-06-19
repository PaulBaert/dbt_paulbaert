with 

source as (

    select products_id,
    purchSE_PRICE AS purchase_price
     from {{ source('raw', 'product') }}

),

renamed as (

    select *

    from source

)

select * from renamed
