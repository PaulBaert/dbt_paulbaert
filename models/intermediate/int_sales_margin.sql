with sales as (
    select 
        date_date,
        orders_id,
        pdt_id,
        revenue,
        quantity
    from 
        {{ source('raw', 'sales') }}
),
product as (
    select 
        products_id,
        purchSE_PRICE
    from 
        {{ source('raw', 'product') }}
),
joined as (
    select
        sales.date_date,
        sales.orders_id,
        sales.pdt_id,
        sales.revenue,
        sales.quantity,
        cast(product.purchSE_PRICE as FLOAT64) as purchase_price,
        sales.quantity * cast(product.purchSE_PRICE as FLOAT64) as purchase_cost,
        sales.revenue - (sales.quantity * cast(product.purchSE_PRICE as FLOAT64)) as margin
    from 
        sales
    join 
        product 
    on 
        sales.pdt_id = product.products_id 
)
select * from joined