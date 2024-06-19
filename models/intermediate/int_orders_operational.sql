with int_orders_margin as (
    select * from {{ ref('int_orders_margin') }}
),

ship as (
    select 
        orders_id,
        ROUND(CAST(shipping_fee AS FLOAT64), 2) as shipping_fee,
        ROUND(CAST(logCost AS FLOAT64), 2) as logCost,
        ROUND(CAST(ship_cost AS FLOAT64), 2) as ship_cost
    from 
        {{ source('raw', 'ship') }}
),

joined as (
    select
        iso.date_date,
        iso.orders_id,
        iso.margin,  -- Assurez-vous que margin est bien dans la sélection de iso
        ROUND((iso.margin + s.shipping_fee) - (s.logCost + s.ship_cost), 2) as operational_margin
    from 
        int_orders_margin iso
    left join 
        ship s on iso.orders_id = s.orders_id
)

select
    date_date,
    orders_id,
    SUM(margin) as total_margin,
    SUM(operational_margin) as total_operational_margin
from joined
group by date_date, orders_id