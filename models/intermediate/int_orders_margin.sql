with int_sales_margin as (
    select * from {{ ref('int_sales_margin') }}
)

select
    date_date,orders_id,
    ROUND(sum(revenue),2) as total_revenue,  
    ROUND(sum(purchase_cost),2) as total_purchase_cost,
    ROUND(sum(margin),2) as margin,
    ROUND((sum(margin) / sum(revenue)*100),2) as margin_percentage
from int_sales_margin
group by date_date,orders_id

