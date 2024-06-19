
WITH int_sales_margin AS (
    SELECT 
        date_date,
        orders_id,
        ROUND(SUM(revenue), 2) AS total_revenue,
        ROUND(SUM(purchase_cost), 2) AS total_purchase_cost,
        ROUND(SUM(margin), 2) AS margin,
        ROUND((SUM(margin) / SUM(revenue) * 100), 2) AS margin_percentage
    FROM 
        {{ ref('int_sales_margin') }}
    GROUP BY 
        date_date, orders_id
),

ship AS (
    SELECT 
        orders_id,
        ROUND(CAST(shipping_fee AS FLOAT64), 2) AS shipping_fee,
        ROUND(CAST(logCost AS FLOAT64), 2) AS logCost,
        ROUND(CAST(ship_cost AS FLOAT64), 2) AS ship_cost
    FROM 
        {{ source('raw', 'ship') }}
),

joined AS (
    SELECT
        ism.date_date,
        ism.orders_id,
        ism.total_revenue AS revenue,
        ism.margin,
        ROUND((ism.margin + s.shipping_fee) - (s.logCost + s.ship_cost), 2) AS operational_margin
    FROM 
        int_sales_margin ism
    LEFT JOIN 
        ship s ON ism.orders_id = s.orders_id
)

SELECT
    date_date,
    COUNT(orders_id) AS nb_transactions,
    SUM(revenue) AS revenue,
    SUM(revenue) / COUNT(orders_id) AS average_basket,
    SUM(margin) AS margin,
    SUM(operational_margin) AS operational_margin
FROM 
    joined
GROUP BY 
    date_date
ORDER BY 
    date_date DESC