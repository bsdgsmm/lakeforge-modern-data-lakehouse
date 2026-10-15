-- Net revenue per category per day, pivoted so trends are visible side by side.
SELECT *
FROM (
    SELECT f.order_date, p.category, f.net_amount
    FROM fact_order_items AS f
    JOIN dim_product AS p ON p.product_id = f.product_id
    WHERE f.is_revenue
)
PIVOT (
    ROUND(SUM(net_amount), 2)
    FOR category IN ('electronics', 'home', 'books', 'sports', 'beauty', 'uncategorized')
)
ORDER BY order_date
