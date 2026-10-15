-- Revenue and buyer counts by customer country.
SELECT
    c.country_code,
    COUNT(DISTINCT f.customer_id) AS buyers,
    COUNT(DISTINCT f.order_id) AS orders,
    ROUND(SUM(f.net_amount), 2) AS net_revenue,
    ROUND(SUM(f.net_amount) / COUNT(DISTINCT f.order_id), 2) AS avg_order_value
FROM fact_order_items AS f
JOIN dim_customer AS c ON c.customer_id = f.customer_id
WHERE f.is_revenue
GROUP BY c.country_code
ORDER BY net_revenue DESC
