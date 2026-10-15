-- Top 10 products by net revenue with their share of category revenue.
SELECT
    revenue_rank,
    product_id,
    product_name,
    category,
    units_sold,
    orders,
    net_revenue,
    ROUND(category_revenue_share * 100, 1) AS pct_of_category
FROM product_performance
WHERE units_sold > 0
ORDER BY revenue_rank
LIMIT 10
