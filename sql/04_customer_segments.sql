-- Customer segments by order frequency with revenue concentration.
SELECT
    segment,
    COUNT(*) AS customers,
    SUM(orders) AS orders,
    SUM(lifetime_net_revenue) AS net_revenue,
    ROUND(100 * SUM(lifetime_net_revenue) / SUM(SUM(lifetime_net_revenue)) OVER (), 1)
        AS pct_of_revenue,
    ROUND(AVG(average_order_value), 2) AS avg_order_value
FROM customer_activity
GROUP BY segment
ORDER BY net_revenue DESC
