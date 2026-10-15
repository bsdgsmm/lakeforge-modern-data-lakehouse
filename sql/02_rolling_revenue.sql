-- Daily net revenue with a trailing 3-day moving average and day-over-day change.
SELECT
    revenue_date,
    orders,
    net_revenue,
    ROUND(
        AVG(net_revenue) OVER (ORDER BY revenue_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
        2
    ) AS net_revenue_3d_avg,
    ROUND(
        100 * (net_revenue - LAG(net_revenue) OVER (ORDER BY revenue_date))
            / LAG(net_revenue) OVER (ORDER BY revenue_date),
        1
    ) AS pct_change_vs_prior_day
FROM daily_revenue
ORDER BY revenue_date
