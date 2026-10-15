-- Compare recognised order revenue with net captured cash per day.
-- Gaps are expected: late-arriving orders are booked on their order date while their
-- payment is booked on the transaction date, and refunds land on the cancellation date.
SELECT
    revenue_date,
    net_revenue,
    captured_payments,
    refunded_payments,
    captured_payments - refunded_payments AS net_cash,
    captured_payments - refunded_payments - net_revenue AS cash_minus_revenue,
    failed_payments
FROM daily_revenue
ORDER BY revenue_date
