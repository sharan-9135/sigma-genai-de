-- Monthly merchant performance report
-- Shows each merchant's monthly revenue and transaction volume
SELECT m.merchant_name,
       DATE_TRUNC('MONTH', t.transaction_date) as month,
       SUM(t.amount) as revenue,
       COUNT(*) as txn_count,
       SUM(CASE WHEN t.status = 'FAILED' THEN 1 ELSE 0 END) / COUNT(*) * 100 as fail_pct
FROM fact_transactions t, dim_merchant m
WHERE t.merchant_id = m.merchant_id
GROUP BY m.merchant_name, month
ORDER BY month, revenue DESC;
