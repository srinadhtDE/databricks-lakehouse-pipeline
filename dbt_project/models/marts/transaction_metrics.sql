SELECT
DATE(transaction_timestamp) AS transaction_date,
COUNT(*) AS total_transactions,
SUM(transaction_amount) AS total_revenue,
COUNT(DISTINCT user_id) AS active_users,
AVG(transaction_amount) AS avg_transaction_value,
SUM(fraud_flag) AS fraud_transactions
FROM {{ ref('stg_transactions') }}
GROUP BY transaction_date
