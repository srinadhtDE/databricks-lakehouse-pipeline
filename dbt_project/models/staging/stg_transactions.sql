SELECT
    transaction_id,
    user_id,
    merchant_id,
    transaction_amount,
    transaction_type,
    payment_method,
    transaction_status,
    transaction_timestamp,
    country,
    fraud_flag
FROM bronze_transactions
WHERE transaction_status = 'completed'
