SELECT COUNT(*) 
FROM silver_transactions
WHERE amount IS NULL;

SELECT COUNT(*) 
FROM bronze_transactions
WHERE transaction_id IS NULL;
