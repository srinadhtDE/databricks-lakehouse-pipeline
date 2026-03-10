from pyspark.sql.functions import sum, countDistinct

gold_df = spark.sql("""
SELECT
    transaction_date,
    SUM(amount) AS daily_revenue,
    COUNT(DISTINCT user_id) AS active_users
FROM silver_transactions
GROUP BY transaction_date
""")

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_daily_metrics")

print("Gold analytics table created")
