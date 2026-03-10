import pandas as pd
import random
from datetime import datetime, timedelta

rows = []

start_time = datetime(2024, 1, 1)

for i in range(10000):

    rows.append({
        "transaction_id": i + 1,
        "user_id": random.randint(1000, 2000),
        "merchant_id": random.randint(1, 100),
        "transaction_amount": round(random.uniform(5, 500), 2),
        "transaction_type": random.choice(["purchase", "refund"]),
        "payment_method": random.choice(["card", "wallet", "upi"]),
        "transaction_status": random.choice(["completed", "failed"]),
        "transaction_timestamp": start_time + timedelta(minutes=i),
        "country": random.choice(["US", "UK", "IN", "CA"]),
        "fraud_flag": random.choice([0, 0, 0, 1])
    })

df = pd.DataFrame(rows)

df.to_csv("transactions.csv", index=False)

print("Generated 10000 transactions")
