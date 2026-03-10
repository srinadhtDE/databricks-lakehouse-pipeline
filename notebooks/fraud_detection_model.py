# Databricks notebook source

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

from utils.config_loader import load_config, get_logger

logger = get_logger("fraud_model_training")

config = load_config("../configs/pipeline_config.yaml")

logger.info("Starting fraud detection model training")

# --------------------------------------------
# Load Feature Table
# --------------------------------------------

features_df = spark.table("transaction_features")

logger.info("Loaded transaction_features table")

# --------------------------------------------
# Feature Selection
# --------------------------------------------

feature_columns = [
    "transaction_amount",
    "transaction_hour",
    "transaction_day_of_week",
    "high_value_transaction",
    "is_weekend_transaction"
]

assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features"
)

# --------------------------------------------
# Train/Test Split
# --------------------------------------------

train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)

logger.info("Training and test datasets created")

# --------------------------------------------
# Logistic Regression Model
# --------------------------------------------

lr = LogisticRegression(
    featuresCol="features",
    labelCol="fraud_flag",
    maxIter=10
)

pipeline = Pipeline(stages=[assembler, lr])

model = pipeline.fit(train_df)

logger.info("Model training completed")

# --------------------------------------------
# Predictions
# --------------------------------------------

predictions = model.transform(test_df)

# --------------------------------------------
# Model Evaluation
# --------------------------------------------

evaluator = BinaryClassificationEvaluator(
    labelCol="fraud_flag",
    metricName="areaUnderROC"
)

auc = evaluator.evaluate(predictions)

logger.info(f"Model AUC score: {auc}")

# --------------------------------------------
# Save Model
# --------------------------------------------

model.write().overwrite().save("/mnt/models/fraud_detection_model")

logger.info("Fraud detection model saved successfully")
