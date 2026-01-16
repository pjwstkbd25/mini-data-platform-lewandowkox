import os

import mlflow
import mlflow.sklearn

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score


EXPERIMENT_NAME = os.environ.get("MLFLOW_EXPERIMENT_NAME", "orders-mvp")
TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow_test:5000")

SILVER_DELTA_DIR = os.environ.get("SILVER_DIR", "/data/lake/silver/orders_delta")

FEATURE_COL = "quantity"
TARGET_COL = "total_price"


def main():
    # Spark (Delta)
    spark = (
        SparkSession.builder
        .appName("train-mlflow")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    sdf = spark.read.format("delta").load(SILVER_DELTA_DIR)
    sdf = sdf.select(col(FEATURE_COL).cast("double"), col(TARGET_COL).cast("double")).dropna()

    pdf = sdf.toPandas()
    spark.stop()

    if len(pdf) < 10:
        raise ValueError(f"Not enough rows to train, got {len(pdf)}")

    X = pdf[[FEATURE_COL]]
    y = pdf[TARGET_COL]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    model = LinearRegression()

    with mlflow.start_run(run_name="linreg_quantity_to_total_price"):
        model.fit(X_train, y_train)
        preds = model.predict(X_test)

        rmse = mean_squared_error(y_test, preds, squared=False)
        mae = mean_absolute_error(y_test, preds)
        r2 = r2_score(y_test, preds)

        mlflow.log_param("model_type", "LinearRegression")
        mlflow.log_param("feature", FEATURE_COL)
        mlflow.log_param("target", TARGET_COL)

        mlflow.log_metric("rmse", float(rmse))
        mlflow.log_metric("mae", float(mae))
        mlflow.log_metric("r2", float(r2))

        mlflow.sklearn.log_model(model, "model")

        print("✅ Training done. Metrics:", {"rmse": rmse, "mae": mae, "r2": r2})


if __name__ == "__main__":
    main()
