import os
from pathlib import Path

import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


SILVER_PATH = Path(os.environ.get("SILVER_PATH", "/data/lake/silver/orders_delta"))
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
EXPERIMENT_NAME = os.environ.get("MLFLOW_EXPERIMENT_NAME", "orders-mvp")
REGISTERED_MODEL_NAME = os.environ.get("REGISTERED_MODEL_NAME", "orders_total_price_model")


def find_one_parquet(delta_dir: Path) -> Path:
    files = list(delta_dir.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found under {delta_dir}")
    return files[0]


def main():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    parquet_file = find_one_parquet(SILVER_PATH)
    df = pd.read_parquet(parquet_file)

    # Minimalny model: przewidujemy total_price na podstawie quantity (i opcjonalnie id-ków)
    required_cols = {"quantity", "total_price"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in silver data: {missing}. Available: {list(df.columns)}")

    # Feature engineering (super proste, MVP)
    X = df[["quantity"]].copy()
    y = df["total_price"].astype(float)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    with mlflow.start_run():
        model = LinearRegression()
        model.fit(X_train, y_train)

        preds = model.predict(X_test)

        rmse = mean_squared_error(y_test, preds, squared=False)
        mae = mean_absolute_error(y_test, preds)
        r2 = r2_score(y_test, preds)

        mlflow.log_param("model_type", "LinearRegression")
        mlflow.log_param("features", "quantity")
        mlflow.log_metric("rmse", float(rmse))
        mlflow.log_metric("mae", float(mae))
        mlflow.log_metric("r2", float(r2))

        # zapis modelu jako artefakt
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            registered_model_name=REGISTERED_MODEL_NAME,
        )

        print("Training done.")
        print("Metrics:", {"rmse": rmse, "mae": mae, "r2": r2})
        print("Registered model:", REGISTERED_MODEL_NAME)


if __name__ == "__main__":
    main()
