import os
import warnings

import pandas as pd
import mlflow
import mlflow.sklearn

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

# delta-rs reader (bez Sparka)
from deltalake import DeltaTable


def load_silver_from_minio_delta() -> pd.DataFrame:
    """
    Czyta Delta Table z MinIO/S3 przy pomocy deltalake (delta-rs), bez Sparka.
    Wymaga SILVER_URI w stylu: s3://lake/silver/orders_delta
    """
    silver_uri = os.environ.get("SILVER_URI", "s3://lake/silver/orders_delta")

    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    # delta-rs używa "storage_options" (AWS_*). Dla MinIO ważne: HTTP + endpoint.
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_ENDPOINT_URL": minio_endpoint,
        "AWS_ALLOW_HTTP": "true",
        "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
    }

    dt = DeltaTable(silver_uri, storage_options=storage_options)
    df = dt.to_pandas()  # małe dane = OK na MVP
    return df


def main():
    warnings.filterwarnings("ignore")

    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow_test:5000")
    experiment_name = os.environ.get("MLFLOW_EXPERIMENT_NAME", "orders-mvp")
    registered_model_name = os.environ.get("REGISTERED_MODEL_NAME", "orders_mvp_model")

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)

    df = load_silver_from_minio_delta()

    # Minimalny sanity check
    required_cols = {"quantity", "total_price"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Brakuje kolumn w Silver: {missing}. Mam: {list(df.columns)}")

    # przygotowanie danych
    df = df.copy()
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce")
    df = df.dropna(subset=["quantity", "total_price"])

    X = df[["quantity"]]
    y = df["total_price"]

    # split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    with mlflow.start_run(run_name="linreg_quantity_to_total_price"):
        model = LinearRegression()
        model.fit(X_train, y_train)

        preds = model.predict(X_test)

        mse = mean_squared_error(y_test, preds)
        rmse = mse ** 0.5

        mae = mean_absolute_error(y_test, preds)
        r2 = r2_score(y_test, preds)

        mlflow.log_param("model_type", "LinearRegression")
        mlflow.log_param("features", "quantity")
        mlflow.log_metric("rmse", float(rmse))
        mlflow.log_metric("mae", float(mae))
        mlflow.log_metric("r2", float(r2))

        # Spróbuj z rejestrowaniem (jeśli registry działa). Jak nie, to fallback.
        try:
            mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path="model",
                registered_model_name=registered_model_name,
            )
            registered_info = f"Registered model: {registered_model_name}"
        except Exception as e:
            mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path="model",
            )
            registered_info = f"Model zapisany jako artifact (bez rejestru). Powód: {type(e).__name__}: {e}"

        print("Training done.")
        print("Rows used:", len(df))
        print("Metrics:", {"rmse": float(rmse), "mae": float(mae), "r2": float(r2)})
        print(registered_info)
        print("Tracking URI:", tracking_uri)
        print("Experiment:", experiment_name)


if __name__ == "__main__":
    main()
