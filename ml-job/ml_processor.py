import os
import json
import boto3
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import statsmodels.api as sm


# -----------------------------------------------------------
# Load ALL recent metrics JSONs from S3 (time-series)
# -----------------------------------------------------------
def load_all_metrics(bucket, limit=30):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )

    objects = s3.list_objects_v2(Bucket=bucket, Prefix="raw-metrics/")["Contents"]
    files = sorted(objects, key=lambda x: x["LastModified"])

    # Use last N scrapes to build time series
    files = files[-limit:]

    print(f"[INFO] Loading {len(files)} metric files for time-series…")

    all_rows = []

    for f in files:
        key = f["Key"]
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read().decode()

        data = json.loads(raw)
        df = process_metrics(data)
        all_rows.append(df)

    return pd.concat(all_rows, ignore_index=True)


# -----------------------------------------------------------
# Convert Prometheus metric JSON to a DataFrame
# -----------------------------------------------------------
def process_metrics(metrics_json):
    rows = []

    cpu_section = metrics_json.get("cpu_usage", {})
    results = cpu_section.get("data", {}).get("result", [])

    for item in results:
        try:
            pod = item["metric"].get("pod")
            timestamp = float(item["value"][0])
            value = float(item["value"][1])

            rows.append({
                "pod": pod,
                "timestamp": timestamp,
                "value": value
            })

        except Exception as e:
            print(f"[WARN] Skipping row due to error: {e}\nRow = {item}")
            continue

    return pd.DataFrame(rows)


# -----------------------------------------------------------
# ML Processing: ARIMA + Anomaly Detection
# -----------------------------------------------------------
def run_ml(df):
    pod_predictions = {}
    unique_pods = df["pod"].unique()

    for pod in unique_pods:
        pod_data = df[df["pod"] == pod].copy().sort_values("timestamp")

        pod_data["timestamp"] = pd.to_datetime(pod_data["timestamp"], unit="s")
        pod_data = pod_data.set_index("timestamp").asfreq("5min")
        pod_data["value"] = pod_data["value"].interpolate()

        # Always run anomaly detection — even with low samples
        series = pod_data["value"]

        # ---- Isolation Forest ----
        try:
            scaler = StandardScaler()
            pod_data["scaled"] = scaler.fit_transform(pod_data[["value"]])

            iso = IsolationForest(contamination=0.15, n_estimators=200, random_state=42)
            pod_data["iso_anomaly"] = iso.fit_predict(pod_data[["scaled"]])
        except Exception:
            pod_data["iso_anomaly"] = 1  # no anomalies by default

        # ---- Z-score ----
        mean = pod_data["value"].mean()
        std = pod_data["value"].std()

        pod_data["z_score"] = (
            (pod_data["value"] - mean) / std if std != 0 else 0
        )
        pod_data["z_anomaly"] = np.abs(pod_data["z_score"]) > 2

        pod_data["final_anomaly"] = (
            pod_data["iso_anomaly"].eq(-1) | pod_data["z_anomaly"]
        )

        anomaly_count = int(pod_data["final_anomaly"].sum())

        # ---- ARIMA FORECAST (only if enough data) ----
        forecast_mean = None
        #---------IMPORTANT-------------
        if len(pod_data) >= 3:
            try:
                model = sm.tsa.ARIMA(series, order=(3, 1, 1))
                model_fit = model.fit()
                forecast = model_fit.forecast(steps=5)
                forecast_mean = float(forecast.mean())
            except Exception:
                forecast_mean = None

        # ---- Risk Logic ----
        if forecast_mean and forecast_mean > 0.8:
            risk = "HIGH"
        elif anomaly_count >= 3:
            risk = "MEDIUM"
        else:
            risk = "LOW"

        pod_predictions[pod] = {
            "forecast_next_5_mean": forecast_mean,
            "anomaly_count": anomaly_count,
            "risk_level": risk,
        }

    return pod_predictions


# -----------------------------------------------------------
# Upload predictions to S3
# -----------------------------------------------------------
def upload_predictions(predictions, bucket):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )

    key = "model-output/model_predictions.json"

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(predictions, indent=4),
    )

    print(f"[INFO] Uploaded predictions → s3://{bucket}/{key}")


# -----------------------------------------------------------
# Main
# -----------------------------------------------------------
def main():
    bucket = os.getenv("S3_BUCKET", "kubesentinel-metrics")

    print("\n[INFO] Step 1: Loading time-series metrics…")
    df = load_all_metrics(bucket)

    print("[INFO] Step 2: Running ML models…")
    predictions = run_ml(df)

    print("[INFO] Step 3: Uploading predictions…")
    upload_predictions(predictions, bucket)

    print("\n[INFO] ML Processing Complete ✔\n")


if __name__ == "__main__":
    main()
