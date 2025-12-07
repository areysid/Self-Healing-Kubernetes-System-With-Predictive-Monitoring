import os
import json
import boto3
from kubernetes import client, config

# -------------------------------------------------------
#  Check if pod actually exists in Kubernetes
# -------------------------------------------------------
def pod_exists(pod_name, namespace="default"):
    v1 = client.CoreV1Api()
    pods = v1.list_namespaced_pod(namespace)
    return any(p.metadata.name == pod_name for p in pods.items)


# -------------------------------------------------------
#  Kubernetes Auto-Healing: Restart a Pod
# -------------------------------------------------------
def restart_pod(pod_name, namespace="default"):
    try:
        v1 = client.CoreV1Api()
        print(f"[ACTION] Restarting pod: {pod_name}")
        v1.delete_namespaced_pod(name=pod_name, namespace=namespace)
    except Exception as e:
        print(f"[ERROR] Failed to restart {pod_name}: {e}")


# -------------------------------------------------------
#  MAIN PROCESS
# -------------------------------------------------------
def main():
    bucket = os.getenv("S3_BUCKET", "kubesentinel-metrics")
    key = os.getenv("PREDICTIONS_KEY", "model-output/model_predictions.json")
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    print(f"[INFO] Reading predictions from s3://{bucket}/{key}")

    # S3 client
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=region,
    )

    # -------------------------------------------------------
    # Load predictions JSON
    # -------------------------------------------------------
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read().decode("utf-8")
        predictions = json.loads(data)
    except Exception as e:
        print(f"[ERROR] Could not load predictions: {e}")
        return

    if not predictions:
        print("[WARN] Predictions file is empty — no actions taken.")
        return

    # -------------------------------------------------------
    # Connect to Kubernetes in-cluster API
    # -------------------------------------------------------
    try:
        config.load_incluster_config()
        print("[INFO] Connected to Kubernetes API")
    except Exception as e:
        print(f"[ERROR] Failed to load Kubernetes config: {e}")
        return

    print("\n[INFO] ML Pod Risk Summary:\n")

    # -------------------------------------------------------
    # Process each pod's ML risk classification
    # -------------------------------------------------------
    for pod_name, result in predictions.items():

        # Normalize risk string to avoid case/spacing mismatch
        raw_risk = result.get("risk_level", "UNKNOWN")
        risk = str(raw_risk).strip().upper()

        forecast = result.get("forecast_next_5_mean", None)
        anomalies = result.get("anomaly_count", 0)

        print(f" • Pod: {pod_name}")
        print(f"   - Risk level        : {risk}")
        print(f"   - Forecast CPU mean : {forecast}")
        print(f"   - Anomaly count     : {anomalies}")

        # Skip pods that are no longer running
        if not pod_exists(pod_name):
            print(f"   -> [SKIP] Pod does not exist anymore — skipping.\n")
            continue

        # ------------------------
        # Healing Logic
        # ------------------------
        if risk == "HIGH":
            print(f"   -> [HEALING] HIGH risk detected — restarting pod.\n")
            restart_pod(pod_name)

        elif risk == "MEDIUM" and anomalies >= 5:
            print(f"   -> [HEALING] MEDIUM risk + anomalies — restarting pod.\n")
            restart_pod(pod_name)

        else:
            print(f"   -> [OK] No action needed.\n")

    print("[INFO] Execution completed.\n")


# -------------------------------------------------------
#  Entry Point
# -------------------------------------------------------
if __name__ == "__main__":
    main()
