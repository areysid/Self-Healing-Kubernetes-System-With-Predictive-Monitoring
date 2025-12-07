import requests
import json
import boto3
import os
from datetime import datetime

# -------------------------------
# CONFIGURATION
# -------------------------------

# Prometheus service inside Minikube (your working endpoint)
PROMETHEUS_URL = os.getenv(
    "PROMETHEUS_URL",
    "http://monitoring-kube-prometheus-prometheus:9090"
)

# Your S3 bucket
S3_BUCKET = os.getenv("S3_BUCKET", "kubesentinel-metrics")
S3_PREFIX = "raw-metrics/"   # folder inside S3 bucket

# PromQL queries to extract
QUERIES = {
    "cpu_usage": 'sum(rate(container_cpu_usage_seconds_total[2m])) by (pod)',
    "memory_usage": 'sum(container_memory_working_set_bytes) by (pod)',
    "pod_restarts": 'sum(kube_pod_container_status_restarts_total) by (pod)'
}

# -------------------------------
# FUNCTIONS
# -------------------------------

def fetch_prometheus_metrics():
    results = {}
    for name, query in QUERIES.items():
        url = f"{PROMETHEUS_URL}/api/v1/query"
        response = requests.get(url, params={"query": query})

        if response.status_code == 200:
            results[name] = response.json()
        else:
            results[name] = {
                "error": f"Failed query {name}",
                "status_code": response.status_code
            }

    return results


def upload_to_s3(data):
    # boto3 automatically reads AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY from env vars
    s3 = boto3.client("s3")

    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    filename = f"metrics-{timestamp}.json"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{S3_PREFIX}{filename}",
        Body=json.dumps(data),
        ContentType="application/json"
    )

    print(f"[OK] Uploaded to s3://{S3_BUCKET}/{S3_PREFIX}{filename}")


def main():
    print("[INFO] Fetching Prometheus metrics...")
    metrics = fetch_prometheus_metrics()

    print("[INFO] Uploading to S3...")
    upload_to_s3(metrics)

    print("[DONE] Export + upload complete.")


if __name__ == "__main__":
    main()
