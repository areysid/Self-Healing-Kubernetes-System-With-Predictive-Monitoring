#!/bin/bash

# Delete previous pod if exists
kubectl delete pod stress-demo --ignore-not-found

# Create a new Debian pod with a dummy command to keep it alive
kubectl run stress-demo --image=debian --restart=Never -- sleep infinity

echo "Waiting for pod to be in Running state..."
while [[ $(kubectl get pod stress-demo -o jsonpath='{.status.phase}') != "Running" ]]; do
    sleep 1
done

echo "Pod is running! Installing stress inside the pod..."

# Install stress and run it indefinitely
kubectl exec -it stress-demo -- bash -c "
    apt-get update &&
    apt-get install -y stress &&
    echo 'Starting infinite CPU stress...' &&
    stress --cpu 2
"
