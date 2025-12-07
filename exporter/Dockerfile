# Use official lightweight Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy requirements inline (we only need requests + boto3)
RUN pip install requests boto3

# Copy exporter script into container
COPY metrics_exporter.py .

# Entry point: run script
CMD ["python", "metrics_exporter.py"]
