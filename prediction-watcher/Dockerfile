FROM python:3.10-slim

WORKDIR /app

RUN pip install boto3 kubernetes

COPY prediction_watcher.py .

CMD ["python", "prediction_watcher.py"]
