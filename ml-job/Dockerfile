FROM python:3.10-slim

WORKDIR /app

COPY ml_processor.py .

RUN pip install boto3 pandas numpy statsmodels scikit-learn

CMD ["python", "ml_processor.py"]
