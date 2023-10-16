FROM python:3.8-slim

WORKDIR /usr/src/app

RUN pip install --no-cache-dir minio pandas

COPY data_consumer.py .

CMD ["python", "data_consumer.py"]

