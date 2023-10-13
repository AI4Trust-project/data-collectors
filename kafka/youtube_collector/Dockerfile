FROM python:3.8-slim

WORKDIR /usr/src/app

RUN pip install --no-cache-dir googleapi pulsar-client kafka-python

COPY youtube_collector.py .

CMD ["python", "youtube_collector.py"]

