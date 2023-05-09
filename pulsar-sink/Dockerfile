# Use pulsar-functions-python-runner since we pack python function
FROM streamnative/pulsar-functions-python-runner:2.7.1
RUN pip install  minio pandas
# Copy function .py package into /pulsar directory
COPY sink-csv.py /pulsar/
