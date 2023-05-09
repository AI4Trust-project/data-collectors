from pulsar import Function
from minio import Minio
import urllib.request
import json
import pandas as pd
import io

# The classic ExclamationFunction that appends an exclamation at the end
# of the input
class SinkCSV(Function):
    def __init__(self):
        self.minio_client = Minio(
            "minio_url",
            access_key="key",
            secret_key="key",
            secure=False,
        )

    def create_or_update_csv_in_minio(self, data):

        file_exists = False
        try:
            obj = self.minio_client.get_object(
                "csvbucket",
                "test.csv")
            df = pd.read_csv(obj)
            file_exists = True
        except Exception:
            df = pd.DataFrame(data, index=data.keys())

        if file_exists:
            my_new_data = pd.DataFrame(data, index=data.keys())
            df = pd.concat([df, my_new_data], keys=data.keys()).drop_duplicates().reset_index(drop=True)


        csv = df.to_csv(index=False).encode('utf-8')

        self.minio_client.put_object(
            "csvbucket",
            "test.csv",
            data=io.BytesIO(csv),
            length=len(csv),
            content_type='application/csv')

    def process(self, input, context):
        logger = context.get_logger()
        comment = json.loads(input)

        self.create_or_update_csv_in_minio(comment)
    
