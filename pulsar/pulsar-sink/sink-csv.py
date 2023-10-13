import io
import json
import os

import pandas as pd
from minio import Minio
from pulsar import Function


# The classic ExclamationFunction that appends an exclamation at the end
# of the input
class SinkCSV(Function):
    def __init__(self):
        self.minio_client = Minio(
            os.environ.get("MINIO_HOME"),
            access_key=os.environ.get("MINIO_ACCESS_KEY"),
            secret_key=os.environ.get("MINIO_SECRET_KEY"),
            secure=False,
        )

    def create_or_update_csv_in_minio(self, data):

        file_exists = False
        try:
            obj = self.minio_client.get_object("csvbucket", "test.csv")
            df = pd.read_csv(obj)
            file_exists = True
        except Exception:
            df = pd.DataFrame(data, index=data.keys())

        if file_exists:
            my_new_data = pd.DataFrame(data, index=data.keys())
            df = (
                pd.concat([df, my_new_data], keys=data.keys())
                .drop_duplicates()
                .reset_index(drop=True)
            )

        csv = df.to_csv(index=False).encode("utf-8")

        try:
            self.minio_client.put_object(
                "csvbucket",
                "test.csv",
                data=io.BytesIO(csv),
                length=len(csv),
                content_type="application/csv",
            )
        except Exception as e:
            print(e)

    def process(self, input, context):
        logger = context.get_logger()
        comment = json.loads(input)

        self.create_or_update_csv_in_minio(comment)
