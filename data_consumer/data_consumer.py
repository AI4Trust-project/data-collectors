import os
import time
from io import BytesIO

import pandas as pd
from minio import Minio

# Set up MinIO client
client = Minio(
    os.environ.get("MINIO_HOME"),
    access_key=os.environ.get("MINIO_ACCESS_KEY"),
    secret_key=os.environ.get("MINIO_SECRET_KEY"),
    secure=False,
)

try:
    while True:

        # Read CSV file from MinIO
        bucket_name = "csvbucket"
        file_name = "test.csv"
        csv_data = client.get_object(bucket_name, file_name)

        # Load CSV data into a Pandas DataFrame
        df = pd.read_csv(BytesIO(csv_data.read()))

        # Count lines with the label HATE_SPEECH
        print(
            "Number of hate speech comments: {} from {}\n".format(
                len(df.loc[df["hate_speech_classification"] == "HATE_SPEECH"]), len(df)
            )
        )
        time.sleep(5)

except Exception as e:
    print(e)
