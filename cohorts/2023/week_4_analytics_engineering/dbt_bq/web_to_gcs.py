import os

import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

load_dotenv()

init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download'

# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name 
        file_name = service + '_tripdata_' + year + '-' + month + '.csv'

        # download 
        request_url = f"{init_url}/{service}/{file_name}.gz"

        os.system(f"wget {request_url} -O data/{file_name}.gz ")
        os.system(f"gunzip data/{file_name}.gz ")

        df = pd.read_csv('data/' + file_name, low_memory=False)

        df['PULocationID'] = df.PULocationID.astype(str)
        df['DOLocationID'] = df.DOLocationID.astype(str)

        if service == 'green':
            df['lpep_pickup_datetime'] = pd.to_datetime(df.lpep_pickup_datetime)
            df['lpep_dropoff_datetime'] = pd.to_datetime(df.lpep_dropoff_datetime)
        elif service == 'yellow':
            df['tpep_pickup_datetime'] = pd.to_datetime(df.tpep_pickup_datetime)
            df['tpep_dropoff_datetime'] = pd.to_datetime(df.tpep_dropoff_datetime)

        print(f"Local: {file_name}")

        # read it back into a parquet file
        file_name = file_name.replace('.csv', '.parquet')
        df.to_parquet('data/' + file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", 'data/' + file_name)
        print(f"GCS: {service}/{file_name}")


if __name__ == "__main__":

    web_to_gcs('2019', 'green')
    web_to_gcs('2020', 'green')
    web_to_gcs('2019', 'yellow')
    web_to_gcs('2020', 'yellow')
    