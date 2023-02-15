import os

from dotenv import load_dotenv
import pandas as pd
from google.cloud import storage


load_dotenv()

data_source_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/'
BUCKET = os.environ.get("GCP_GCS_BUCKET")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name 
        file_name = 'fhv_tripdata_' + year + '-' + month + '.csv'

        os.system(f"wget {data_source_url + file_name + '.gz'} -O {'data/' + file_name + '.gz'} ")
        os.system(f"gunzip {'data/' + file_name + '.gz'} ")

        df = pd.read_csv('data/' + file_name)
        df['pickup_datetime'] = pd.to_datetime(df.pickup_datetime)
        df['dropOff_datetime'] = pd.to_datetime(df.dropOff_datetime)
        df['PUlocationID'] = df.PUlocationID.astype(float)
        df['DOlocationID'] = df.DOlocationID.astype(float)
        df['SR_Flag'] = df.SR_Flag.astype(float)
        
        file_name = file_name.replace('.csv', '.parquet')
        df.to_parquet('data/' + file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"fhv_tripdata/{file_name}", 'data/' + file_name)
        print(f"GCS: fhv_tripdata/{file_name}")


if __name__ =="__main__":
    web_to_gcs('2019')
