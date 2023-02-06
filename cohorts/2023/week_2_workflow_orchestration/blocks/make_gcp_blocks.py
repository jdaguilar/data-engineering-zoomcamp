import json
import os

from dotenv import load_dotenv
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


load_dotenv()

gcp_service_account_path = os.path.join(os.getcwd(), os.getenv('GCP_SERVICE_ACCOUNT'))
gcp_cloud_storage_bucket = os.getenv('GCP_CLOUD_STORAGE_BUCKET')

with open(gcp_service_account_path, "r") as read_file:
    gcp_service_account = json.load(read_file)

credentials_block = GcpCredentials(
    service_account_info=gcp_service_account
).save(
    "zoom-gcp-creds", 
    overwrite=True
)

bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("zoom-gcp-creds"),
    bucket=gcp_cloud_storage_bucket
).save(
    "zoom-gcs", 
    overwrite=True
)
