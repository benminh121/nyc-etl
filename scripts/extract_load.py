import sys
import os
from glob import glob
from minio import Minio

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)
from helpers import load_cfg
from minio_utils import MinIOClient

CFG_FILE = "./config/datalake.yaml"

def extract_load(endpoint_url, access_key, secret_key, source, dest):
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]

    client = MinIOClient(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key
    )

    client.create_bucket(datalake_cfg["bucket_name_1"])

    print(f"Uploading")
    client_minio = client.create_conn()
    client_minio.fput_object(
        bucket_name=datalake_cfg["bucket_name_1"],
        object_name=dest,
        file_path=source
            )