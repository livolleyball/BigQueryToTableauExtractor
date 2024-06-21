from google.cloud import storage
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"your_google_credentials_file.json"

def create_bucket(bucket_name):

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name=bucket_name)

    # 现在实际创建 bucket
    new_bucket = storage_client.create_bucket(bucket, location="us-east4")

    print(f"Bucket {new_bucket.name} created.")

if __name__ == '__main__':

    bucket_name = "your_bucket_name"
    create_bucket(bucket_name)