import os
import boto3
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

bucket = os.getenv("S3_BUCKET_NAME")

# Test ghi 1 file thử
s3.put_object(Bucket=bucket, Key="bronze/test/hello.json", Body=b'{"status": "ok"}')
print("Upload thanh cong!")

# Test đọc lại
response = s3.get_object(Bucket=bucket, Key="bronze/test/hello.json")
print("Noi dung doc lai:", response["Body"].read().decode())

# Liệt kê object trong bucket
objects = s3.list_objects_v2(Bucket=bucket, Prefix="bronze/")
print("So object trong bronze/:", objects.get("KeyCount", 0))