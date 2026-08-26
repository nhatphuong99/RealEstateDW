import boto3, time
from parser.config import get_s3_bucket

s3 = boto3.client("s3")
bucket = get_s3_bucket()
key = "bronze/dataset/part=67.parquet"  # <-- thay đúng s3_key lấy từ bước 1 trước đó

head = s3.head_object(Bucket=bucket, Key=key)
print("Size (bytes):", head["ContentLength"])

t0 = time.time()
try:
    s3.download_file(bucket, key, "/tmp/test_download.parquet")
    print("OK, mat", time.time() - t0, "giay")
except Exception as e:
    print("Loai loi:", type(e).__name__)
    print("Nguyen nhan goc:", getattr(e, "last_exception", e))
