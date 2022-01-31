from minio import Minio

read_bucket = "read"
intermediate_bucket = "intermediate"
final_bucket = "final"

client = Minio(
    "127.0.0.1:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

buckets = client.list_buckets()
for bucket in buckets:
    print(bucket.name, bucket.creation_date)
