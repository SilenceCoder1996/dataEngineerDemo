import boto3

MINIO_ENDPOINT = 'http://localhost:9000'  # 本地端口映射
ACCESS_KEY = 'minio'
SECRET_KEY = 'minio123'
BUCKET = 'imdb-movies'

s3 = boto3.client('s3',
                  endpoint_url=MINIO_ENDPOINT,
                  aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=SECRET_KEY)

# 列出所有 bucket
buckets = s3.list_buckets()
print("所有 Buckets:")
for bucket in buckets['Buckets']:
    print(" -", bucket['Name'])

# 列出特定 bucket 下的对象
print(f"\n Bucket: {BUCKET} 中的文件:")
objects = s3.list_objects_v2(Bucket=BUCKET)
for obj in objects.get('Contents', []):
    print(" -", obj['Key'])
