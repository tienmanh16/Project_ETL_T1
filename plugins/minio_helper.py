import boto3
from botocore.client import Config
import json
from plugins.config import config
import openpyxl
from io import BytesIO

class MinioHelper:
    def __init__(self):
        self.s3 = boto3.client(
            's3',
            endpoint_url=config.MINIO_ENDPOINT,
            aws_access_key_id=config.MINIO_ACCESS_KEY,
            aws_secret_access_key=config.MINIO_SECRET_KEY,
            config=Config(signature_version='s3v4')
        )

    def write_json(self, bucket_name=config.MINIO_BUCKET, object_name=None, json_data=None):
        try:
            self.s3.put_object(Bucket=bucket_name, Key=object_name, Body=json_data, ContentType='application/json')
        except Exception as e:
            raise e
        
    def read_xlsx(self, object_name, bucket_name=config.MINIO_BUCKET):
        try:
             # Tải tệp từ MinIO
            response = self.s3.get_object(Bucket=bucket_name, Key=object_name)
            file_stream = response['Body'].read()

            # Trả về nội dung của tệp Excel dưới dạng BytesIO
            return BytesIO(file_stream)
        except Exception as e:
            raise e
    def upload_xlsx_to_minio(self, file, object_name, bucket_name=config.MINIO_BUCKET):
        try:
            # Đọc nội dung của file Excel
            file_content = file.read()

            # Upload tệp lên MinIO
            self.s3.put_object(
                Bucket=bucket_name,
                Key=object_name,
                Body=file_content,
                ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            print("Upload thành công tệp Excel lên MinIO")
        except Exception as e:
            raise e
    