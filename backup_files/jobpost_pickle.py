import os
import pickle
import boto3
from datetime import datetime

# S3 연결 설정
s3 = boto3.client('s3')

# # 버킷이 없으면 생성하는 함수
# def create_bucket_if_not_exists(bucket_name, region_name):
#     try:
#         s3.head_bucket(Bucket=bucket_name)
#         print(f"Bucket '{bucket_name}' already exists.")
#     except:
#         # 버킷이 존재하지 않으면 생성
#         if region_name:
#             s3.create_bucket(
#                 Bucket=bucket_name,
#                 CreateBucketConfiguration={'LocationConstraint': region_name}
#             )
#         else:
#             s3.create_bucket(Bucket=bucket_name)
#         print(f"Bucket '{bucket_name}' created.")

# 파일을 피클 형태로 변환 후 S3에 업로드하는 함수
def upload_files_to_s3(file_paths, bucket_name, s3_base_path):
    # 버킷 생성 확인
    # create_bucket_if_not_exists(bucket_name, region_name)
    
    for file_path in file_paths:
        # 파일명 추출
        file_name = os.path.basename(file_path)
        
        # 파일 열기
        with open(file_path, 'rb') as f:
            # 피클로 덤프
            pickled_data = pickle.dumps(f.read())
            
            # S3 업로드 경로 생성 (기본 경로와 추가 경로 설정)
            s3_key = f"{s3_base_path}/{file_name}_{datetime.now().strftime('%Y%m%d')}.pkl"
            
            # S3에 업로드
            s3.put_object(Bucket=bucket_name, Key=s3_key, Body=pickled_data)
            print(f"{file_name} has been uploaded to S3 as {s3_key}")

# 주기적으로 실행할 파일 목록
file_paths_data = [
    '/home/ubuntu/job_crawling/saramin/saramin.py',
    '/home/ubuntu/job_crawling/incruit/incruit.py',
    '/home/ubuntu/job_crawling/jumpit/jumpit.py',
    '/home/ubuntu/job_crawling/wanted/wanted_pch.py',
    '/home/ubuntu/job_posting/query_base.sql'
]
# region_name = 'ap-northeast-2'
bucket_name = '@@@'
s3_base_path = '@@@'  # S3 내 경로

# S3 업로드 함수 호출 (기본 경로에 업로드)
upload_files_to_s3(file_paths_data, bucket_name, s3_base_path)
