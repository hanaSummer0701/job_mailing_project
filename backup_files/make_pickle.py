import os
import pickle
from datetime import datetime

# 파일을 피클 형태로 변환 후 로컬에 저장하는 함수
def save_files_as_pickle(file_paths, base_path):
    for file_path in file_paths:
        # 파일명 추출
        file_name = os.path.basename(file_path)
        
        # 파일 열기
        with open(file_path, 'rb') as f:
            # 피클로 덤프
            pickled_data = pickle.dumps(f.read())
            
            # 로컬에 저장 경로 생성
            pickle_file_name = f"{base_path}/{file_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"
            
            # 피클 파일로 저장
            with open(pickle_file_name, 'wb') as pickle_file:
                pickle_file.write(pickled_data)
            print(f"{file_name} has been saved as {pickle_file_name}")

# 주기적으로 실행할 파일 목록
file_paths = [
    '/home/ubuntu/job_crawling/saramin/saramin.py',
    '/home/ubuntu/job_crawling/incruit/incruit.py',
    '/home/ubuntu/job_crawling/jumpit/jumpit.py',
    '/home/ubuntu/job_crawling/wanted/wanted_pch.py',
    '/home/ubuntu/job_posting/query_base.sql'
]
base_path = '/home/ubuntu/job_crawling/pickle_files'  # 로컬 저장 경로

# 파일을 피클로 변환하여 저장하는 함수 호출
save_files_as_pickle(file_paths, base_path)


# import os
# import pickle
# from datetime import datetime

# # 파일을 피클 형태로 변환 후 로컬에 저장하는 함수
# def save_files_as_pickle(file_paths, base_path, overwrite=False):
#     for file_path in file_paths:
#         # 파일명 추출
#         file_name = os.path.basename(file_path)
        
#         # 로컬에 저장될 피클 파일 경로
#         pickle_file_name = f"{base_path}/{file_name}.pkl"
        
#         # 덮어쓰기 설정에 따라 기존 파일 삭제
#         if overwrite and os.path.exists(pickle_file_name):
#             os.remove(pickle_file_name)
#             print(f"Previous version of {pickle_file_name} deleted.")
        
#         # 파일 열기
#         with open(file_path, 'rb') as f:
#             # 피클로 덤프
#             pickled_data = pickle.dumps(f.read())
            
#             # 피클 파일로 저장
#             with open(pickle_file_name, 'wb') as pickle_file:
#                 pickle_file.write(pickled_data)
#             print(f"{file_name} has been saved as {pickle_file_name}")

# # 파일 목록 및 경로
# file_paths = [
#     '/home/ubuntu/job_crawling/saramin/saramin.py',
#     '/home/ubuntu/job_crawling/incruit/incruit.py',
#     '/home/ubuntu/job_crawling/jumpit/jumpit.py',
#     '/home/ubuntu/job_crawling/wanted/wanted_pch.py',
#     '/home/ubuntu/job_posting/query_base.sql'
# ]
# base_path = '/home/ubuntu/job_crawling/pickle_files'

# # 파일을 피클로 변환하여 저장하는 함수 호출 (덮어쓰기 모드)
# save_files_as_pickle(file_paths, base_path, overwrite=True)

# import os
# import pickle
# import boto3
# from datetime import datetime

# # S3 연결 설정
# s3 = boto3.client('s3')

# # 파일을 피클 형태로 변환 후 로컬에 저장하고 S3에 업로드하는 함수
# def save_and_upload_files_to_s3(file_paths, base_path, bucket_name, s3_base_path):
#     for file_path in file_paths:
#         # 파일명 추출
#         file_name = os.path.basename(file_path)
        
#         # 파일 열기
#         with open(file_path, 'rb') as f:
#             # 피클로 덤프
#             pickled_data = pickle.dumps(f.read())
            
#             # 로컬에 저장 경로 생성
#             pickle_file_name = f"{base_path}/{file_name}.pkl"
            
#             # 피클 파일로 로컬에 저장
#             with open(pickle_file_name, 'wb') as pickle_file:
#                 pickle_file.write(pickled_data)
#             print(f"{file_name} has been saved as {pickle_file_name}")

#             # S3 업로드 경로 생성 (S3 버킷 내 경로)
#             s3_key = f"{s3_base_path}/{os.path.basename(pickle_file_name)}"
            
#             # S3에 업로드
#             with open(pickle_file_name, 'rb') as data:
#                 s3.put_object(Bucket=bucket_name, Key=s3_key, Body=data)
#             print(f"{pickle_file_name} has been uploaded to S3 as {s3_key}")

# # 주기적으로 실행할 파일 목록
# file_paths = [
#     '/home/ubuntu/job_crawling/saramin/saramin.py',
#     '/home/ubuntu/job_crawling/incruit/incruit.py',
#     '/home/ubuntu/job_crawling/jumpit/jumpit.py',
#     '/home/ubuntu/job_crawling/wanted/wanted_pch.py',
#     '/home/ubuntu/job_posting/query_base.sql'
# ]
# base_path = '/home/ubuntu/job_crawling/pickle_files'  # 로컬 저장 경로
# bucket_name = 'job-crawling'
# s3_base_path = 'post_code'  # S3 내 경로

# # 파일을 로컬에 저장하고 S3에 업로드하는 함수 호출
# save_and_upload_files_to_s3(file_paths, base_path, bucket_name, s3_base_path)

