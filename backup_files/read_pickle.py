# ## py파일 버전
# import pickle

# # 피클 파일에서 Python 파일 내용 불러오기
# with open('/home/ubuntu/job_crawling/pickle_files/incruit.py_20241025_092353.pkl', 'rb') as pickle_file:
#     loaded_script_content = pickle.load(pickle_file)

# # 바이트 문자열을 문자열로 변환
# script_string = loaded_script_content.decode('utf-8')

# # 실행하기 - 실제 해당 파일 실행됨
# exec(script_string)

import pickle

# 피클 파일에서 Python 파일 내용 불러오기
with open('/home/ubuntu/job_crawling/pickle_files/query_base.sql_20241025_111223.pkl', 'rb') as pickle_file:
    loaded_script_content = pickle.load(pickle_file)

# 바이트 문자열을 문자열로 변환
script_string = loaded_script_content.decode('utf-8')

# 콘솔에 출력
print(script_string)

