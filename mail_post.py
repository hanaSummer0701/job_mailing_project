import boto3
from botocore.exceptions import ClientError
import mysql.connector
import gspread
import datetime
import json
import os
import logging
import re
from flask import Flask, request, jsonify

import asyncio
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

import datetime
import time
import logging
import copy

# 로그 설정
logging.basicConfig(filename='/home/ubuntu/job_posting/job_posting.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# 현재 날짜 로그
logging.info("Program started")
logging.info(f"Today's date: {datetime.datetime.today().strftime('%Y-%m-%d')}")

# Selenium 웹 드라이버 설정 함수
def setup_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36")
    driver = webdriver.Chrome(options=options)
    return driver

# 채용공고 db
def connect_to_database():
    db_host = 'localhost'
    db_user = 'root'
    db_password = '1231'
    db_name = 'crawling'
    db_port = 3306
    
    try:
        connection = mysql.connector.connect(host=db_host,
                                             user=db_user,
                                             password=db_password,
                                             db=db_name,
                                             port=db_port,
                                             charset='utf8mb4',
                                             collation='utf8mb4_unicode_ci'
                                             )
        logging.info("Database connection successful")
        return connection
    except mysql.connector.Error as err:
        logging.error(f"Database connection error: {err}")
        return None

# 수신자 정보 테이블(lms) 불러오기
def connect_to_lms_database():
    db_host = '*******'  
    db_user = '******'
    db_password = '******!'
    db_name = '*****'
    db_port = ****
    
    try:
        connection = mysql.connector.connect(host=db_host,
                                             user=db_user,
                                             password=db_password,
                                             db=db_name,
                                             port=db_port,
                                             charset='utf8mb4',
                                             collation='utf8mb4_unicode_ci'
                                             )
        logging.info("Database connection successful")
        return connection
    except mysql.connector.Error as err:
        logging.error(f"Database connection error: {err}")
        return None
    
# 구글 시트에서 수신거부 목록 불러오기
json_file_path = "/home/ubuntu/job_posting/GOOGLE_API/genia_email-recommand-6976a7d469c3.json" 
gc = gspread.service_account(json_file_path) 
spreadsheet_url = "https://docs.google.com/spreadsheets/d/1GdC3sv6q-t2v25alAmS83M76eDsrhfZBwTFOrd0Q1jw/edit?resourcekey=&gid=277815760#gid=277815760"
worksheet = gc.open_by_url(spreadsheet_url)
sheet = worksheet.worksheet("UnsubList")
rows = sheet.get_all_values()

# n개월 후 날짜를 계산하는 함수
def add_months(date, months):
    # 정수 부분은 전체 월 수, 소수 부분은 일 수로 계산
    whole_months = int(months)
    additional_days = int((months - whole_months) * 30) # 0.5개월을 15일롤 계산
    
    #월 단위 계산
    month = date.month + whole_months
    year = date.year + (month - 1) // 12
    month = (month -1) % 12 + 1
    # day = min(date.day, (datetime.datetime(year, month + 1, 1) - timedelta(days=1)).day)
    try:
    # 월 단위 계산 결과
        new_date = datetime.datetime(year, month, date.day)
    except ValueError:
        last_day_of_month = (datetime(year, month +1, 1) - timedelta(days=1)).day
        new_date = datetime(year, month, last_day_of_month)
    
    # 추가 일 단위 계산
    final_date = new_date + timedelta(days=additional_days)
    return final_date

def insert_and_update_records(insert_months, update_months):
    connection = connect_to_lms_database()
    cursor = connection.cursor(dictionary=True)
    
    try:
        #오늘 날짜 계산
        today = datetime.datetime.today().date()
        print(f"오늘 날짜: {today}")
      
        cursor.execute("""
                       SELECT *
                       FROM lms_test.course
                       """)
        courses = cursor.fetchall()
        
        for course in courses:
            if course['start_date']:
                # 문자열을 datetime 객체로 변환 아니면 그대로
                if isinstance(course['start_date'], str):
                    start_date = datetime.datetime.strptime(course['start_date'], '%Y-%m-%d').date()
                else:
                    start_date = course['start_date'].date()
                target_date_insert = add_months(start_date, insert_months)
                
                if target_date_insert.date() <= today:
                    no = course['no']
                    print(f"조건에 맞는 코스 발견 (업데이트)! no: {no}, target_date_insert: {target_date_insert}")
                    
                    # member 테이블에서 cno 값이 flag 값이 일치하는 레코드 값 가져오기
                    cursor.execute("""
                                   SELECT *
                                   FROM lms_test.member
                                   WHERE cno = %s
                                   """, (no,))
                    members = cursor.fetchall()
                    print(f"일치하는 멤버 수: {len(members)}")

                    # member 테이블의 일치하는 레코드만 test_member 테이블에 엡데이트
                    for member in members:
                        cursor.execute("""
                                   UPDATE lms_test.member
                                   SET send_email = '1'
                                   WHERE cno = %s AND send_email = '0'
                                   """, (no,))
                    connection.commit() 
                    # print(f"Record updated for member: {member['name']} with cno: {member['cno']}")
                    
                    
            if course['end_date']:
                # 문자열을 datetime  객체로 변환
                end_date = datetime.datetime.strptime(course['end_date'], '%Y-%m-%d').date()
                target_date_update = add_months(end_date, update_months) # 테스트용으로 n일 후
                
                # end_data에서 n개월 후의 날짜가 오늘과 같으면 레코드 변경
                if target_date_update.date() <= today:
                    # flag = course['flag']
                    print(f"조건에 맞는 코스 발견! (업데이트)! cno: {course['no']}, target_date_update: {target_date_update}")
                    
                    # member 테이블에서 cno값이 no 값과 일치하는 레코드 가져오기
                    cursor.execute("""
                                   UPDATE lms_test.member
                                   SET send_email = '0'
                                   WHERE cno = %s AND send_email = '1'
                                   """, (course['no'],))
                    connection.commit() 
                    print(f"send_email updated to 0 for cno: {course['no']}")
                        
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:    
        cursor.close()
        connection.close()

# 실행 4.5개월, 6개월
insert_and_update_records(4.5, 6)

# DB 연결 및 status 조건에 따라 send_email 업데이트
try:
    db_connection = connect_to_lms_database()
    cursor = db_connection.cursor(dictionary=True)

    # status 컬럼이 'EMPLOYED', 'OUT', 'REST'인 사용자 가져오기
    cursor.execute("""
        SELECT name, email, status, send_email
        FROM lms_test.member
        WHERE status IN ('EMPLOYED', 'OUT', 'REST')
    """)
    users = cursor.fetchall()  # 조건에 맞는 모든 데이터 가져오기

    for user in users:
        # send_email 필드가 1일 경우에만 업데이트
        if user['send_email'] == '1':
            update_query = "UPDATE lms_test.member SET send_email = 0 WHERE name = %s AND email = %s"
            cursor.execute(update_query, (user['name'], user['email']))
            db_connection.commit()
            print(f"{user['name']}의 send_email 값이 성공적으로 0으로 업데이트되었습니다.")
        else:
            print(f"{user['name']}의 send_email 값은 이미 0입니다.")

        cursor.execute("""
                    UPDATE lms_test.member
                    SET send_email = '1'
                    WHERE role = 'MANAGER'
                    """)
    db_connection.commit()
    print("role이 'MANAGER'인 사용자들의 send_email값이 1로 설정되었습니다.")

except mysql.connector.Error as e:
    print("DB 오류:", e)

finally:
    cursor.close()
    db_connection.close()

# 각 행에 대해 이름과 과정 정보 추출 및 데이터베이스와 시트 업데이트 - 수신거부
for idx, row in enumerate(rows[1:], start=2):  # 첫 번째 행(헤더) 제외
    if len(row) > 2 and '/' in row[2]:  # 3번째 열이 있고 '/' 포함 시에만 진행
        name, email = row[2].split('/')[0].strip(), row[2].split('/')[1].strip()
        print(f"이름: {name}, 메일: {email}")
        
        # DB에서 이름과 메일이 일치하는지 검사
        try:
            db_connection = connect_to_lms_database()
            cursor = db_connection.cursor(dictionary=True)

            # DB에서 이름과 메일이 일치하는 사용자 가져오기
            cursor.execute("""
                SELECT tm.name, tm.email, tm.send_email, tm.status
                FROM lms_test.member tm
                JOIN lms_test.course tc ON tm.cno = tc.no
                WHERE tm.name = %s AND tm.email = %s
            """, (name, email))
            users = cursor.fetchall()  # 모든 데이터 가져오기
            
            if users:  # 이름과 이메일이 일치하는 사용자가 있을 경우        
                for user in users:
                    # 사용자 send_email 필드 처리
                    if user['send_email'] == '1':
                        # DB에서 send_email 필드 업데이트
                        update_query = "UPDATE lms_test.member SET send_email = 0 WHERE name = %s AND email = %s"
                        cursor.execute(update_query, (user['name'], user['email']))  # 사용자 이메일로 업데이트
                        db_connection.commit()
                        print(f"{user['name']}의 send_email 값이 성공적으로 업데이트되었습니다.")
                    elif user['send_email'] == '0':
                        print(f"{user['name']}의 send_email 값은 이미 0입니다.")
            else:
                print(f"{name} 사용자를 찾을 수 없습니다.")

        except mysql.connector.Error as e:
            print("DB 오류:", e)
        finally:
            cursor.close()
            db_connection.close()

# sql 쿼리 읽기
def get_sql_query_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            sql_query = file.read()
        logging.info(f"Successfully read SQL query from {file_path}")
        return sql_query
    except Exception as e:
        logging.error(f"Error reading SQL query from file: {e}")
        return None

# test2쿼리 조건에 따라 db에서 데이터 가져오기
def get_data_from_database(connection):
    try:
        cursor = connection.cursor()
        sql = get_sql_query_from_file('/home/ubuntu/job_posting/query.sql')
        # logging.info(f"Executing SQL query: {sql}")
        cursor.execute(sql)
        result = cursor.fetchall()
        logging.info(f"Number of rows fetched: {len(result)}")  # 결과 행 개수 확인
        data = ""
        # 서로 다른 테이블에서 데이터 가져오기
        for row in result:
            # logging.info(f"Row data: {row}")  # 각 행 로그 출력
            data += "{}|{}|{}|{}|{}|".format(row[0], row[1], row[2], row[3], row[4].strftime('%Y-%m-%d %H:%M:%S')) # 업체명, 마감날짜, 제목, 공고url, crawling_time
        logging.info("Successfully fetched data from database")
        return data
    except Exception as e:
        logging.error(f"Error fetching data from database: {e}") 
        return ""

# 이메일 전송
def send_email(sender_email, recipient_email, subject, html_body):
    aws_region = '**********'
    aws_access_key_id = '*************'
    aws_secret_access_key = '***********'
    
    # ses_client 생성
    ses_client = boto3.client('ses',
                              region_name = aws_region,
                              aws_access_key_id = aws_access_key_id,
                              aws_secret_access_key = aws_secret_access_key
                              )
    
    # 'chunjae'라는 문자열이 recipient_email에 포함된 경우 ConfigurationSetName을 생략
    config_set = None  # 기본적으로 Configuration Set을 사용하지 않도록 설정

    if 'chunjae' in recipient_email:  # 제목에 'chunjae'가 포함되어 있으면
        config_set = None  # ConfigurationSetName을 None으로 설정
    else:
        config_set = 'observe'  # 그렇지 않으면 'observe'라는 Configuration Set을 사용
    
    try:
        response = ses_client.send_email(
            Destination = {
                'ToAddresses': [
                    recipient_email
                ],
            },
            Message = {
                'Body' : {
                    'Html' : {
                        'Charset' : 'UTF-8',
                        'Data' : html_body
                    },
                },
                'Subject' : {
                    'Charset' : 'UTF-8',
                    'Data' : subject
                },
            },
            # ConfigurationSetName을 동적으로 설정
            **({"ConfigurationSetName": config_set} if config_set else {}),  # config_set이 None이면 제거
            Source = sender_email,
        )
        logging.info(f"Email sent successfully to {recipient_email}")
    except ClientError as e:
        logging.error(f"Error sending email: {e.response['Error']['Message']}")
    else:
        logging.info("Email sent successfully")

# 이메일 발송할 목록 전송하는 함수
def read_html_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()
    return html_content

# 직무 조건을 생성하는 함수
def create_job_conditions(course):
    if '빅데이터' in course or 'BIGDATA' in course:
        return ['데이터 사이언티스트', '데이터 엔지니어', '데이터 분석가', '데이터사이언티스트', '데이터엔지니어', '데이터분석가',
        '데이터 분석','데이터분석', '데이터 정제', '데이터정제', '데이터 처리', '데이터처리', 'ai 기획', 'AI 기획', 'ai기획',
        '데이터 관리', '데이터 분석 매니저', '머신러닝', 'AI 엔지니어', '인공지능 엔지니어', '데이터 시각화', 'tableau', 'Tableau'
        '데이터 마이닝', '비즈니스 인텔리전스', 'ETL 개발', 'SQL', 'R 분석', 'Hadoop', '에듀테크 콘텐츠 개발', '딥러닝/머신러닝',
        'Data Scientist', 'Data Engineer', 'Data Analyst', 'Machine Learning', 'AI Engineer', 'Data Visualization', 
        'Business Intelligence', 'ETL Developer', 'Big Data Engineer', 'Data Management', 'Data Mining', '딥러닝', '자연어 처리',
        'Deep Learning Engineer', 'Natural Language Processing', 'NLP', 'DBA', '빅데이터' 'AI 모델','DB관리', 'db관리',
        '사이언티스트', '인공지능', '데이터분석','데이터처리','데이터관리','데이터마이닝', 'LLM','Data Architect','데이터 리터러시']
    elif '풀스택' in course or 'FULLSTACK' in course:
        return ['프론트엔드', '프론트앤드' '백엔드', '백앤드', '웹 서비스', '모바일 서비스','풀스택', 'Java', '웹 개발자',
        '소프트웨어 엔지니어', '소프트웨어 개발자', '시스템 엔지니어', '시스템 개발자', '모바일 개발자',
        '앱 개발자', 'API 개발자', '클라우드 엔지니어', 'DevOps 엔지니어', 'DevOps 개발자', '서버 개발자',
        '네트워크 엔지니어', '네트워크 개발자', 'Front-End Developer', 'Front-End', 'Back-End'
        'Back-End Developer', 'Full Stack Developer', 'Software Engineer', 'Mobile Developer', 'Cloud Engineer', 
        'API Developer', 'DevOps Engineer', 'Server Developer', 'Web Designer', '웹 퍼블리셔','Vue.js', 
        'Node.js', 'Frontend Engineer', 'Backend Engineer', 'React Developer', 'Node.js Developer', 'Vue.js Developer', 'CI/CD']
    elif 'PM' in course.upper():
        return['웹 기획', '앱 기획', '서비스 기획', '콘텐츠 기획', '전략 기획', '프로덕트 매니저']
        # return ['웹 서비스', '서비스 기획', '서비스기획', 'PM', '프로덕트 매니저', '프로덕트매니저', '서비스 기획자', '프로젝트 매니저', '프로젝트매니저', '서비스 운영',
        # '서비스 디자이너', '서비스 기획 매니저', '서비스 기획 PM', '서비스 기획 PL', '서비스 기획 담당자', '콘텐츠 기획', '교육 콘텐츠 기획']
    else:
        return []

# 불용어 생성하는 함수
def create_stopwords(course):
    if '빅데이터' in course or 'BIGDATA' in course:
        return ['석사', '박사', '코드잇', '교육생', '국비', '청년수당', '제약', '건설', '연구', '기계', '행정', '호텔', '천재교육', '천재교과서', 'SeSAC', 'APM', 'apm', 'edi', 'EDI', '마케터', '양성']
    elif '풀스택' in course or 'FULLSTACK' in course:
        return ['석사', '박사', '코드잇', '교육생', '국비', '청년수당', '제약', '건설', '연구', '기계', '행정', '호텔', '천재교육', '천재교과서', 'SeSAC', 'APM', 'apm', 'edi', 'EDI', 'UX', 'UI', '마케터','양성']
    elif 'PM' in course.upper():
        return ['창업', '번역', '석사', '박사', '코드잇', '교육생', '국비', '청년수당', '제약', '건설', '연구', '기계', '행정', '호텔', '천재교육', '천재교과서', 'SeSAC', 'APM', 'apm', 'edi', 'EDI', '개발자', '시공','유튜브','양성']
    else:
        return []

def get_unsub_data(connection):
    try:
        cursor = connection.cursor(dictionary=True)
        logging.info("Cursor initialized.")
        sql = """
        SELECT tm.name, tm.email, tm.role, tm.course_nos, tm.cno, tm.send_email
        FROM lms_test.member tm
        WHERE tm.send_email <> '0' AND tm.send_email <> '2'
        """
        cursor.execute(sql)
        members = cursor.fetchall() #모든 행 가져오기
        logging.info(f"Members fetched: {members}")
        
        unsubscribed_data =[]
        
        for member in members:
            processed_subject = set() #이미 처리한 subject를 추적
            logging.info(f"Processing member: {member}")
            if member['role'] == 'MANAGER' and member['course_nos']:
                # course_nos 처리
                course_ids = [int(course_id.strip()) for course_id in member['course_nos'].split(',') if course_id.strip().isdigit()]
                if not course_ids:
                    logging.info(f'No course IDs for memeber: {member['name']}')
                    continue
                
                #course_ids를 사용하여 매칭되는 코스 정보를 가져옴
                sql_course="""
                SELECT no,subject
                FROM lms_test.course
                WHERE no IN (%s)
                """ % ','.join(['%s'] * len(course_ids)) # IN 절 동적으로 구성
                cursor.execute(sql_course, course_ids)
                courses = cursor.fetchall()
                logging.info(f'Courses fetched for member:{courses}')
                
                for course in courses:
                    if course['subject'] not in processed_subject:
                        unsubscribed_data.append({
                            'name' : member['name'],
                            'email' : member['email'],
                            'subject' : course['subject'],
                            'send_email' : member['send_email']
                        })
                        processed_subject.add(course['subject']) # 중복 방지 
            elif member['cno']:
                # MANAGER가 아닌 경우 기존 cno를 사용하여 코스 정보를 가져옴
                sql_course="""
                SELECT no,subject
                FROM lms_test.course
                WHERE no=%s
                """
                cursor.execute(sql_course, (member['cno'],))
                course = cursor.fetchone()
                logging.info(f"Single course fetched:{course}")
                if course:
                    unsubscribed_data.append({
                        'name': member['name'],
                        'email': member['email'],
                        'subject': course['subject'],
                        'send_email': member['send_email']
                    })
                    processed_subject.add(course['subject']) # 멤버의 처리된 subject 추가
        logging.info(f"Unsubscribed Data: {unsubscribed_data}")
        return unsubscribed_data
    
    except mysql.connector.Error as err:
        logging.error(f"Error fetching data: {err}")
        return None

async def fetch_job_details(job_key, source, job_url, title, stop_words, to_delete):
    driver = setup_driver()
    try:
        driver.get(job_url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*')))
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        if "incruit" in job_url:
            text = driver.find_element(By.XPATH, '//*[@id="headInfoRight"]/div[2]/button').text
        elif "jumpit" in job_url:
            # text = soup.find('div', class_='sc-190507ae-1 bAkpEB').text
            text = soup.find('div', class_='sc-407d4306-1 mirSB').text
        elif "wanted" in job_url:
            text = soup.find('div', class_='WantedApplyBtn_container__fjWVh').text
        elif "saramin" in job_url:
            text = driver.find_element(By.XPATH, '//*[@id="content"]/div[3]/section[1]/div[1]/div[1]/div/div[2]/button/span').text

        time.sleep(1)
        logging.info(f'Text from {source} - {title}: {text}')

        # 텍스트에 특정 단어가 포함되어 있는지 확인
        if any(word in text for word in stop_words):
            # 삭제할 항목 저장
            to_delete.append(job_key)  # job_key를 to_delete에 추가

    except Exception as e:
        logging.error(f"Error with {job_url}: {e}")
        to_delete.append(job_key)  # 오류 발생 시 삭제 항목에 추가
    finally:
        driver.quit()

async def process_source(source, postings, stop_words):
    to_delete = []  # 이 리스트는 삭제할 작업 키를 저장합니다.
    tasks = []

    for company, duedate, title, job_url, crawling_time in postings:
        company_cleaned = re.sub(r"\(주\)|주식회사\s*|\s*주식회사\s*", "", company).strip()
        job_key = (company_cleaned, title)

        task = asyncio.create_task(fetch_job_details(job_key, source, job_url, title, stop_words, to_delete))
        tasks.append(task)

        # 4개 작업을 진행한 후 대기
        if len(tasks) == 4:
            await asyncio.gather(*tasks)
            tasks = []  # 작업 리스트 초기화

    # 남아있는 작업 실행
    if tasks:
        await asyncio.gather(*tasks)

    return to_delete  # 삭제할 항목 목록을 반환

async def main(grouped_data, stop_words, unique_jobs):
    all_to_delete = []
    tasks = []

    # 각 source에 대해 비동기 태스크로 실행
    for source, postings in grouped_data.items():
        print(f"Processing source: {source}")
        initial_count = len(postings)  # 초기 데이터 개수 기록
        
        limited_postings = postings[:50]# 포스팅을 최대 10개로 제한
        task = asyncio.create_task(process_source(source, limited_postings, stop_words))
        tasks.append((source, initial_count, task))  # 소스와 초기 개수 함께 저장

    # 모든 태스크가 완료될 때까지 기다림
    results = await asyncio.gather(*[task for _, _, task in tasks])

    # 각 source의 to_delete 리스트를 all_to_delete에 추가
    for index in range(len(tasks)):
        source, initial_count, _ = tasks[index]  # 각 source와 초기 개수 가져오기
        to_delete = results[index]  # 결과에서 to_delete를 가져옵니다.
        
        # to_delete에 course 정보를 추가하여 최종 삭제할 리스트 생성
        for job_key in to_delete:
            company, title = job_key
            # 각 course별로 to_delete를 추가합니다.
            for course in unique_jobs.keys():
                if job_key in unique_jobs[course]:
                    all_to_delete.append((course, company, title))

        final_count = initial_count - len(to_delete)  # 삭제 후 남은 데이터 개수
        print(f"Source: {source} - Initial count: {initial_count}, Deleted: {len(to_delete)}, Final count: {final_count}")

    return all_to_delete  # 최종 삭제할 리스트 반환

course_list = ['BIGDATA', 'FULLSTACK', 'PM']

def fetch_and_deduplicate_data(course_list):
    unique_jobs = {course: {} for course in course_list}
    
    for course in course_list:
        print(f"Fetching data for course: {course}")
        job_conditions = create_job_conditions(course)
        stopwords_list = create_stopwords(course)
        with open('/home/ubuntu/job_posting/query_base.sql', 'r', encoding='utf-8') as file:
            query = file.read()
            job_conditions_sql_1 = ' OR '.join([f'(saramin_data.job_title LIKE "%{job}%")' for job in job_conditions])
            job_conditions_sql_2 = ' OR '.join([f'(incruit_data.job_title LIKE "%{job}%")' for job in job_conditions])
            job_conditions_sql_3 = ' OR '.join([f'(new_wanted_data.job_title LIKE "%{job}%")' for job in job_conditions])
            job_conditions_sql_4 = ' OR '.join([f'(jumpit_data.job_title LIKE "%{job}%")' for job in job_conditions])
            # 불용어 조건 추가 - 공고명
            stopword_sql_1 = 'AND'.join([f'(saramin_data.job_title NOT LIKE "%{word}%")' for word in stopwords_list])
            stopword_sql_2 = 'AND'.join([f'(incruit_data.job_title NOT LIKE "%{word}%")' for word in stopwords_list])
            stopword_sql_3 = 'AND'.join([f'(new_wanted_data.job_title NOT LIKE "%{word}%")' for word in stopwords_list])
            stopword_sql_4 = 'AND'.join([f'(jumpit_data.job_title NOT LIKE "%{word}%")' for word in stopwords_list])

            # 불용어 조건 추가 - 회사명
            stopword_sql_5 = 'AND'.join([f'(saramin_data.company_name NOT LIKE "%{word}%")' for word in stopwords_list])
            stopword_sql_6 = 'AND'.join([f'(incruit_data.company NOT LIKE "%{word}%")' for word in stopwords_list])
            stopword_sql_7 = 'AND'.join([f'(new_wanted_data.company_name NOT LIKE "%{word}%")' for word in stopwords_list])
            stopword_sql_8 = 'AND'.join([f'(jumpit_data.company_name NOT LIKE "%{word}%")' for word in stopwords_list])

        # 쿼리 생성 및 저장   
        query = query.format(
                condition1=f"({job_conditions_sql_1}) AND ({stopword_sql_1}) AND ({stopword_sql_5})",
                condition2=f"({job_conditions_sql_2}) AND ({stopword_sql_2}) AND ({stopword_sql_6})",
                condition3=f"({job_conditions_sql_3}) AND ({stopword_sql_3}) AND ({stopword_sql_7})",
                condition4=f"({job_conditions_sql_4}) AND ({stopword_sql_4}) AND ({stopword_sql_8})"
            )

        with open('/home/ubuntu/job_posting/query.sql', 'w') as output_file:
            output_file.write(query) 

        db_connection = connect_to_database() 
        db_to_html = get_data_from_database(db_connection) 
        logging.info("========================================")
        logging.info(f"Course '{course}' data cached. Number of records: {len(db_to_html)}")
        data = db_to_html.split('|')[:-1]
        # logging.info(data)
        grouped_data = {
            '사람인' : [],
            '인크루트' : [],
            '원티드' : [],
            '점핏' : []
        }
        # 각 소스별로 데이터를 분리
        for i in range(0, len(data), 5):
            company, duedate, title, job_url, crawling_time = data[i:i+5]
            crawling_time = datetime.datetime.strptime(crawling_time, '%Y-%m-%d %H:%M:%S')

            if 'jumpit' in job_url:
                grouped_data['점핏'].append((company, duedate, title, job_url, crawling_time))
            elif 'saramin' in job_url:
                grouped_data['사람인'].append((company, duedate, title, job_url, crawling_time))
            elif 'incruit' in job_url:
                grouped_data['인크루트'].append((company, duedate, title, job_url, crawling_time))
            elif 'wanted' in job_url:
                grouped_data['원티드'].append((company, duedate, title, job_url, crawling_time))
            # crawling_time 기준으로 정렬
            for source in grouped_data.keys():
                grouped_data[source].sort(key=lambda x: x[4], reverse=True)  # x[4] = crawling_time
                # logging.info(f"{source} 데이터: {grouped_data[source]}")

        # 소스 내 중복을 먼저 제거한 후, 소스 간 중복도 처리하여 최종 데이터를 저장
        for source, postings in grouped_data.items():
            
            for company, duedate, title, job_url, crawling_time in postings:
                company_cleaned = re.sub(r"\(주\)|주식회사\s*|\s*주식회사\s*", "", company).strip()
                job_key = (company_cleaned, title)
                # 소스 간 중복 확인: company_cleaned 기준으로 중복 처리
                if job_key not in unique_jobs[course]:  # 코스별로 확인
                    unique_jobs[course][job_key] = (source, company, duedate, title, job_url, crawling_time)
         
        # logging.info(f'unique_jobs : {unique_jobs}')  
        # 각 URL에 대해 텍스트 추출
        stop_words = ['마감된 공고', '마감된 포지션', '지원마감', '접수마감']

        start_time = datetime.datetime.now()
        # 비동기 실행
        logging.info(f'start time : {start_time}')

        # main 함수 호출로 grouped_data와 stop_words, unique_jobs 처리
        to_delete = asyncio.run(main(grouped_data, stop_words, unique_jobs))
        
        # to_delete에 있는 항목을 course별로 unique_jobs에서 제거
        for course, company, title in to_delete:
            job_key = (company, title)  # (company, title) 형식의 job_key 생성

            # 해당 course에서 job_key가 존재하는지 확인 후 삭제
            if job_key in unique_jobs.get(course, {}):
                del unique_jobs[course][job_key]  # 삭제

        logging.info(f"Items to delete: {to_delete}")

        end_time = datetime.datetime.now()
        finish = end_time - start_time
        logging.info(f'finish time : {finish}')
        # logging.info(unique_jobs)
        logging.info('========================================')

    return unique_jobs, grouped_data

def select_posting(unique_jobs, course):
    fin_course_list = {
        'BIGDATA': {},
        'FULLSTACK': {},
        'PM': {}
    }

    # BIGDATA 및 FULLSTACK 조건 처리
    if '빅데이터' in course or 'BIGDATA' in course:
        total_bigdata_count = 0
        # 공고를 사이트별로 저장할 임시 구조 생성
        bigdata_site_jobs = {}
        for (company, job_title), (job_source, company, duedate, title, job_url, crawling_time) in unique_jobs[course].items():
            if job_source not in bigdata_site_jobs:
                bigdata_site_jobs[job_source] = []
            bigdata_site_jobs[job_source].append((company, duedate, title, job_url, crawling_time))
            # print(f"bigdata_site_jobs: {len(bigdata_site_jobs)}")

        # 분배용 리스트 초기화
        for job_source in bigdata_site_jobs:
            if job_source not in fin_course_list['BIGDATA']:
                fin_course_list['BIGDATA'][job_source] = []

        # 모든 사이트에서 최대 5개씩 공고를 추가
        max_jobs = 5
        while total_bigdata_count < 20:
            any_added = False  # 각 라운드에서 공고를 추가했는지 확인
            for job_source, jobs in bigdata_site_jobs.items():
                if total_bigdata_count >= 20:
                    break
                if jobs and len(fin_course_list['BIGDATA'][job_source]) < max_jobs:  # 사이트별 최대 수 제한
                    fin_course_list['BIGDATA'][job_source].append(jobs.pop(0))  # 한 개씩 추가
                    total_bigdata_count += 1
                    any_added = True
                    print(f"[추가됨] 사이트: {job_source}, 총 공고 수: {total_bigdata_count}")  # 디버깅 출력
            if not any_added:
                print("더 이상 추가할 공고가 없습니다.")  # 디버깅 출력
                break
        # 공고가 20개가 안되면 '사람인'에서 추가로 공고를 채움
        if total_bigdata_count < 20 and '사람인' in bigdata_site_jobs:
            while total_bigdata_count < 20 and bigdata_site_jobs['사람인']:
                fin_course_list['BIGDATA']['사람인'].append(bigdata_site_jobs['사람인'].pop(0))
                total_bigdata_count += 1
                print(f"[사람인 추가됨] 총 공고 수: {total_bigdata_count}")  # 디버깅 출력

    if '풀스택' in course or 'FULLSTACK' in course:
        total_fullstack_count = 0
        # 공고를 사이트별로 저장할 임시 구조 생성
        full_site_jobs = {}
        for (company, job_title), (job_source, company, duedate, title, job_url, crawling_time) in unique_jobs[course].items():
            if job_source not in full_site_jobs:
                full_site_jobs[job_source] = []
            full_site_jobs[job_source].append((company, duedate, title, job_url, crawling_time))
            # print(f"full_site_jobs: {len(full_site_jobs)}")

        # 분배용 리스트 초기화
        for job_source in full_site_jobs:
            if job_source not in fin_course_list['FULLSTACK']:
                fin_course_list['FULLSTACK'][job_source] = []
        
        # 모든 사이트에서 최대 5개씩 공고를 추가
        max_jobs = 5  
        while total_fullstack_count < 20:
            any_added = False  # 각 라운드에서 공고를 추가했는지 확인
            for job_source, jobs in full_site_jobs.items():
                if total_fullstack_count >= 20:
                    break
                if jobs and len(fin_course_list['FULLSTACK'][job_source]) < max_jobs:  # 사이트별 최대 수 제한
                    fin_course_list['FULLSTACK'][job_source].append(jobs.pop(0))  # 한 개씩 추가
                    total_fullstack_count += 1
                    any_added = True
                    print(f"[추가됨] 사이트: {job_source}, 총 공고 수: {total_fullstack_count}")  # 디버깅 출력
            if not any_added:
                print("더 이상 추가할 공고가 없습니다.")  # 디버깅 출력
                break
        # 공고가 20개가 안되면 '사람인'에서 추가로 공고를 채움
        if total_fullstack_count < 20 and '사람인' in full_site_jobs:
            while total_fullstack_count < 20 and full_site_jobs['사람인']:
                fin_course_list['FULLSTACK']['사람인'].append(full_site_jobs['사람인'].pop(0))
                total_fullstack_count += 1
                print(f"[사람인 추가됨] 총 공고 수: {total_fullstack_count}")  # 디버깅 출력

    # PM 조건 처리
    if 'PM' in course.upper():
        total_pm_count = 0
        # 공고를 사이트별로 저장할 임시 구조 생성
        pm_site_jobs = {}
        for (company, job_title), (job_source, company, duedate, title, job_url, crawling_time) in unique_jobs[course].items():
            if job_source not in pm_site_jobs:
                pm_site_jobs[job_source] = []
            pm_site_jobs[job_source].append((company, duedate, title, job_url, crawling_time))
            # print(f"pm_site_jobs: {len(pm_site_jobs)}")
        
        # 분배용 리스트 초기화
        for job_source in pm_site_jobs:
            if job_source not in fin_course_list['PM']:
                fin_course_list['PM'][job_source] = []
        
        # 모든 사이트에서 최대 5개씩 공고를 추가
        max_jobs = 5
        while total_pm_count < 20:
            any_added = False  # 각 라운드에서 공고를 추가했는지 확인
            for job_source, jobs in pm_site_jobs.items():
                if total_pm_count >= 20:
                    break
                if jobs and len(fin_course_list['PM'][job_source]) < 5:  # 사이트별 최대 5개 제한
                    fin_course_list['PM'][job_source].append(jobs.pop(0))  # 한 개씩 추가
                    total_pm_count += 1
                    any_added = True
                    print(f"[추가됨] 사이트: {job_source}, 총 공고 수: {total_pm_count}")  # 디버깅 출력
            if not any_added:
                print("더 이상 추가할 공고가 없습니다.")  # 디버깅 출력
                break
        # 공고가 20개가 안되면 '사람인'에서 추가로 공고를 채움
        if total_pm_count < 20 and '사람인' in pm_site_jobs:
            while total_pm_count < 20 and pm_site_jobs['사람인']:
                fin_course_list['PM']['사람인'].append(pm_site_jobs['사람인'].pop(0))
                total_pm_count += 1
                print(f"[사람인 추가됨] 총 공고 수: {total_pm_count}")  # 디버깅 출력
    # logging.info(f"최종 반환 값: {fin_course_list}")
    return fin_course_list

# def load_json_jobs(post_json_path):
#     """json파일의 리스트 불러오기"""
#     json_jobs = {}
#     if os.path.exists(post_json_path):
#         with open(post_json_path, 'r', encoding='utf-8') as json_file:
#             json_data = json.load(json_file)
#             # logging.info(f'Loaded JSON data: {json_data}')
#         # 각 키에 접근하여 데이터 수집
#         categories = ['BIGDATA', 'FULLSTACK', 'PM']
#         for category in categories:
#             category_data = json_data[0]['data'].get(category, {})
#             # logging.info(f'{category} Data: {category_data}')

#         for category in categories:
#             json_jobs[category] = json_data[0]['data'].get(category, {})
#         # logging.info(f'Combined JSON jobs data: {json_jobs}')
#     return json_jobs

def load_json_jobs(post_json_path):
    """json파일의 리스트 불러오기"""
    json_jobs = {}
    if os.path.exists(post_json_path):
        with open(post_json_path, 'r', encoding='utf-8') as json_file:
            json_data = json.load(json_file)
            # logging.info(f'Loaded JSON data: {json_data}')
        # 각 키에 접근하여 데이터 수집
        categories = ['BIGDATA', 'FULLSTACK', 'PM']

        # 모든 데이터를 합치는 딕셔너리 초기화
        combined_data = {category: {} for category in categories}
        # JSON의 모든 timestamp 데이터를 순회
        for entry in json_data:
            for category in categories:
                category_data = entry['data'].get(category, {})
                for source, jobs in category_data.items():
                    if source not in combined_data[category]:
                        combined_data[category][source] = []
                    
                    # 중복되지 않는 데이터만 추가
                    for job in jobs:
                        if job not in combined_data[category][source]:
                            combined_data[category][source].append(job)
        
        json_jobs = combined_data
        # logging.info(f'Combined JSON jobs data: {json_jobs}')
    return json_jobs

def compare_unique_jobs_with_json(json_jobs, unique_jobs):
    """json과 중복된 공고 확인 및 제거"""
    filtered_jobs = {}

    for category, jobs in unique_jobs.items():
        filtered_jobs[category] = {}
        # logging.info(f"filtered_jobs:{filtered_jobs}")
        for job_key, job_data in jobs.items():
            # `unique_jobs` 데이터 형식 (source, company, duedate, title, job_url, crawling_time)
            source, company, duedate, title, job_url, _ = job_data
            
            # JSON 데이터에 해당 공고가 있는지 확인
            if category in json_jobs:
                json_category_data = json_jobs[category]

                # JSON 데이터에서 (company, title) 쌍을 사용해 중복 확인
                json_job_tuples = {}
                for company_name, company_jobs in json_category_data.items():
                    for data in company_jobs:
                        # 데이터가 리스트 형태인지 확인하고 길이를 체크하여 안전하게 접근
                        if isinstance(data, list) and len(data) > 2:
                            json_job_tuples[(data[0], data[2])] = data
                
                # 현재 공고의 키를 (company, title) 튜플로 생성
                job_tuple = (company, title)
                
                # JSON 데이터와 중복되는 경우 제거 및 로그
                if job_tuple in json_job_tuples:
                    logging.info(f"중복된 공고 제거됨: category={category}, source={source}, company={company}, title={title}, duedate={duedate}, job_url={job_url}")
                else:
                    # 중복되지 않는 공고만 filtered_jobs에 추가
                    filtered_jobs[category][job_key] = job_data
            else:
                # 해당 category가 json_jobs에 없으면 중복으로 간주하지 않음
                filtered_jobs[category][job_key] = job_data

    logging.info(f"중복되지 않은 unique_jobs 수: {sum(len(jobs) for jobs in filtered_jobs.values())}")
    return filtered_jobs

def send_emails(unique_jobs, unique_data_list, post_json_path):
    html_header = read_html_file('/home/ubuntu/job_posting/header.html')
    html_tr = read_html_file('/home/ubuntu/job_posting/tr.html')

    # JSON 파일로부터 중복 공고 불러오기
    json_jobs = load_json_jobs(post_json_path)
    # 중복 공고를 제거하고 unique_jobs 업데이트
    unique_jobs = compare_unique_jobs_with_json(json_jobs, unique_jobs)
    # logging.info(f'unique_jobs{unique_jobs}')
    # 모든 과정에 대한 최종 리스트 생성
    fin_course_list = {}
    # 각 과정에 대해 select_posting 호출하여 결과를 직접 fin_course_list에 할당
    for course in ['BIGDATA', 'FULLSTACK', 'PM']:
        fin_course_list[course] = select_posting(unique_jobs, course)[course]

    # datetime 객체를 문자열로 변환
    for course, postings in fin_course_list.items():
        for source, job_list in postings.items():
            for i, job in enumerate(job_list):
                company, duedate, title, job_url, crawling_time = job
                job_list[i] = (company, duedate, title, job_url, crawling_time.strftime("%Y-%m-%d %H:%M:%S"))

    # logging.info(f'fin_course_list: {fin_course_list}')

    # JSON 파일이 이미 존재하는지 확인
    if os.path.exists(post_json_path):
        # 기존 데이터를 읽고 새로운 데이터를 추가
        with open(post_json_path, 'r', encoding='utf-8') as json_file:
            try:
                # 기존 데이터를 로드
                existing_data = json.load(json_file)
                if not isinstance(existing_data, list):
                    existing_data = []
            except json.JSONDecodeError:
                existing_data = []
                # 파일이 비어 있는 경우 초기화
    else:
        # 파일이 존재하지 않으면 빈 리스트 초기화
        existing_data = []
    
    # 현재 timestamp 추가
    new_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "data": fin_course_list
    }
    # 새 데이터를 기존 리스트에 추가
    existing_data.append(new_entry)

    # 업데이트된 전체 데이터를 파일에 다시 저장
    with open(post_json_path, 'w', encoding='utf-8') as json_file:
        json.dump(existing_data, json_file, ensure_ascii=False, indent=2)

    # logging.info(f"최종 선택 리스트가 JSON 파일에 저장되었습니다: {fin_course_list}")
    logging.info(f"=========================최종 선택 리스트가 JSON 파일에 저장되었습니다=========================")

    for data in unique_data_list:
        name = data['name']
        email = data['email']
        course = data['subject']
        logging.info(f"Sending email to: {name}, Email: {email}, Course: {course}")
        tr = ""     
        total_job_postings = 0

        # course에 대한 정보가 fin_course_list에 있는지 확인하고 추가
        if course in fin_course_list:  # 특정 과정이 fin_course_list에 있는지 확인
            postings = fin_course_list[course]  # 해당 과정의 모든 포스팅 가져오기
            for source, posting_list in postings.items():  # 각 소스별 포스팅 가져오기
                if posting_list:  # postings가 있는 경우만 추가
                    # 소스 이름을 추가 (밑줄 추가)
                    tr += f"<h3 style='font-family: Malgun Gothic, sans-serif; font-size: 25px; color: #000000; text-align: center; background-color: #e3eaff; solid;'>{source}</h3>"
                    tr += "<table width='100%' border='0' cellspacing='0' cellpadding='0'>"

                    for job in posting_list:
                        company, duedate, title, job_url, crawling_time = job
                        job_html = html_tr.format(job_url, company, duedate, title)
                        tr += job_html
                        total_job_postings += 1
                        # logging.info(f"Added {course} job: {title} from {company}")

                    tr += "</table>"
        logging.info(f"Total job postings added for {name}: {total_job_postings}")

        today = datetime.datetime.today().strftime("%Y-%m-%d")
                
        html_feed_back = read_html_file('/home/ubuntu/job_posting/feedback.html')
        #공고 수가 20개 미만일 경우 경고 문구 추가
        if total_job_postings < 20:
            tr +="""
            <div style='font-family: Malgun Gothic, sans-serif; font-size:12px; color: #DF3535; text-align: center; margin-top: 20px;'>
            최신 공고 제공으로 채용공고가 20개 미만일 수 있습니다.
            </div>
            """
            
        # 피드백 링크 추가
        if html_feed_back: 
            tr +=f"""
            <div style='margin-top: 20px;'>
            {html_feed_back}
            </div>
            """
        # 수신 거부 추가
        tr +="""
        <div style='font-family: Malgun Gothic, sans-serif; color: #9D9D9D; text-decoration: underline; font-size: 12px; text-align: center;'>
        <a href="https://forms.gle/C785dS1va78w6PMH7">수신 거부</a>
        </div>
        """
        html_res = html_header.format(today, name, total_job_postings, tr)

        sender_email = 'chunjaecloud@gmail.com' 
        recipient_email = email
        subject = '📌 [천재IT교육센터] 이번주 나에게 맞는 채용공고는?' 
        feedback_ = '📌 [천재IT교육센터] 채용공고 피드백 요청' 

        send_email(sender_email, recipient_email, subject, html_res) 

# 가져온 데이터를 unique_data_list로 저장
db_connection = connect_to_lms_database()
unique_data_list = get_unsub_data(db_connection)

# 메인 처리 로직
post_json_path = '/home/ubuntu/job_posting/send_posting_log.json'
unique_jobs, grouped_data = fetch_and_deduplicate_data(course_list)
send_emails(unique_jobs, unique_data_list, post_json_path)
