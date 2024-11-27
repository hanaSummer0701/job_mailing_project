from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
import requests
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import numpy as np
import re
import pymysql
import datetime
from tqdm import tqdm
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging

# 기본 로거 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 파일 핸들러 설정
file_handler = logging.FileHandler('/home/ubuntu/job_crawling/jumpit/log/app.log', encoding='utf-8')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# 핸들러 추가
logger.addHandler(file_handler)


# MySQL 연결 설정
conn = pymysql.connect(
    host='@@@@',
    user='@@@',
    password='@@@',
    database='@@@@',
    charset='utf8mb4',
    port=@@@,
    collation="utf8mb4_general_ci"
)
cursor = conn.cursor()

# 로그인 정보 
login = {
    "id": "chunjaecloud@gmail.com",
    "pw": "cloudchunjae@123"
}

# 페이지에서 공고 URL 수집하는 함수
def collect_urls():
    url = 'https://www.jumpit.co.kr/positions?sort=rsp_rate'
    options = Options()
    options.add_argument('--headless')
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument('--disable-gpu')
    options.add_argument('--no-cache')
    
    # 웹드라이버 실행
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    logging.info('url get!')
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    urls = ['https://jumpit.saramin.co.kr' + link['href'] for link in soup.select('div[class*="sc-d609d44f-0"] a')]
    driver.quit()

    return urls

# 공고 페이지에서 데이터 수집하는 함수
def collect_data(recruit_url):
    try:
        req = requests.get(recruit_url)
        soup = BeautifulSoup(req.content, 'html.parser')

        title = soup.find('h1').text.strip() 

        dt_tags = soup.find_all('dt')
        dd_tags = soup.find_all('dd')
        data_dict = {}
        for dt, dd in zip(dt_tags, dd_tags):
            data_dict[dt.text.strip()] = dd.text.strip()    
        keywords = data_dict.get('주요업무', None)
        qualifications = data_dict.get('자격요건', None)
        preferred_qualifications = data_dict.get('우대사항', None)
        career = data_dict.get('경력', None)
        education = data_dict.get('학력', None)
        deadline = data_dict.get('마감일', None)

        # 웹드라이버 실행
        options = Options()
        options.add_argument('--headless')
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument('--disable-gpu')
        
        driver = webdriver.Chrome(options=options)
        driver.get(recruit_url)

        # 업체 URL과 이름 수집
        company_urls = []
        company_names = []

        # 현재 공고 URL을 회사 URL로 사용
        driver.get(recruit_url)

        # 회사 이름 추출
        try:
            company_name = driver.find_element(By.CLASS_NAME, "name").text 
            company_names.append(company_name)
            company_tag = soup.select_one('a.name')
            company_url = 'https://www.jumpit.co.kr' + company_tag['href']
            company_urls.append(company_url)
        except Exception as e:
            print(f"회사 정보 추출 오류: {e}")
            company_names.append(None)
            company_urls.append(None)

        # 기술 스택 수집
        tech = driver.find_elements(By.CLASS_NAME, 'sc-d9de2de1-0.hMgXwE')
        tech_stack = str([element.text for element in tech]) if tech else None

        # 회사 추가 정보 수집
        # driver.find_element(By.XPATH, "/html/body").click()
        # time.sleep(2)
        # driver.find_element(By.CLASS_NAME, "sc-9715e912-6.jfXAyx").click()
        # time.sleep(21)
        # driver.find_element(By.NAME, 'email').send_keys(login.get("id"))
        # driver.find_element(By.XPATH, '/html/body/main/div/form/div/div[1]/button').click()

        # time.sleep(1)
        # driver.find_element(By.NAME, 'password').send_keys(login.get("pw"))
        # driver.find_element(By.XPATH, '/html/body/main/div/form/div/div/button').click()
        # time.sleep(2)

        driver.get(company_urls[0])
        
        time.sleep(2)  # 페이지 로딩 대기
        ex2 = driver.find_element(By.XPATH, "/html/body/main/div/div").text
        experience = re.search(r'업력\n(.+?)\n', ex2).group(1) if re.search(r'업력\n(.+?)\n', ex2) else None
        address = re.search(r'대표주소\n(.+?)\n', ex2).group(1) if re.search(r'대표주소\n(.+?)\n', ex2) else None
        company_url = re.search(r'(www\.\S+|https?://\S+)', ex2).group(1) if re.search(r'(www\.\S+|https?://\S+)', ex2) else None
        revenue = re.search(r'매출액\n(.*?)원', ex2).group() if re.search(r'매출액\n(.*?)원', ex2) else None
        if revenue is not None:
            revenue = revenue[4:]
        operating_profit = re.search(r'영업이익\n(.*?)원', ex2).group() if re.search(r'영업이익\n(.*?)원', ex2) else None
        if operating_profit is not None:
            operating_profit = operating_profit[5:]        
        employee_count = re.search(r'직원수\n(.*?)명', ex2).group() if re.search(r'직원수\n(.*?)명', ex2) else None
        if employee_count is not None:
            employee_count = employee_count[4:]        
        salary = re.search(r'전체\n(.*?)원', ex2).group() if re.search(r'전체\n(.*?)원', ex2) else None
        if salary is not None:
            salary = salary[3:]

        driver.quit()
        now = datetime.datetime.now()
        formatted_now = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        return formatted_now, company_names[0], title, keywords, qualifications, preferred_qualifications, \
               tech_stack, career, education, salary, deadline, revenue, operating_profit, employee_count, experience, address, \
               company_urls[0], recruit_url
    
    except Exception as e:
        print(f"에러 발생: {e}")
        return None

# 데이터베이스에 데이터 삽입하는 함수
def insert_data_to_db(data_dict):
    insert_query = """
    INSERT INTO jumpit (date, company_name, job_title, keywords, qualifications, preferred_qualifications, \
                          tech_stack, career, education, salary, deadline, revenue, operating_profit, \
                          employee_count, history, address, company_url, recruit_url)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
    ON DUPLICATE KEY UPDATE \
    date = VALUES(date), \
    company_name = VALUES(company_name), \
    job_title = VALUES(job_title), \
    keywords = VALUES(keywords), \
    qualifications = VALUES(qualifications), \
    preferred_qualifications = VALUES(preferred_qualifications), \
    tech_stack = VALUES(tech_stack), \
    career = VALUES(career), \
    education = VALUES(education), \
    salary = VALUES(salary), \
    deadline = VALUES(deadline), \
    revenue = VALUES(revenue), \
    operating_profit = VALUES(operating_profit), \
    employee_count = VALUES(employee_count), \
    history = VALUES(history), \
    address = VALUES(address), \
    company_url = VALUES(company_url), \
    recruit_url = VALUES(recruit_url) \
    """
    # data_dict를 튜플로 변환
    data_tuple = (
        data_dict['date'], data_dict['company_name'], data_dict['job_title'], data_dict['keywords'],
        data_dict['qualifications'], data_dict['preferred_qualifications'], data_dict['tech_stack'],
        data_dict['career'], data_dict['education'], data_dict['salary'], data_dict['deadline'],
        data_dict['revenue'], data_dict['operating_profit'], data_dict['employee_count'], data_dict['history'],
        data_dict['address'], data_dict['company_url'], data_dict['recruit_url']
    )
    cursor.execute(insert_query, data_tuple)
    conn.commit()

# 메인 함수 
def main():
    job_urls = collect_urls()
    for recruit_url in tqdm(job_urls):
        data = collect_data(recruit_url)
        if data:
            data_dict = {
                'date': data[0],
                'company_name': data[1],
                'job_title': data[2],
                'keywords': data[3],
                'qualifications': data[4],
                'preferred_qualifications': data[5],
                'tech_stack': data[6],
                'career': data[7],
                'education': data[8],
                'salary': data[9],
                'deadline': data[10],
                'revenue': data[11],
                'operating_profit': data[12],
                'employee_count': data[13],
                'history': data[14],
                'address': data[15],
                'company_url': data[16],
                'recruit_url': data[17]
            }
            insert_data_to_db(data_dict)

if __name__ == "__main__":
    main()

