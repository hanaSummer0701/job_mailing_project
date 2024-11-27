import re
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import pymysql
import mysql.connector
import time
from datetime import datetime
import logging
from selenium.common.exceptions import TimeoutException

# 기본 로거 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 파일 핸들러 설정
file_handler = logging.FileHandler('/home/ubuntu/job_crawling/incruit/log/app.log', encoding='utf-8')  # UTF-8 인코딩 설정
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# 핸들러 추가
logger.addHandler(file_handler)

# 데이터베이스 연결
def connect_to_db():
    return pymysql.connect(
        host='@@@@',  # 또는 Docker 컨테이너의 IP 주소
        user='@@@',
        password='@@@',
        database='@@@@',
        charset='utf8mb4',
        port=@@@@,  # MySQL 기본 포트
        collation="utf8mb4_general_ci"
)

# 검색 결과에서 총 건수를 추출하고 페이지 수 계산
def get_total_pages(soup):
    total = soup.find('button', {'id': 'SearchResultCount'}).text
    result_count = int(re.sub(r'[^\d]', '', total))
    return (result_count // 60) + (1 if result_count % 60 != 0 else 0)  # 페이지 수 계산

# 마감기한 날짜 형식 통일
def parse_deadline(deadline_text):
    # 오늘 날짜의 연도와 월/일 추출
    current_year = datetime.now().strftime("%Y")  # 현재 년도 (예: 2024)
    today = datetime.now().strftime("%Y-%m-%d")  # 오늘 날짜 yyyy-mm-dd 형식

    # "채용시"가 포함된 경우 "채용시" 반환
    if "채용시" in deadline_text:
        return "채용시"
    
    # "상시"가 포함된 경우 "상시" 반환
    if "상시" in deadline_text:
        return "상시"

    # 1. 날짜 추출 (~10.07 (월) 형식에서 날짜 추출)
    date_pattern = re.compile(r'~(\d{1,2}\.\d{1,2})')  # 예: ~10.07
    match = date_pattern.search(deadline_text)
    
    if match:
        # 추출한 날짜 형식을 yyyy-mm-dd로 변환
        extracted_date = match.group(1)
        month, day = extracted_date.split('.')
        return f"{current_year}-{int(month):02d}-{int(day):02d}"

    # 2. 시간만 있는 경우 (11시 마감 형식에서 오늘 날짜로 변환)
    time_pattern = re.compile(r'(\d{1,2})시\s*마감')  # 예: 11시 마감
    match = time_pattern.search(deadline_text)
    
    if match:
        return today

    # 변환되지 않은 경우 None 반환
    return None

    
def collect_job_urls_and_details(base_url, iframe_id, cursor, connection, headless=True):
    # Selenium 드라이버 설정
    options = Options()
    
    if headless:
        options.add_argument('--headless')  # 브라우저 창을 띄우지 않음
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument('--disable-gpu')
    
    # 웹드라이버 실행
    driver = webdriver.Chrome(options=options)

    page = 1

    while True:
        # 페이지 처리 시작 시간 측정
        page_start_time = time.time()
        logging.info(f'{page} page 시작....')
        
        # 구인 공고 목록 페이지로 이동
        driver.get(f'{base_url}&page={page}')
        
        # 페이지 로딩 대기
        time.sleep(2)
        
        # BeautifulSoup으로 페이지 내용 파싱
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        
        # 구인 공고 URL 수집
        job_links = soup.select('a[href*="jobdb_info/jobpost.asp"]')
        
        if not job_links:
            break  # 더 이상 공고가 없으면 종료
            
        urls = [] 
        
        stop_crawling = False  # 수집 종료 여부를 제어하는 변수
        
        # 각 공고의 URL과 기타 정보 수집
        for link in job_links:
            job_url = link['href'] if link['href'].startswith('http') else f'https://job.incruit.com{link["href"]}'
            
            # Category 추출
            parent_div = link.find_parent('div', class_='cell_mid')
            category_div = parent_div.find('div', class_='cl_btm') if parent_div else None
            category = ', '.join([span.text.strip() for span in category_div.find_all('span')]) if category_div else None

            # 마감 기한 추출
            deadline_parent_div = parent_div.find_next_sibling('div', class_='cell_last') if parent_div else None
            deadline_div = deadline_parent_div.find('div', class_='cl_btm') if deadline_parent_div else None
            raw_deadline = deadline_div.text.strip() if deadline_div else None

            # 추출한 텍스트에서 정규식 사용하여 마감 기한 변환
            deadline = parse_deadline(raw_deadline) if raw_deadline else None
            
            # 등록일자가 None이 아닐 경우에만 처리
            if raw_deadline:
                # "시간" 또는 "분" 단위의 경우 1일을 의미하는 1로 지정
                if '시간' in raw_deadline or '분' in raw_deadline or '수정' in raw_deadline:
                    days_ago = 1

                # 등록일자에서 숫자만 추출 (예: "3일 전"에서 3을 추출)
                match = re.search(r'(\d+)\s*일', raw_deadline)
                days_ago = int(match.group(1)) if match else 0


                # 등록일자가 3일 초과면 저장하지 않고 전체 반복 중단
                if days_ago > 10:
                    logging.info(f"등록일자 {days_ago}일 전 공고 발견. 현재 페이지 수집 후 종료")
                    stop_crawling = True  # 이후의 수집은 멈추게 설정
                    break  # 현재 페이지 내 반복 종료, 더 이상 공고 수집 안함
            
            # URL과 카테고리 저장
            urls.append({'url': job_url, 'category': category, 'deadline': deadline})
        logging.info(f'수집된 URL 총 개수: {len(urls)}')
        # 각 URL에 대해 세부 정보 수집
        all_job_details = []
        for job_data in urls:
            if job_data is None:
                print("Error: job_data가 None입니다.")
                continue  # 다음 반복으로 넘어감

            job_details = collect_job_details(job_data['url'], iframe_id)
            
            # job_details가 None인지 확인
            if job_details is None:
                logging.error(f"Error: {job_data['url']}의 세부 정보를 수집할 수 없습니다.")
                continue  # 다음 반복으로 넘어감

            job_details['category'] = job_data['category']
            job_details['deadline'] = job_data['deadline']
            all_job_details.append(job_details)
        
        # DataFrame으로 변환 및 DB 저장
        if all_job_details:
            df = convert_to_dataframe(all_job_details)
            save_to_db(df, cursor, connection)
        
        # 페이지 처리 완료 시간 계산
        page_end_time = time.time()
        elapsed_time = page_end_time - page_start_time
        logging.info(f"{page} 페이지 완료 (처리 시간: {elapsed_time:.2f} 초)")
        
        # 다음 페이지로 이동
        page += 1
        if stop_crawling:
            break  # 총 페이지 수를 넘거나 수집 종료 플래그가 True면 종료

    # 브라우저 종료
    driver.quit()

# 공고 상세 정보 및 iframe의 숨겨진 텍스트 수집
def collect_job_details(job_url, iframe_id, headless=True):
    # Selenium 드라이버 설정
    options = Options()
    
    if headless:
        options.add_argument("--headless")  # 헤드리스 모드 추가
        options.add_argument("--no-sandbox")
        options.add_argument('--no-cache')
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-popup-blocking")  # 팝업 차단 옵션
        
    
    # 웹드라이버 실행
    driver = webdriver.Chrome(options=options)
    try:
        driver.get(job_url)
    except TimeoutException:
        logging.error(f"페이지 로딩 타임아웃: {job_url}")
        return None  # None을 반환하거나 다음 URL로 넘어가도록 처리    

    try:
        
        try:
            close_button = WebDriverWait(driver, 2).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'dPosLayer-close'))
            )
            logging.info("팝업 창 닫음")
            close_button.click()
        except TimeoutException:
            # 팝업이 없는 경우 진행
            logging.info("팝업이 없어 바로 상세 정보를 수집합니다.")
            
        WebDriverWait(driver, 4).until(EC.presence_of_element_located((By.CLASS_NAME, 'jc_list')))

        # BeautifulSoup으로 페이지 내용 파싱
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        
        # 채용 공고 상세 정보 추출
        job_title = soup.find('h1').text.strip()
    
        # 회사 이름 추출 추가
        company = soup.find('div', class_='top-cnt').find('a').text.strip() if soup.find('div', class_='top-cnt') else None
        
        # 구인 정보 저장할 딕셔너리
        job_info = {}

        # ul 요소 찾기
        job_type = soup.find('ul', class_='jc_list')

        # 리스트 항목 추출
        if job_type:
            items = job_type.find_all('li')
            for item in items:
                title = item.find('div', class_='tt').find('em').text.strip() if item.find('div', class_='tt') else None
                
                # class가 'txt'인 div에서 값 추출
                value_div = item.find('div', class_='txt')
                if value_div:
                    value = value_div.text.strip()  # 항목 값
                else:
                    # class가 'bb'인 요소에서 값 추출
                    value = item.find('em', class_='bb').text.strip() if item.find('em', class_='bb') else None

                if title and value:  # 제목과 값이 존재할 때만 딕셔너리에 추가
                    job_info[title] = value  # 딕셔너리에 추가
    except Exception as e:
        logging.error(f"공고 상세 정보를 수집하는 도중 오류 발생. 해당 공고를 스킵합니다.")
        driver.quit()  # 드라이버 종료
        return None  # 오류가 발생하면 None을 반환

    # 구인 정보 저장할 딕셔너리
    com_info = {}

    # 회사 정보를 추출하는 함수
    def extract_company_info(com_type_class):
        com_type = soup.find('ul', class_=com_type_class)
        if com_type:
            items = com_type.find_all('li')
            for item in items:
                title = item.find('div', class_='tt').find('em').text.strip() if item.find('div', class_='tt') else None
                
                # class가 'txt'인 div에서 값 추출
                value_div = item.find('div', class_='txt')
                value = value_div.find('em').text.strip() if value_div and value_div.find('em') else None
                
                # 값이 없을 경우 a 태그의 텍스트 추출
                if value is None:
                    value = value_div.find('a').text.strip() if value_div and value_div.find('a') else None

                if title and value:  # 제목과 값이 존재할 때만 딕셔너리에 추가
                    com_info[title] = value  # 딕셔너리에 추가

    # 회사 정보 추출
    extract_company_info('jcinfo_list')  # 첫 번째 회사 정보
    extract_company_info('jcinfo_list last')  # 두 번째 회사 정보

    # iframe에서 숨겨진 텍스트 추출
    description_text = get_hidden_text_from_iframe(driver, iframe_id)

    # 브라우저 종료
    driver.quit()
    
    return {
        'title': job_title,
        'company': company,
        'job_info': job_info,
        'com_info': com_info,
        'description_text': description_text,  # 상세 텍스트 추가
        'url': job_url
    }

# iframe에서 숨겨진 텍스트 추출 함수
def get_hidden_text_from_iframe(driver, iframe_id):
    try:
        WebDriverWait(driver, 3).until(EC.frame_to_be_available_and_switch_to_it((By.ID, iframe_id)))
    except Exception as e:
        logging.info(f"iframe을 찾지 못했습니다")
        # driver.quit()  # 드라이버 종료
        return None
    # 페이지 로딩 대기
    time.sleep(2)
    
    # BeautifulSoup으로 iframe 내부 페이지 내용 파싱
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    
    # style="display:none"인 div 태그에서 텍스트 추출
    hidden_div = soup.find('div', id="content_job")
    hidden_text = hidden_div.get_text(strip=True) if hidden_div else None
    
    # 원래 프레임으로 돌아가기
    driver.switch_to.default_content()
    
    return hidden_text

# DataFrame으로 변환하는 함수
def convert_to_dataframe(all_job_details):
    df = pd.DataFrame(all_job_details)
    job_info_df = df['job_info'].apply(pd.Series)
    com_info_df = df['com_info'].apply(pd.Series)
    df = pd.concat([df.drop(['job_info', 'com_info'], axis=1), job_info_df, com_info_df], axis=1)
    
    #df['description_text'] = df['description_text'].str.replace('\n', '', regex=False)
    df['category'] = df['category'].str.replace(r',,+', ',', regex=True).str.strip(',')
    df.drop(columns=['설립일'], inplace=True)
    
    return df

# 데이터베이스에 데이터 저장
def save_to_db(df, cursor, connection):
    column_mapping = {
    'title': 'job_title',
    'company': 'company',
    'description_text': 'description_text',
    'url': 'recruit_url',
    'category': 'keywords',
    '고용형태': 'job_type',
    '경력': 'experience',
    '근무지역': 'location',
    '학력': 'education',
    '급여조건': 'salary',
    '기업규모': 'company_size',
    '업종': 'industry',
    '주소': 'address',
    '홈페이지': 'company_url',
    '복리후생': 'benefits',
    'deadline' : 'deadline'
    }
    df.rename(columns=column_mapping, inplace=True)
    if '우대사항' in df.columns:
        df.drop(columns=['우대사항'], inplace=True)

    df = df.fillna('-')
    
    cursor = connection.cursor()
    columns = df.columns.tolist()
    
    # UPDATE 구문 생성
    update_query = ', '.join([f'{col} = VALUES({col})' for col in columns])
    
    insert_query = f"""
        INSERT INTO incruit ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(columns))}) \
            ON DUPLICATE KEY UPDATE {update_query}
    """
    
    for _, row in df.iterrows():
        cursor.execute(insert_query, tuple(row))
    connection.commit()

# 전체 URL에서 정보 수집
def main():
    connection = connect_to_db()
    cursor = connection.cursor()
    base_url = 'https://job.incruit.com/jobdb_list/searchjob.asp?crr=99&crr=1&rgn2=18&rgn2=14&rgn2=11&sortfield=reg&sortorder=1'
    iframe_id = 'ifrmJobCont'
    collect_job_urls_and_details(base_url, iframe_id, cursor, connection)

    cursor.close()
    connection.close()

if __name__ == '__main__':
    main()
