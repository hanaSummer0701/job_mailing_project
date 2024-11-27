import logging
from bs4 import BeautifulSoup
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
import pymysql
from pymysql.cursors import DictCursor

# 로그 설정 함수
def setup_logging():
    # 로그 파일 이름 설정 - 절대경로로 설정해야 로그 나옴.
    log_filename = '/home/ubuntu/job_crawling/saramin/crawling_log.log'
    # 로그 파일 핸들러 설정
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')  # UTF-8 인코딩 설정 - 안될 경우 encoding='utf-8-sig'
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    # 루트 로거 설정
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # 핸들러 추가
    logger.addHandler(file_handler)
    return logger

# 로그 설정
logger = setup_logging()
# 현재 시간 출력 
now = datetime.now()
# 현재 시간을 출력하기
logger.info(f'▶️▶️▶️▶️▶️▶️▶️▶️▶️▶️ 시작 시간 :, {now} ◀️◀️◀️◀️◀️◀️◀️◀️◀️◀️')

# 직업군 매핑 딕셔너리 (번호에 따라 직업 카테고리 지정)
job_mapping = {
    2: 'IT·개발·데이터',
    3: '회계·세무·재무',
    4: '총무·법무·사무',
    5: '인사·노무·HRD',
    6: '의료',
    7: '운전·운송·배송',
    8: '영업·판매·무역',
    9: '연구·R&D',
    10: '서비스',
    11: '생산',
    12: '상품기획·MD',
    13: '미디어·문화·스포츠',
    14: '마케팅·홍보·조사',
    15: '디자인',
    16: '기획·전략',
    17: '금융·보험',
    18: '구매·자재·물류',
    19: '교육',
    20: '공공·복지',
    21: '고객상담·TM',
    22: '건설·건축'
}

# MariaDB 연결 함수
def connect_to_db():
    connection = pymysql.connect(
        host='@@@@@',  # 또는 Docker 컨테이너의 IP 주소
        user='@@@',
        password='@@@@',
        database='@@@@',
        charset='utf8mb4',
        port='@@@', 
        collation="utf8mb4_general_ci",
        cursorclass=pymysql.cursors.DictCursor
    )
    return connection

# Selenium 웹 드라이버 설정 함수
def setup_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36")
    driver = webdriver.Chrome(options=options, service=Service(ChromeDriverManager().install()))
    return driver

# 페이지에서 공고 수 계산 함수
def get_job_count(driver):
    jobs = driver.find_element(By.XPATH, '//*[@id="content"]/div[4]/div/div[1]/span/em').text
    num_jobs = int(jobs.replace(',', ''))  # 쉼표 제거 후 정수 변환
    return num_jobs

# 공고 상세 정보 크롤링 함수
def crawl_job_details(driver, job_url):
    start_time = time.time()  # 시작 시간 기록

    # 현재 창 핸들 저장 (원래 창)
    main_window = driver.current_window_handle
    # 새로운 탭에서 공고 페이지 열기
    WebDriverWait(driver, 3).until(
        lambda d: d.execute_script('return document.readyState') == 'complete')
    driver.execute_script(f"window.open('{job_url}', '_blank');")
    time.sleep(1)

    # 새로 열린 탭으로 전환
    WebDriverWait(driver, 15).until(lambda d: len(d.window_handles) > 1)
    time.sleep(2)
    WebDriverWait(driver, 15).until(lambda d: d.execute_script('return document.readyState') == 'complete')
    driver.switch_to.window(driver.window_handles[1])  # 새 탭으로 전환
    time.sleep(1)  # 페이지 로딩을 기다리기 위해 추가
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')

    # [공고 사전 확인 서비스] 제외
    current_url = driver.current_url  # 현재 페이지의 URL 가져오기
    if current_url == "https://www.saramin.co.kr/zf_user/recruit/inspection-view" or current_url == "https://www.saramin.co.kr/zf_user/recruit/not-exist-view":
        logger.info("공고 사전 확인 서비스 OR 찾을 수 없는 공고로 크롤링하지 않음.")
        driver.close()  # 탭 닫기
        time.sleep(1)
        driver.switch_to.window(main_window)  # 원래 창으로 돌아가기
        return None  # None을 반환하여 데이터 추출을 건너뜀

    # 'noti_jumpit' 클래스가 있는지 확인 - 점핏 공고 제외
    if driver.find_elements(By.CLASS_NAME, 'meta') and "에서 제공된 공고입니다." in driver.find_elements(By.CLASS_NAME, 'meta')[0].text:
        logger.info("점핏 공고로 크롤링하지 않음.")
        driver.close()  # 탭 닫기
        time.sleep(1)
        driver.switch_to.window(main_window)  # 원래 창으로 돌아가기
        return None  # None을 반환하여 데이터 추출을 건너뜀

    # 공고 데이터 추출
    time.sleep(1)
    title = driver.find_elements(By.CLASS_NAME, 'tit_job')[0].text
    company = driver.find_elements(By.CLASS_NAME, 'company')[0].text

    # 웹 페이지에서 텍스트 추출 - 경력, 학력, 근무형태
    text1 = driver.find_elements(By.CLASS_NAME, 'col')[0].text.split('\n')

    # 각 항목을 저장할 변수 초기화
    lv = None
    edu = None
    job_type_info = None
    # '경력', '학력', '근무형태' 찾고, 그 다음 값을 추출
    for i in range(len(text1)):
        # '경력' 추출
        if text1[i] == '경력':
            try:
                lv = text1[i + 1].strip()
            except IndexError:
                lv = None
        
        # '학력' 추출
        elif text1[i] == '학력':
            try:
                edu = text1[i + 1].strip()
            except IndexError:
                edu = None
        
        # '근무형태' 추출
        elif text1[i] == '근무형태':
            try:
                job_type_info = text1[i + 1].strip()
            except IndexError:
                job_type_info = None

    comp_url = soup.select('div.info_area dd a')[0]['href'] if soup.select('div.info_area dd a') else None
    post_url = driver.current_url

    # 근무지역과 급여 추출 - 근무지역, 급여
    text2 = driver.find_elements(By.CLASS_NAME, 'col')[1].text.split('\n')
    # 각 항목을 저장할 변수 초기화
    work_place = None  # 근무지역
    salary = None  # 급여

    # '근무지역', '급여' 찾고, 그 다음 값을 추출
    for i in range(len(text2)):
        # '근무지역' 추출
        if text2[i] == '근무지역':  # 공백 제거
            try:
                work_place = text2[i+1].strip()
                if work_place.endswith('지도'):
                    work_place = work_place[:-len('지도')].strip()
            except:
                work_place = None
        # '급여' 추출
        elif text2[i] == '급여':
            try:
                salary = text2[i + 1].strip()
            except:
                salary = None

    # 마감일
    try:
        deadline = driver.find_elements(By.CLASS_NAME, 'info_period')[0].text.split('\n')[3]
        if deadline == "채용시":
            deadline = "채용시"  # 채용시는 그대로 반환
        else:
            try:
                # 날짜 형식 변환: 2024.10.16 23:59 -> 2024-10-16
                deadline = datetime.strptime(deadline, '%Y.%m.%d %H:%M').strftime('%Y-%m-%d')
            except ValueError:
                pass  # 변환할 수 없는 경우 원본 반환
    except:
        deadline = "채용시 마감" 

    # 공고 내용 추출 (텍스트 또는 OCR로 이미지 처리) － 새탭 내에서 페이지 이동
    iframe = soup.find('iframe', id='iframe_content_0')
    if iframe:
        src_value = 'https://www.saramin.co.kr' + iframe['src']
        driver.get(src_value)
        # 페이지가 모두 로드될 때까지 대기
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )
        time.sleep(1)  # 페이지 로딩을 기다리기 위해 추가
        # 페이지 로딩 완료 후 텍스트 및 OCR 추출 - 현재 ocr은 None으로 채워짐
        result = text, ocr_text = extract_text(driver)
        text, ocr_text = result if result is not None else (None, None)  # unpacking 시 None 처리
    else:
        text = None
        ocr_text = None
        
    # 탭 닫기 및 목록 페이지로 돌아가기
    time.sleep(2)
    driver.close()
    driver.switch_to.window(main_window)
    WebDriverWait(driver, 3).until(
        lambda d: d.execute_script('return document.readyState') == 'complete'
    )
    elapsed_time = time.time() - start_time  # 소요 시간 계산
    logger.info(f"공고 상세 정보 크롤링 완료 - 소요 시간: {elapsed_time:.2f}초")
    return title, company, lv, edu, job_type_info, salary, work_place, text, ocr_text, post_url, comp_url, deadline

# 텍스트 공고 내용을 추출하는 함수
def extract_text(driver):
    time.sleep(2)
    text = None
    ocr_text = None
    try:
        text_elements = driver.find_elements(By.XPATH, '/html/body/div/div')
        if text_elements:
            text = text_elements[0].text.strip()
        # /html/body/div/div에서 텍스트가 없는 경우 혹은 너무 짧은 경우 /html/body/div에서 텍스트 추출 시도
        if not text or len(text.strip().splitlines()) == 1:
            text_elements = driver.find_elements(By.XPATH, '/html/body/div')
            if text_elements:
                text = text_elements[0].text.strip()
        # 텍스트가 여전히 없으면 None으로 설정
        if not text:
            text = None

    except Exception as e:
        logger.info(f"OCR 처리 중 오류 발생: {e}")
    return text, ocr_text

# 데이터베이스에 공고 데이터 저장 함수
def save_to_db(cursor, job_cat, title, company, lv, edu, job_type_info, salary, work_place, text, keywords, ocr_text, post_url, comp_url, deadline):
    now = datetime.now()  # 현재 시간 가져오기
    query = """
    INSERT IGNORE INTO saramin (job_cat, job_title, company_name, experience, education, job_type, salary, location, description_text, keywords, description_img, recruit_url, company_url, deadline, crawling_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (job_cat, title, company, lv, edu, job_type_info, salary, work_place, text, keywords, ocr_text, post_url, comp_url, deadline, now)
    cursor.execute(query, values)

# 페이지를 스크롤하여 모든 공고 로드하는 함수
def scroll_page(driver):
    # JavaScript를 사용하여 페이지를 맨 아래로 스크롤
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    WebDriverWait(driver, 3).until(
        lambda d: d.execute_script('return document.readyState') == 'complete'
    )  # 페이지 로딩 대기

    # 페이지가 맨 아래로 스크롤된 후 다시 위로 스크롤
    driver.execute_script("window.scrollTo(0, 0);")
    WebDriverWait(driver, 3).until(
        lambda d: d.execute_script('return document.readyState') == 'complete'
    )  # 페이지 로딩 대기

# 중복 공고명 확인 함수
def is_duplicate_posturl(cursor, title):
    try:
        query = "SELECT COUNT(*) as count FROM crawling.saramin WHERE job_title = %s"
        cursor.execute(query, (title,))
        result = cursor.fetchone()
        # 쿼리 실행 후 result가 None인지 로그로 확인
        if result is None or result['count'] == 0:  # result가 튜플이면 인덱스 0으로 접근
            return False  # 중복이 아니라고 처리        
        return result['count'] > 0   # 중복된 경우 True 반환
    except Exception as e:
        logger.error(f"중복 URL 확인 중 오류 발생: {e}", exc_info=True)
        return False  # 에러 발생 시 중복이 아니라고 처리

# 주요 크롤링 로직 함수
def crawl_jobs(mapping, connection, driver):
    cursor = connection.cursor()
    current_page = 1
    total_jobs_crawled = 0  # 총 크롤링된 공고 수를 저장할 변수 추가
    error_count = 0  # 연속 오류 발생 횟수 저장 변수 추가
    max_errors = 3  # 최대 허용 오류 횟수

    # 페이지 URL 설정
    url = f'https://www.saramin.co.kr/zf_user/jobs/list/domestic?page={current_page}&loc_mcd=101000%2C102000%2C108000&exp_cd=1&exp_none=y&cat_mcls={mapping}&search_optional_item=y&search_done=y&panel_count=y&preview=y&isAjaxRequest=0&page_count=100&sort=RD&type=domestic&is_param=1&isSearchResultEmpty=1&isSectionHome=0&searchParamCount=4#searchTitle'
    driver.get(url)

    # 총 공고 수 / 페이지 수 계산
    # num_jobs = get_job_count(driver)
    num_jobs = 400
    # max_page = (num_jobs + 99) // 100
    max_page = 4
    while True:
        try:
            if current_page==max_page:
                # 마지막 페이지일 경우
                # num_jobs_postings = num_jobs % 100
                # if num_jobs_postings == 0:
                #     num_jobs_postings = 100
                ## 현재는 마지막까지 크롤링이 아닌 일정 페이지까지만 크롤링 중으로 다음과 같이 100개 지정
                num_jobs_postings = 100
            elif current_page<max_page:
                # 마지막 페이지가 아닌 경우
                num_jobs_postings = 100
            # 페이지 스크롤하여 모든 공고 로드
            scroll_page(driver)

            # 현재 페이지에서 공고 순차적으로 크롤링
            for p in range(0, num_jobs_postings):
                try:
                    # # 페이지가 완전히 로드될 때까지 기다림
                    time.sleep(2)
                    # 공고 페이지로 이동 위한 url 추출
                    post_url = driver.find_elements(By.CLASS_NAME, 'col.notification_info')[p].find_elements(By.CLASS_NAME, 'str_tit')[0].get_attribute('href')
                    keywords = None
                    # 텍스트 추출
                    keywords = driver.find_elements(By.CLASS_NAME, 'job_sector')[p].text.replace('\n', '|')

                    # '외'가 마지막에 있을 경우 제거
                    if keywords.endswith('|외'):
                        keywords = keywords[:-2]  # 마지막 두 글자 '|외' 제거

                    time.sleep(2)
                    job_details = crawl_job_details(driver, post_url)
                    if job_details is None:
                        continue  # None인 경우 다음 공고로 넘어감

                    # db저장 순서
                    title, company, lv, edu, job_type_info, salary, work_place, text, ocr_text, post_url, comp_url, deadline = job_details
                    time.sleep(1)
                    WebDriverWait(driver, 3).until(
                        lambda d: d.execute_script('return document.readyState') == 'complete'
                    )
                    # DB에서 제목 중복 확인url
                    if is_duplicate_posturl(cursor, title):
                        logger.info(f'중복된 공고입니다: {title}')
                        continue  # 중복된 경우 다음 공고로 넘어감
                    # 공고를 CSV에 저장하고 중복이 아니면 공고 수 카운트 및 로그 기록
                    save_to_db(cursor, job_mapping.get(mapping, 'Unknown'), title, company, lv, edu, job_type_info, salary, work_place, text, keywords, ocr_text, post_url, comp_url, deadline)
                    connection.commit()
                    WebDriverWait(driver, 3).until(
                        lambda d: d.execute_script('return document.readyState') == 'complete'
                    )
                    total_jobs_crawled += 1  # 총 크롤링된 공고 수를 누적
                    logger.info(f"크롤링된 공고 수: {current_page}page - {p+1}/{num_jobs} - {title} - 총 크롤링 공고 수: {total_jobs_crawled}")

                except NoSuchElementException:
                    logger.info("예외 공고: 추천광고")
                    continue

            # 다음 페이지로 이동
            if current_page < max_page:
                current_page += 1
                url = f'https://www.saramin.co.kr/zf_user/jobs/list/domestic?page={current_page}&loc_mcd=101000%2C102000%2C108000&exp_cd=1&exp_none=y&cat_mcls={mapping}&search_optional_item=y&search_done=y&panel_count=y&preview=y&isAjaxRequest=0&page_count=100&sort=RD&type=domestic&is_param=1&isSearchResultEmpty=1&isSectionHome=0&searchParamCount=4#searchTitle'
                time.sleep(2)
                driver.get(url)
                WebDriverWait(driver, 3).until(
                    lambda d: d.execute_script('return document.readyState') == 'complete'
                )
            else:
                break
        except Exception as e:
            logger.error(f"크롤링 중 오류 발생: {e}", exc_info=True)
            error_count += 1  # 오류 발생 시 카운트 증가
            if error_count >= max_errors:
                logger.error(f"최대 허용 오류 {max_errors}회를 초과하여 크롤링 중단")
                driver.quit()  # 오류 횟수 초과 시 크롤링 중단
                break

# 메인 함수
def main():
    connection = connect_to_db()  # MariaDB 연결
    driver = setup_driver()  # Selenium 웹 드라이버 설정
    # 크롤링하고자 하는 mapping 숫자를 리스트로 정의
    desired_mappings = [2, 16]  # 원하는 숫자를 리스트
    # 직업군 카테고리 mapping에 따라 크롤링
    for mapping in desired_mappings:
        logger.info(f"크롤링 시작 - {job_mapping[mapping]}")
        crawl_jobs(mapping, connection, driver)
        WebDriverWait(driver, 3).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )
    # 데이터베이스 연결 종료
    connection.close()
    driver.quit()

# 프로그램 실행
if __name__ == "__main__":
    main()
