import pandas as pd
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
from tqdm import tqdm
from sqlalchemy import create_engine
import mysql.connector
import re
import datetime
from bs4 import BeautifulSoup
import pymysql
from pymysql.cursors import DictCursor
import requests
from fake_useragent import UserAgent

ua = UserAgent()

# 현재 시간 출력 
now = datetime.datetime.now()
# 현재 시간을 출력하기
print("▶️▶️▶️▶️▶️▶️▶️▶️▶️▶️ 시작 시간 :", now, "◀️◀️◀️◀️◀️◀️◀️◀️◀️◀️")

# 브라우저 꺼짐 방지 옵션
chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument("--disable-gpu")  # GPU 사용 안 함 (headless 모드에서 필요)
chrome_options.add_argument('--start-fullscreen') # 브라우저 창 크기 설정
chrome_options.add_argument("--no-sandbox")  # 리눅스 환경에서 필요
# chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36")
chrome_options.add_argument(f"user-agent={ua.random}")
# print(f'사용된 User-Agnet:{ua.random}')

# 드라이버 생성
service = Service(executable_path='/usr/bin/chromedriver')
# driver = webdriver.Chrome(service=service, options=chrome_options)
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
driver.get('https://www.wanted.co.kr/wdlist/all?country=kr&job_sort=job.latest_order&years=0&locations=all')
print("driver get !")


########################
# Pg Dn으로 전체 공고 찾기  
########################
driver.implicitly_wait(15) # 모든 드라이버 옵션에 적용 요소를 찾을 때까지 15초간 대기
actions = driver.find_element(By.CSS_SELECTOR, 'body') # home, end 키 설정을 위함

while True:
    
    old_position = driver.execute_script('return window.pageYOffset;')
    # 스크롤 다운
    actions.send_keys(Keys.END)
    # 잠시 대기하여 페이지가 스크롤될 수 있도록 함
    time.sleep(10)
    
    # 스크롤 이후 페이지 내용이 변경되지 않으면 스크롤을 멈춤
    driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
    time.sleep(10)
    
    # 새로운 스크롤 위치를 가져옴
    new_position = driver.execute_script('return window.pageYOffset;')

    # print('1', old_position, new_position)

    # 마지막 페이지인지 확인 
    if new_position == old_position:
        # 진짜 마지막 페이지인지 확인하기 위해 스크롤을 조금 위로 올려서 확인
        driver.execute_script('window.scrollTo(0, window.pageYOffset - 5000);')
        time.sleep(10)  
        actions.send_keys(Keys.END)        
        time.sleep(10)  
        actions.send_keys(Keys.END)
        time.sleep(10)  
        # 새로운 스크롤 위치를 다시 확인
        new_position = driver.execute_script('return window.pageYOffset;')
        # 만약에도 스크롤이 여전히 움직이지 않으면 반복문 종료
        if new_position == old_position:
            break

    # print('2', old_position, new_position)
    
xx = '//*[@id="__next"]/div[3]/div[2]/ul/li'
# WebDriverWait(driver, 100).until(EC.presence_of_element_located((By.XPATH, xx)))
print(f"총 페이지 공고 개수 : {len(driver.find_elements(By.XPATH, xx))}")
    
max_crawl_count =450    
crawl_url_list = []
total_len = len(driver.find_elements(By.XPATH, '//*[@id="__next"]/div[3]/div[2]/ul/li'))

print('<< crawl_url_list append start >>')
for i in range(1, total_len + 1):
    if len(crawl_url_list) >= max_crawl_count:
        break
    job_link = driver.find_element(By.XPATH, f'//*[@id="__next"]/div[3]/div[2]/ul/li[{i}]/div/a').get_attribute('href')
    crawl_url_list.append(job_link)
    
wanted_df = pd.DataFrame(columns= ['업체명', '주소', '산업분류', '연혁', '직급', '모집분야', '자격요건', '우대사항',
                                   '지원마감일자', '연매출', '직원수', '연봉', '채용정보url'])

name_list = []       # 업체명
loca_list = []       # 주소
industry_list = []   # 산업분류
year_list = []       # 연혁 
experience_list = [] # 직급
job_list = []        # 모집분야 
must_list = []       # 자격요건 
prefer_list = []     # 우대사항 
end_date_list = []   # 지원마감일자 
money_list = []      # 연매출 
member_list = []     # 직원수 
price_list = []      # 연봉
url_list = []        # 채용정보url 

# MariaDB 연결 설정 # mysql 버전 정보가 안맞아서 수정함 charset, collation 추가.
# 신버전 (hostname 서버에서 사용)
connection = pymysql.connect(
        host='@@@@',  # 또는 Docker 컨테이너의 IP 주소
        user='@@@@',
        password='@@@@',
        database='@@@@',
        charset='utf8mb4',
        port=@@@@,  
        collation="utf8mb4_general_ci",
        cursorclass=pymysql.cursors.DictCursor
    )
cursor = connection.cursor()
print('<< Start DB Load >>')
for i in tqdm(crawl_url_list):
    '''
    url_list 링크 하나씩 타고 가서 크롤링 시작
    '''

    if i == '':
        continue
    # 하나씩 웹 띄워서 크롤링 
    chrome_options = Options()
    chrome_options.add_argument("--disable-dev-shm-usage")  # 리눅스 환경에서 필요, 메모리 캐시 매핑 해제 : 불필요한 공간 차지 방지
    chrome_options.add_argument('--headless')
    chrome_options.add_argument("--disable-gpu")  # GPU 사용 안 함 (headless 모드에서 필요)
    chrome_options.add_argument('--start-fullscreen') # 브라우저 창 크기 설정
    chrome_options.add_argument("--no-sandbox")  # 리눅스 환경에서 필요
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36")

    # # 드라이버 생성
    service = Service(executable_path='/usr/bin/chromedriver')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    # driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get(i)
    
    # 페이지 소스를 가져와 BeautifulSoup로 파싱
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    # 특정 요소가 나타날 때까지 대기
    try:
    # 예를 들어, 페이지 타이틀을 확인하여 페이지가 제대로 로딩되었는지 확인할 수 있습니다.
        WebDriverWait(driver, 2).until(EC.presence_of_element_located(
            (By.XPATH,'//*[@id="__next"]/main/div[1]/div/section/header/div/div[1]/a')))
        # print("웹 페이지가 성공적으로 열렸습니다.")
    except:
        # print("웹 페이지가 제대로 열리지 않았습니다.")
        driver.quit() 
        continue 
        

    # 기업 이름 
    company_name = driver.find_element(By.CSS_SELECTOR,'#__next main div div section header div div a').text 
    name_list.append(company_name) 

    # 모집분야
    job_name =driver.find_element(By.CSS_SELECTOR, '#__next main div div section header h1').text 
    job_list.append(job_name)

    # 상세보기 
    driver.find_element(By.XPATH,'//*[@id="__next"]/main/div[1]/div/section/section/article[1]/div/button').send_keys(Keys.ENTER) # .click() #클릭 안되는 오류로 인해서 엔터로 상세 페이지
    
    # 경력
    try:
        experience = driver.find_element(By.XPATH, '//*[@id="__next"]/main/div[1]/div/section/header/div/div[1]/span[4]').text 
    except:
        experience = None
    experience_list.append(experience)
        

    # url
    url_list.append(driver.current_url) #해당 페이지 URL

    # 자격요건 & 우대사항
    for i in range(2, 4):
        try:
            if i == 2:
                must = re.sub('•','',driver.find_element(By.XPATH,f'//*[@id="__next"]/main/div[1]/div/section/section/article[1]/div/div[{i}]').text).split('\n')[1:]
            must_list.append(must)
        except:
            must_list.append(None)
        try:
            if i == 3:
                prefer = re.sub('•','',driver.find_element(By.XPATH,f'//*[@id="__next"]/main/div[1]/div/section/section/article[1]/div/div[{i}]').text).split('\n')[1:]
            prefer_list.append(prefer)
        except:
            prefer_list.append(None)

    # 마감일자 두가지 경우가 있는 것 같아서 조건문 걸어서 확인 후 있는 경로에 텍스트 받아옴.
    try:
        # 최대 10초 동안 요소가 로드되길 기다림
        end_date_data = driver.find_elements(By.CSS_SELECTOR, '#__next main div div section section article:nth-child(3) span')
        # end_date_data = WebDriverWait(driver,10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR,'#__next main div div section section article:nth-child(3) span')))
        if end_date_data and end_date_data[-1].text.strip():
            last_element = end_date_data[-1].text.strip()
        else:
            end_date_data = driver.find_elements(By.CSS_SELECTOR, '#__next main div div section section article:nth-child(4) span')
            # end_date_data = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '#__next main div div section section article:nth-child(4) span')))
            if end_date_data and end_date_data[-1].text.strip():
                last_element = end_date_data[-1].text.strip()
            else:
                last_element = None  # 명시적으로 None 처리
    except Exception as e:
        print(f"Error while fetching end date: {str(e)}")
        last_element = None  # 예외 발생 시에도 None으로 처리

# 검증 및 로깅
    if last_element is None or last_element == "":
        print("End date is None or empty")
    else:
        print(f"End date fetched: {last_element}")

    end_date_list.append(last_element)
    


    '''
    기업정보 가져오기 
    -> 기업 정보 클릭하는 방식으로 되어 있음.
    '''

    # 화면 최상단으로 이동
    driver.execute_script("window.scrollTo(0, 0)")
    
    # 기업명 클릭     
    try:
        WebDriverWait(driver, 2).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, '#__next main div div section header div div a'))).click()
        # print('click success')
    except:
        # print('click unsuccess')
        pass
    
    time.sleep(1)
    
    # 페이지를 세 번에 걸쳐 스크롤
    page_height = driver.execute_script("return document.body.scrollHeight")
    for i in range(10):
        # 스크롤
        driver.execute_script(f"window.scrollTo(0, {(i+1)*page_height/10});")
        time.sleep(0.1)
        
    # driver.implicitly_wait(2)

    # 매출 
    try:
        # 지정된 css경로에서 'dd' 요소를 찾음
        print('attempting to find element...')
        
        element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(7) > div > dl:nth-child(4) > dd')
        # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(7) > div > dl:nth-child(4) > dd')))
        # element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(7) > div > dl:nth-child(4) > dd')))
        # 요소의 텍스트를 가져옴 첫번쨰
        price = element.text.strip() if element.text.strip() else None
    except Exception as e:
        print(f'Error in first element: {e}')
        price = None
    if not price:
        try:
        # 두 번째 선택자에서 요소 찾기
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(8) > div > dl:nth-child(4) > dd')
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(8) > div > dl:nth-child(4) > dd')))
            price = element.text.strip() if element.text.strip() else None
        except Exception as e:       
            print(f'error in second element: {e}')
            price=None
    if not price:
        try:
        # 세 번째 선택자에서 요소 찾기
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(4) > dd')
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(4) > dd')))
            price = element.text.strip() if element.text.strip() else None
        except Exception as e:       
            print(f'error in third element: {e}')
            price=None
    if not price:
        try:
        # 세 번째 선택자에서 요소 찾기
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(6) > div > dl:nth-child(4) > dd')
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(4) > dd')))
            price = element.text.strip() if element.text.strip() else None
        except Exception as e:       
            print(f'error in fourth element: {e}')
            price=None
    
    # 검증 및 로깅
    if price == "-":
        print("price is None or empty")
        price_list.append('NULL')
    else:
        print(f"price fetched: {price}")

        price_list.append(price)
      
    # 인원(국민연금 가입 사원수) -> 국민연금 탭이 없을 수 있어서 그럴 땐 고용보험 가입 사원 수로 넘어가도록
    try:
        # 지정된 css경로에서 'dd' 요소를 찾음
        print('attempting to find element...')
        element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(7) > div > dl:nth-child(12) > dd')
        # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(7) > div > dl:nth-child(12) > dd')))
        # 요소의 첫번째 텍스트를 가져옴
        member = element.text.strip() if element.text.strip() else None
    except Exception as e:
        print(f"Error in first(12) element: {e}")
        member = None

    if not member:
        try:
            # 세 번째 선택자에서 요소 찾기
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(8) > div > dl:nth-child(12) > dd')
            #__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(7) > div > dl:nth-child(12) > dd
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(8) > div > dl:nth-child(12) > dd')))
            member = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in third element: {type(e).__name__} - {e}')
            member=None
    if not member:
        try:
            # 네 번째 선택자에서 요소 찾기-2
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(8) > div > dl:nth-child(9) > dd')
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(8) > div > dl:nth-child(12) > dd')))
            member = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in fourth element: {type(e).__name__} - {e}')
            member=None
    if not member:
        try:
            # 다섯 번째 선택자에서 요소 찾기
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(12) > dd')
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(12) > dd')))
            member = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in fifth element: {type(e).__name__} - {e}')
            member=None
    if not member:
        try:
            # 여섯 번째 선택자에서 요소 찾기
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(6) > div > dl:nth-child(12) > dd')
            
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(12) > dd')))
            member = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in sixth element: {type(e).__name__} - {e}')
            member=None
    if not member:
        try:
            # 일곱 번째 선택자에서 요소 찾기
            #__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(7) > div > dl:nth-child(12) > dd
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(7) > div > dl:nth-child(15) > dd')
            
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(12) > dd')))
            member = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in seventh element: {type(e).__name__} - {e}')
            member=None
    if not member:
        try:
            # 여덟 번째 선택자에서 요소 찾기
            #__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(7) > div > dl:nth-child(12) > dd
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(8) > div > dl:nth-child(15) > dd')
            
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(12) > dd')))
            member = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in eighth element: {type(e).__name__} - {e}')
            member=None
            
    if not member:
        try:
            # 지정된 css경로에서 'dd' 요소를 찾음
            print('attempting to find element...')
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(7) > div > dl:nth-child(13) > dd')
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(7) > div > dl:nth-child(12) > dd')))
            # 요소의 첫번째 텍스트를 가져옴
            member = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f"Error in first(13) element: {e}")
            member = None
        
    if not member:
        try:
            # 고용보험 가입 사원 수로 찾기 첫번째 요소
            #__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(6) > div > dl:nth-child(9) > dd
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(7) > div > dl:nth-child(9) > dd')
            
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(12) > dd')))
            member = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in another first element: {type(e).__name__} - {e}')
            member=None
    if not member:
        try:
            # 고용보험 가입 사원 수로 찾기 두번째 요소
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(9) > dd')
            
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(12) > dd')))
            member = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in another second element: {type(e).__name__} - {e}')
            member=None
    if not member:
        try:
            # 고용보험 가입 사원 수로 찾기 세번째 요소
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(6) > div > dl:nth-child(9) > dd')
            
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(12) > dd')))
            member = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in another third element: {type(e).__name__} - {e}')
            member=None
    if not member:
        try:
            # 고용보험 가입 사원 수로 찾기 네번째 요소
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(8) > div > dl:nth-child(13) > dd')
            
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(12) > dd')))
            member = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in another fourth element: {type(e).__name__} - {e}')
            member=None
    if not member:
        try:
            # 고용보험 가입 사원 수로 찾기 다섯번째 요소
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(7) > div > dl:nth-child(10) > dd')
            
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(12) > dd')))
            member = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in another fifth element: {type(e).__name__} - {e}')
            member=None
      
    if member == "-":
        print("member is empty")
        member_list.append('NULL')
    else:
        print(f'member: {member}')
        member_list.append(member)
    
    
    # 평균연봉
    try:
        # 지정된 css경로에서 'dd' 요소를 찾음
        print('attempting to find element...')
        
        element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(7) > div > dl:nth-child(6) > dd')
        # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(7) > div > dl:nth-child(6) > dd')))
        # 요소의 첫번째 텍스트를 가져옴
        money = element.text.strip() if element.text.strip() else None
    except Exception as e:
        print(f'Error in first element: {e}')
        money = None
    if not money:
        try:
            # 두 번째 선택자에서 요소 찾기
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(8) > div > dl:nth-child(6) > dd')
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(8) > div > dl:nth-child(6) > dd')))
            money = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in second element : {e}')
            money = None
    if not money:
        try:
            # 세 번째 선택자에서 요소 찾기
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(6) > dd')
            # WebDriverWait(driver, 2).unitl(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(6) > dd')))
            money = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in third element : {e}')
            money = None
    if not money:
        try:
            # 네 번째 선택자에서 요소 찾기
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(6) > div > dl:nth-child(6) > dd')
            # WebDriverWait(driver, 2).unitl(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(6) > dd')))
            money = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in fourth element : {e}')
            money = None
     
    if money == "-":
        print("money is empty")
        money_list.append('NULL')
    else:
        print(f'money: {money}')
        money_list.append(money)


    # 주소 -> 위치 맵에서 주소 텍스트만 받아옴
    try:
        # 지정된 css경로에서 'dd' 요소를 찾음
        print('attempting to find element...')
        
        #__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > div:nth-child(4) > section > div > span
        element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > div:nth-child(3) > section > div > span')
        # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > div:nth-child(3) > section > div > span')))
        location = element.text.strip() if element.text.strip() else None
    except Exception as e:       
      print(f'error in first element: {e}')
      location=None
    if not location:
        try:
            # 두번째 경로에서 요소 가져옴
            
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > div:nth-child(4) > section > div > span')
            #__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > div:nth-child(4) > section > div > span
            #__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > div:nth-child(3) > section > div > span
            #__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > div:nth-child(4) > section > div > span
            #__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > div:nth-child(2) > section > div > span
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > div:nth-child(4) > section > div > span')))
            location = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in second element: {e}')
            location = None
    if not location:
        try:
            # 세번째 경로에서 요소 가져옴
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > div:nth-child(2) > section > div > span')
            #__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > div:nth-child(4) > section > div > span
            #__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > div:nth-child(3) > section > div > span
            #__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > div:nth-child(4) > section > div > span
            
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > div:nth-child(4) > section > div > span')))
            location = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in third element: {e}')
            location = None
      
    if location == " ":
        print("address is empty")
        loca_list.append('NULL')
    else:
        print(f'address: {location}')
        loca_list.append(location)   
    
    
    # 표준산업분류 -> 태그간 부모 자식간의 관계에 이용해서 구하려고 했으나 구조가 너무 복잡합 ----> 결국 element를 이용해서 구함(wanted는 수시로 태그 명이 바뀌어서 아마 막히면 새로 적어줘야 할 것임.)
    try:
        # 지정된 css경로에서 'dd' 요소를 찾음
        print('attempting to find element...')
        
        #__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(1) > dd
        element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(7) > div > dl:nth-child(1) > dd')
        # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(7) > div > dl:nth-child(1) > dd')))
        industry = element.text.strip() if element.text.strip() else None
    except Exception as e:
        print(f'error in first element: {e}')
        industry = None
    if not industry:
        try:
            # 두번쨰 경로에서 요소 가져옴
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(8) > div > dl:nth-child(1) > dd')
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(8) > div > dl:nth-child(1) > dd')))
            industry = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in second element: {e}')
            industry = None
    if not industry:
        try: 
            # 세번째 경로에서 요소 가져옴
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(1) > dd')
            #__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(1) > dd
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(1) > dd')))
            industry = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in third element: {e}')
            industry = None
    if not industry:
        try: 
            # 네번째 경로에서 요소 가져옴
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(6) > div > dl:nth-child(1) > dd')
            #__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(1) > dd
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(1) > dd')))
            industry = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in fourth element: {e}')
            industry = None
    # 최종 결과 처리
    if industry == "-":
        print("industry is empty")
        industry_list.append("NULL")
    else:
        print(f'industry: {industry}')
        industry_list.append(industry)

    # 연혁 -> 표준산업 분류와 마찬가지(같은 태그 안에 dl클래스의 순서만 다름)
    try:
        # 지정된 css경로에서 'dd' 요소를 찾음
        print('attempting to find element...')
        
        element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(7) > div > dl:nth-child(2) > dd')
        # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(7) > div > dl:nth-child(2) > dd')))
        year_element = element.text.strip() if element.text.strip() else None
    except Exception as e:
        print(f'error in first element : {e}')
        year_element = None
    if not year_element:
        try:
            # 두번쨰 경로에서 요소 가져옴
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(8) > div > dl:nth-child(2) > dd')
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(8) > div > dl:nth-child(2) > dd')))
            #__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(2) > dd
            year_element = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in second element : {e}')
            year_element = None
    if not year_element:
        try:
            # 세번쨰 경로에서 요소 가져옴
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(2) > dd')
           #__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(2) > dd
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(2) > dd')))
            year_element = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in third element: {e}')
            year_element = None
    if not year_element:
        try:
            # 네번쨰 경로에서 요소 가져옴
            element = driver.find_element(By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(6) > div > dl:nth-child(2) > dd')
           #__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(2) > dd
            # WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#__next > div.CompanyDetail_CompanyDetail__DkZ6D > div.CompanyDetail_CompanyDetail__ContentWrapper__KNNdN > div > div.CompanyDetail_CompanyDetail__Content__SectionWrapper__Qw2AZ > section:nth-child(9) > div > dl:nth-child(2) > dd')))
            year_element = element.text.strip() if element.text.strip() else None
        except Exception as e:
            print(f'error in fourth element: {e}')
            year_element = None
            
    if year_element == "-":
        print("year is empty")
        year_list.append('NULL')
    else:
        print(f'year: {year_element}')
        year_list.append(year_element)

    now = datetime.datetime.now()
    formatted_now = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    # 새로운 테이블 생성 sql 쿼리
    # create_table_query = '''
    # CREATE TABLE IF NOT EXISTS new_wanted(
    #     date DATETIME,
    #     company_name VARCHAR(255),
    #     address TEXT,
    #     industry VARCHAR(255),
    #     history VARCHAR(255),
    #     career VARCHAR(255),
    #     job_title VARCHAR(255),
    #     keywords TEXT,
    #     preferred_qualifications TEXT,
    #     deadline VARCHAR(255),
    #     revenue VARCHAR(255),
    #     employee_count VARCHAR(255),
    #     salary VARCHAR(255),
    #     recruit_url VARCHAR(255),
    #     PRIMARY KEY (recruit_url)
    # );
    # '''
    # # 테이블 생성
    # cursor.execute(create_table_query)

    # 각 리스트의 가장 최근 추가된 값 INSERT
    insert_query = "INSERT INTO new_wanted (date, company_name, address, industry, history, \
                                        career, job_title, keywords, preferred_qualifications, deadline, \
                                        revenue, employee_count, salary, recruit_url) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                    ON DUPLICATE KEY UPDATE \
                    date = VALUES(date), \
                    company_name = VALUES(company_name), \
                    address = VALUES(address), \
                    industry = VALUES(industry), \
                    history = VALUES(history), \
                    career = VALUES(career), \
                    job_title = VALUES(job_title), \
                    keywords = VALUES(keywords), \
                    preferred_qualifications = VALUES(preferred_qualifications), \
                    deadline = VALUES(deadline), \
                    revenue = VALUES(revenue), \
                    employee_count = VALUES(employee_count), \
                    salary = VALUES(salary), \
                    recruit_url = VALUES(recruit_url) \
                    "
    values = (
        formatted_now, # 현재 시간 
        str(name_list[-1]),  # 회사 이름
        str(loca_list[-1]),  # 주소
        str(industry_list[-1]),  # 산업 분야
        str(year_list[-1]),  # 연혁
        str(experience_list[-1]),  # 경력
        str(job_list[-1]),  # 직무 제목
        str(must_list[-1]),  # 필수 자격
        str(prefer_list[-1]),  # 우대 자격
        str(end_date_list[-1]),  # 마감일
        str(price_list[-1]),  # 매출
        str(member_list[-1]),  # 직원 수
        str(money_list[-1]),  # 연봉
        str(url_list[-1])  # 모집 URL
    )
    # 다른 채용 공고의 마감일 형태와 맞추기 위해 정규식을 이용해서 데이터 형식을 변경함.
    update_query = """
        UPDATE new_wanted
        SET deadline = REPLACE(SUBSTRING_INDEX(deadline, ' ', 1), '.', '-');
        """
    # try:
    #     with connection.cursor() as cursor:
    #         # SQL 쿼리 작성
    #         update_query = """
    #         UPDATE new_wanted
    #         SET deadline = REPLACE(SUBSTRING_INDEX(deadline, ' ', 1), '.', '-');
    #         """
            
    #         # 쿼리 실행
    #         cursor.execute(update_query)
            
    #         # 변경 사항을 커밋
    #         connection.commit()

    # finally:
    # 연결 닫기
    # connection.close()
    cursor.execute(insert_query, values) 
    cursor.execute(update_query)
    # 변경사항 커밋
    connection.commit() 
    
# # 연결 및 커서 닫기  
cursor.close() 
connection.close() 

driver.quit() 
