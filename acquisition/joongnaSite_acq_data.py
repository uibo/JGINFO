from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
import pandas as pd
import time
import pymysql
from sqlalchemy import create_engine
import sys

def get_postInfo(user, passwd):
    engine = create_engine(f"mysql+pymysql://{user}:{passwd}@ls-0417629e59c83e2cfae4e2aac001b7eee2799e0e.cxiwwsmmq2ua.ap-northeast-2.rds.amazonaws.com/joonggoinfo")
    engine.connect()
    Urls = pd.read_sql('SELECT * FROM Urls', engine)

    chrome_options = Options()
    chrome_options.add_argument("--headless")  # GUI 없이 실행
    chrome_options.add_argument("--no-sandbox")  # 샌드박스 모드 비활성화
    chrome_options.add_argument("--disable-dev-shm-usage")  # /dev/shm 사용 비활성화
    chrome_options.add_argument("--log-level=3") # 로그 수준을 낮춰 warning message 출력 제한 

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    i = 0
    while i < len(Urls):
        try:
            url = "https://web.joongna.com/product/" + str(Urls.loc[i, 'num'])
            driver.get(url)
            # article element 로드 될때까지 기다림
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "article")))
            
            # 거래희망지역 기준으로 context와 location분할
            content = driver.find_element(By.TAG_NAME, 'article').text
            if "거래희망지역" in content:
                content = content.split("거래희망지역")
                location = content[1]
                content = content[0]
            else:
                location = None

            # post_info 추출
            title_tag = driver.find_element(By.TAG_NAME, 'h1')
            complex = driver.execute_script("return arguments[0].parentNode;", title_tag)
            complex = driver.execute_script("return arguments[0].parentNode;", complex)
            complex = complex.text.split('\n')
            title = complex[0]
            price = int(complex[1].replace("원", '').replace(",", ''))
            upload_date = str(complex[2][:(complex[2].find('·'))-1])
            img_url = driver.find_element(By.CLASS_NAME, 'col-span-1').find_element(By.TAG_NAME, 'img').get_attribute("src")
            if '전' in upload_date:
                if '일' in upload_date:
                    now = datetime.now()
                    delta = timedelta(int(upload_date[:upload_date.find('일')]))
                    upload_date = (now - delta)
                else :
                    upload_date = datetime.now()
            # post_info를 하나의 sample로 변경
            try:
                status = driver.find_element(By.CLASS_NAME, 'col-span-1').find_element(By.CLASS_NAME, 'absolute').text
                if status == '판매완료' :
                    status = True
                else:
                    status = False
            except:
                status = 0
            sample = pd.DataFrame([{'id': Urls.loc[i, 'id'] ,'title': title, 'content':content, 'price':price, 'upload_date':upload_date, 'location':location, 'status':status, 'img_url':img_url}])
            
            sample.to_sql(name="post_iPhone14", con=engine, if_exists='append', index=False)
            # post_info가 정상적으로 추출됨을 알림
            print(i, flush=True)
        finally:
            i += 1

    


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: python3 ~.py user passwd")
        sys.exit(1)

    user = sys.argv[1]
    passwd = sys.argv[2]

    get_postInfo(user, passwd)
