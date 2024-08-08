import pymysql
from sqlalchemy import create_engine
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
import sys

def get_postUrls(item:list, user, passwd):
    headers = {"User-Agent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"}
    engine = create_engine(f"mysql+pymysql://{user}:{passwd}@ls-0417629e59c83e2cfae4e2aac001b7eee2799e0e.cxiwwsmmq2ua.ap-northeast-2.rds.amazonaws.com/joonggoinfo")
    engine.connect()

    
    start_price = 0

    while (start_price < 3000000):
        df1 = pd.DataFrame(columns=['num', 'site_name', 'keyword'])

        
        for page_num in range(1,126):
            try:
                
                url = f"https://web.joongna.com/search/{item[1]}?page={page_num}&saleYn=SALE_Y&sort=RECENT_SORT&minPrice={start_price}&maxPrice={start_price+499999}&productFilterType=APP"
                res = requests.get(url, headers=headers)
                res_body = res.text
                soup = BeautifulSoup(res_body, 'html.parser')


                post_section = soup.find('main').find('div', "w-full text").find('ul', "grid").find_all('li')

                for post in post_section:
                    try:
                        anchor = post.find('a')['href']
                        df2 = pd.DataFrame([[int(anchor[9:]), 'JoongnaSite', item[0]]], columns=['num', 'site_name', 'keyword'])
                        df1 = pd.concat([df1, df2], ignore_index=True)
                    except:
                        continue
                print('.', end='', flush=True)
            except:
                continue
        print('inserted')
        df1.to_sql(name = 'Urls', con=engine, if_exists='append', index=False)
        start_price += 500000

if __name__ == "__main__" :
    item = ["iPhone14", "%EC%95%84%EC%9D%B4%ED%8F%B014"]
    if len(sys.argv) != 3:
        print("usage: python3 ~.py user passwd")
        sys.exit(1)

    user = sys.argv[1]
    passwd = sys.argv[2]
    get_postUrls(item, user, passwd)

   