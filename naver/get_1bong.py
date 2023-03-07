import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import pymysql
from tqdm import tqdm
import sys
import datetime


class Collecting:
    def __init__(self):
        self.db_connect()

        self.company_list = pd.DataFrame()
        self.get_company_list()
        self.stock_df = pd.DataFrame()

        self.today = datetime.date.today().strftime('%Y-%m-%d')
        self.progress_check()

        split_list = list(range(self.i_1+200, len(self.company_list), 200))
        split_list.append(len(self.company_list))

        for i in split_list:
            print(i)
            self.stock_df_total = pd.DataFrame([], columns=['code', 'name', 'date', 'open', 'high', 'low', 'close',
                                                            'volume'])
            for code in tqdm(list(self.company_list['종목코드'])[self.i_1:i]):
                self.get_price(code)
                self.stock_df_total = pd.concat([self.stock_df_total, self.stock_df], ignore_index=True)
                self.i_1 = i

            self.stock_df_total['date'] = pd.to_datetime(self.stock_df_total['date'], format='%Y%m%d')

            if i == 200:
                self.stock_df_total.to_sql(name='stock_daily', con=self.engine, if_exists='replace', index=False)
            else:
                self.stock_df_total.to_sql(name='stock_daily', con=self.engine, if_exists='append', index=False)
            self.cur.execute(f"UPDATE check_table SET date='{self.today}', progress={i} WHERE id=1")
            self.connection.commit()

    def db_connect(self):
        self.engine = create_engine('mysql+pymysql://jystock:wkdwls1992@localhost/stock')
        self.conn = self.engine.connect()
        self.connection = pymysql.connect(host='localhost', user='jystock', password='wkdwls1992', database='stock')
        self.cur = self.connection.cursor()

    def progress_check(self):
        self.cur.execute("SELECT * FROM information_schema.tables WHERE table_schema='stock' AND table_name='check_table'")
        table_exists = bool(self.cur.fetchone())

        if not table_exists:
            self.i_1 = 0
            self.cur.execute("CREATE TABLE check_table (id INT AUTO_INCREMENT PRIMARY KEY, date DATE, progress INT)")
            self.cur.execute(f"INSERT INTO check_table(id, date, progress) VALUES(1,'{self.today}',{self.i_1})")
            self.date = self.today
            self.connection.commit()
        else:
            self.cur.execute("SELECT date, progress FROM check_table WHERE id=1")
            result = self.cur.fetchone()
            self.date = result[0]
            self.i_1 = result[1]

    def get_price(self, company_code):
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'}
        url = "https://fchart.stock.naver.com/sise.nhn?symbol={}&timeframe=day&count=10000&requestType=0".format(company_code)
        get_result = requests.get(url, headers=headers)
        bs_obj = BeautifulSoup(get_result.content, "html.parser")

        inf = bs_obj.select('item')
        columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        self.stock_df = pd.DataFrame([], columns=columns, index=range(len(inf)))

        for i in range(len(inf)):
            self.stock_df.iloc[i] = str(inf[i]['data']).split('|')

        self.stock_df['code'] = company_code
        self.stock_df['name'] = self.company_list[self.company_list['종목코드']==company_code]['회사명'].iloc[0]

    def get_company_list(self):
        self.company_list = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13')[0]
        self.company_list.종목코드 = self.company_list.종목코드.map('{:06d}'.format)
        self.company_list = self.company_list[['회사명', '종목코드']]