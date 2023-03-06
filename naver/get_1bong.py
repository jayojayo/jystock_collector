import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import pymysql
from tqdm import tqdm


class Collecting:
    def __init__(self):
        self.db_connect()

        self.company_list = pd.DataFrame()
        self.get_company_list()
        self.stock_df = pd.DataFrame()

        split_list = list(range(0,len(self.company_list),200))
        split_list.append(len(self.company_list))

        i_1 = 0
        for i in split_list:
            self.stock_df_total = pd.DataFrame([], columns=['code', 'name', 'date', 'open', 'high', 'low', 'close',
                                                            'volume'])
            for code in tqdm(list(self.company_list['종목코드'])[i_1:i]):
                self.get_price(code)
                self.stock_df_total = pd.concat([self.stock_df_total, self.stock_df], ignore_index=True)
                i_1 = i

            self.stock_df_total['date'] = pd.to_datetime(self.stock_df_total['date'], format='%Y%m%d')

            if i == 500:
                self.stock_df_total.to_sql(name='stock_daily', con=self.db_connection, if_exists='replace', index=False)
            else:
                self.stock_df_total.to_sql(name='stock_daily', con=self.db_connection, if_exists='append', index=False)

    def db_connect(self):
        self.db_connection = create_engine('mysql+pymysql://jystock:wkdwls1992@localhost/stock')
        conn = self.db_connection.connect()

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