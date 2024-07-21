
import pandas as pd
import requests
import calendar, json
from datetime import datetime
from bs4 import BeautifulSoup
from io import StringIO
from threading import Timer
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

class DBUpdater:
    def __init__(self):
        """생성자: SQLAlchemy 연결 및 종목코드 딕셔너리 생성"""
        self.engine = create_engine('mysql+pymysql://root:sk1127..@localhost:3306/Investar?charset=utf8')
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        with self.engine.connect() as conn:
            sql = """
            CREATE TABLE IF NOT EXISTS company_info (
	            code VARCHAR(20), 
                company VARCHAR(40), 
                last_update DATE, 
                PRIMARY KEY (code))
            """
            conn.execute(text(sql))
            sql = """
            CREATE TABLE IF NOT EXISTS daily_price (
                code VARCHAR(20), 
                date DATE, 
                open BIGINT(20), 
                high BIGINT(20), 
                low BIGINT(20), 
                close BIGINT(20), 
                diff BIGINT(20), 
                volume BIGINT(20), 
                PRIMARY KEY (code, date))
            """
            conn.execute(text(sql))
        self.session.commit()
        self.codes = dict()

    def __del__(self):
        """소멸자: SQLAlchemy 연결 해제"""
        self.session.close()
        self.engine.dispose()

    def read_krx_code(self):
        """KRX로부터 상장법인목록 파일을 읽어와서 데이터프레임으로 반환"""
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
        response = requests.get(url)
        response.encoding = 'euc-kr'
        krx = pd.read_html(response.text, header=0)[0]
        krx = krx[['종목코드', '회사명']]
        krx = krx.rename(columns={'종목코드': 'code', '회사명': 'company'})
        krx.code = krx.code.map('{:06d}'.format)
        return krx

    def update_comp_info(self):
        """종목코드를 company_info 테이블에 업데이트한 후 딕셔너리에 저장"""
        sql = "SELECT * FROM company_info"
        df = pd.read_sql(sql, self.engine)
        for idx in range(len(df)):
            self.codes[df['code'].values[idx]] = df['company'].values[idx]

        with self.engine.connect() as conn:
            sql = "SELECT max(last_update) FROM company_info"
            rs = conn.execute(text(sql)).fetchone()
            today = datetime.today().strftime('%Y-%m-%d')

            if rs[0] is None or rs[0].strftime('%Y-%m-%d') < today:
                krx = self.read_krx_code()
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]
                    sql = f"""
                    REPLACE INTO company_info (code, company, last_update) 
                    VALUES ('{code}', '{company}', '{today}')
                    """
                    conn.execute(text(sql))
                    self.codes[code] = company
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] {idx:04d} REPLACE INTO company_info VALUES ({code}, {company}, {today})")
                self.session.commit()
                print('')

    def read_naver(self, code, company, pages_to_fetch):
        """네이버 금융에서 주식 시세를 읽어서 데이터프레임으로 반환"""
        try:
            url = f"https://finance.naver.com/item/sise_day.nhn?code={code}"
            with requests.get(url, headers={'User-agent': 'Mozilla/5.0'}) as doc:
                doc.encoding = 'euc-kr'
                if doc is None:
                    return None
                html = BeautifulSoup(doc.text, "lxml")
                pgrr = html.find("td", class_="pgRR")
                if pgrr is None:
                    return None
                s = str(pgrr.a["href"]).split('=')
                lastpage = s[-1]
            df = pd.DataFrame()
            pages = min(int(lastpage), pages_to_fetch)
            for page in range(1, pages + 1):  # range 수정
                pg_url = '{}&page={}'.format(url, page)
                response = requests.get(pg_url, headers={'User-agent': 'Mozilla/5.0'})
                response.encoding = 'euc-kr'
                newDf = pd.read_html(StringIO(response.text), header=0)[0]
                df = pd.concat([df, newDf])  # ignore_index 추가
                tmnow = datetime.now().strftime("%Y-%m-%d %H:%M")
                print('[{}] {} ({}) : {:04d}/{:04d} pages are downloading...'.format(tmnow, company, code, page,
                                                                                     pages), end="\r")
            df = df.rename(columns={'날짜': 'date', '종가': 'close', '전일비': 'diff', '시가': 'open', '고가': 'high',
                                    '저가': 'low', '거래량': 'volume'})
            df['date'] = df['date'].str.replace('.', '-')
            df = df.dropna()

            # 정리 작업 추가
            df = df.replace({'상승': '', '하락': '', '보합': '', '상한가': '', '하한가': '', ',': ''}, regex=True)

            # 개별 열에 대해 strip 적용
            df['close'] = df['close'].map(lambda x: str(x).strip())
            df['diff'] = df['diff'].map(lambda x: str(x).strip())
            df['open'] = df['open'].map(lambda x: str(x).strip())
            df['high'] = df['high'].map(lambda x: str(x).strip())
            df['low'] = df['low'].map(lambda x: str(x).strip())
            df['volume'] = df['volume'].map(lambda x: str(x).strip())

            # 정수로 변환
            df[['close', 'diff', 'open', 'high', 'low', 'volume']] = df[
                ['close', 'diff', 'open', 'high', 'low', 'volume']].astype(float).astype(int)

            df = df[['date', 'open', 'high', 'low', 'close', 'diff', 'volume']]
        except Exception as e:
            print('Exception occurred :', str(e))
            return None
        return df

    def replace_into_db(self, df, num, code, company):
        """네이버 금융에서 읽어온 주식 시세를 DB에 REPLACE"""
        with self.engine.connect() as conn:
            for r in df.itertuples():
                sql = "REPLACE INTO daily_price (code, date, open, high, low, close, diff, volume) VALUES ('{}', "\
                      "'{}', {}, {}, {}, {}, {}, {})".format(code, r.date, r.open, r.high, r.low, r.close,
                                                             r.diff, r.volume)
                conn.execute(text(sql))
            self.session.commit()
            print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO daily_price [OK]'.format(datetime.now().
                                                                                          strftime('%Y-%m-%d %H:%M'),
                                                                                          num + 1, company, code,
                                                                                          len(df)
                                                                                          ))

    def update_daily_price(self, pages_to_fetch):
        """KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업로드"""
        print(self.codes)
        for idx, code in enumerate(self.codes):
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None:
                continue
            self.replace_into_db(df, idx, code, self.codes[code])

    def execute_daily(self):
        """실행 즉시 및 매일 오후 다섯시에 daily_price 테이블 업데이트"""
        self.update_comp_info()

        try:
            with open('config.json', 'r') as in_file:
                config = json.load(in_file)
                pages_to_fetch = config['pages_to_fetch']
        except FileNotFoundError:
            with open('config.json', 'w') as out_file:
                pages_to_fetch = 100
                config = {'pages_to_fetch': 1}
                json.dump(config, out_file)
        self.update_daily_price(pages_to_fetch)

        tmnow = datetime.now()
        lastday = calendar.monthrange(tmnow.year, tmnow.month)[1]
        if tmnow.month == 12 and tmnow.day == lastday:
            tmnext = tmnow.replace(year=tmnow.year + 1, month=1, day=1, hour=17, minute=0, second=0)
        elif tmnow.day == lastday:
            tmnext = tmnow.replace(month=tmnow.month + 1, day=1, hour=17, minute=0, second=0)
        else:
            tmnext = tmnow.replace(day=tmnow.day + 1, hour=17, minute=0, second=0)
        tmdiff = tmnext - tmnow
        secs = tmdiff.seconds
        t = Timer(secs, self.execute_daily)
        print("Waiting for next update ({}) ... ".format(tmnext.strftime('%Y-%m-%d %H:%M')))
        t.start()

if __name__ == '__main__':
    dbu = DBUpdater()
    dbu.execute_daily()