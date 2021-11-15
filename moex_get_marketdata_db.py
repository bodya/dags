import json
import requests
import pandas as pd
from datetime import datetime
from clickhouse_driver import Client
import pytz
tz_msk = pytz.timezone('Europe/Moscow')
client = Client(host='152.70.160.172',
               port=9000,
               database='db_moex',
               user='default',
               password='bodya',
               settings={'use_numpy': True})

path = 'http://iss.moex.com/iss/engines/futures/markets/options/securities.json'
req = requests.get(path)
json_data = json.loads(req.text)
marketdata_df = pd.DataFrame(json_data['marketdata']['data'],columns=json_data['marketdata']['columns'])
marketdata_df.columns = marketdata_df.columns.str.lower()
marketdata_df.drop(['bid', 'offer', 'biddeptht', 'numbids', 'offerdepth', 'offerdeptht',
                    'numoffers', 'settletoprevsettleprc', 'biddepth', 'settleprice', 'settletoprevsettle'],
                   axis=1, inplace=True)
marketdata_df.insert(0, 'datestamp', str(datetime.now(tz_msk).strftime('%Y-%m-%d %H:%M:%S')), True)
#marketdata_df.insert(0, 'datestamp', str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), True)
marketdata_df.query('numtrades != 0', inplace=True)
marketdata_df.fillna(0, inplace=True)

#записываем данные из pandas в clickhouse
client.insert_dataframe('INSERT INTO marketdata_tb VALUES', marketdata_df)
#дропаем дубликаты

client.execute('OPTIMIZE TABLE marketdata_tb DEDUPLICATE')
