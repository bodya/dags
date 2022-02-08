import pandas as pd
from datetime import datetime
from clickhouse_driver import Client
import pytz
import settings # настройки доступа
import functions # библиотека функций
tz_msk = pytz.timezone('Europe/Moscow')

# создаем подключение к БД
client = Client(host=settings.PROXY_HOST,
               port=settings.PROXY_PORT,
               database=settings.PROXY_DATABASE,
               user=settings.PROXY_USER,
               password=settings.PROXY_PASSWORD,
               settings={'use_numpy': True})

# получаем json данные с сервера
path = 'http://iss.moex.com/iss/engines/futures/markets/options/securities.json'
json_data = functions.get_json(path)

# предобработка фрейма данных для записи
marketdata_df = pd.DataFrame(json_data['marketdata']['data'],columns=json_data['marketdata']['columns'])
marketdata_df.columns = marketdata_df.columns.str.lower()
marketdata_df.drop(['bid', 'offer', 'biddeptht', 'numbids', 'offerdepth', 'offerdeptht',
                    'numoffers', 'settletoprevsettleprc', 'biddepth', 'settleprice', 'settletoprevsettle'],
                   axis=1, inplace=True)
marketdata_df.insert(0, 'datestamp', str(datetime.now(tz_msk).strftime('%Y-%m-%d %H:%M:%S')), True)
marketdata_df.query('numtrades != 0', inplace=True)
marketdata_df.fillna(0, inplace=True)

#записываем данные из pandas в clickhouse
client.insert_dataframe('INSERT INTO new_marketdata_tb VALUES', marketdata_df)

#дропаем дубликаты строк
client.execute('OPTIMIZE TABLE new_marketdata_tb DEDUPLICATE')

