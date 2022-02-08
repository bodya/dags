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
securities_df = pd.DataFrame(json_data['securities']['data'],columns=json_data['securities']['columns'])
securities_df.columns = securities_df.columns.str.lower()
securities_df.insert(0, 'datestamp', str(datetime.now(tz_msk).strftime('%Y-%m-%d %H:%M:%S')), True)

#записываем данные из pandas в clickhouse
client.insert_dataframe('INSERT INTO new_securities_tb VALUES', securities_df)

#дропаем дубликаты строк
client.execute('OPTIMIZE TABLE new_securities_tb DEDUPLICATE')
