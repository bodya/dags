import json
import requests
import pandas as pd
from datetime import datetime
from clickhouse_driver import Client
#import pytz

#tz_msk = pytz.timezone('Europe/Moscow')
client = Client(host='152.70.160.172',
               port=9000,
               database='db_moex',
               user='default',
               password='bodya',
               settings={'use_numpy': True})

path = 'http://iss.moex.com/iss/engines/futures/markets/options/securities.json'
req = requests.get(path)
json_data = json.loads(req.text)
securities_df = pd.DataFrame(json_data['securities']['data'],columns=json_data['securities']['columns'])
securities_df.columns = securities_df.columns.str.lower()
#securities_df.insert(0, 'datestamp', str(datetime.now(tz_msk).strftime('%Y-%m-%d %H:%M:%S')), True)
securities_df.insert(0, 'datestamp', str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), True)
#записываем данные из pandas в clickhouse
client.insert_dataframe('INSERT INTO securities_tb VALUES', securities_df)
#дропаем дубликаты
client.execute('OPTIMIZE TABLE securities_tb DEDUPLICATE')
