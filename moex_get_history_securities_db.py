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

secid_df = client.query_dataframe('SELECT DISTINCT(secid) from marketdata_tb WHERE toDate(datestamp) = today()')
#secid_df = secid_df.iloc[:1]
for index, row in secid_df.iterrows():
    path = f'http://iss.moex.com/iss/history/engines/futures/markets/options/boards/ROPD/securities/{row["secid"]}/candles.json'
    req = requests.get(path)
    json_data = json.loads(req.text)
    history_securities_df = pd.DataFrame(json_data['history']['data'],columns=json_data['history']['columns'])
    history_securities_df.columns = history_securities_df.columns.str.lower()
    history_securities_df.drop(['boardid'], axis=1, inplace=True)
    history_securities_df.fillna(0, inplace=True)
    history_securities_df.insert(0, 'datestamp', datetime.now(tz_msk).strftime('%Y-%m-%d %H:%M:%S'), True)
    # записываем данные из pandas в clickhouse
    client.insert_dataframe('INSERT INTO history_securities_tb VALUES', history_securities_df)
#дропаем дубликаты
client.execute('OPTIMIZE TABLE history_securities_tb DEDUPLICATE')
