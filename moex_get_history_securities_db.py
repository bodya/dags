import json
import requests
import pandas as pd
from datetime import datetime
from clickhouse_driver import Client
import pytz
import time

tz_msk = pytz.timezone('Europe/Moscow')
client = Client(host='152.70.160.172',
               port=9000,
               database='db_moex',
               user='default',
               password='bodya',
               settings={'use_numpy': True})

def get_json(path):
    req = requests.get(path)
    return json.loads(req.text)

def get_history_cursor(json_data):
    history_cursor_df = pd.DataFrame(json_data['history.cursor']['data'],columns=json_data['history.cursor']['columns'])
    history_cursor_df.columns = history_cursor_df.columns.str.lower()
    if history_cursor_df.total.item() > history_cursor_df.pagesize.item():
        cursor_step = history_cursor_df.total.item() // history_cursor_df.pagesize.item()
        return [x*history_cursor_df.pagesize.item() for x in range(1,cursor_step+1)]

def history_securities(json_data):
    history_securities_df = pd.DataFrame(json_data['history']['data'],columns=json_data['history']['columns'])
    history_securities_df.columns = history_securities_df.columns.str.lower()
    history_securities_df.drop(['boardid'], axis=1, inplace=True)
    history_securities_df.fillna(0, inplace=True)
    history_securities_df.query('(volume > 0 & openposition == 0) | openposition > 0', inplace=True)
    history_securities_df.insert(0, 'datestamp', datetime.now(tz_msk).strftime('%Y-%m-%d %H:%M:%S'), True)
    return history_securities_df


secid_df = client.query_dataframe('SELECT DISTINCT(secid) from marketdata_tb WHERE toDate(datestamp) = today()')
#secid_df = secid_df.iloc[:1]
for index, row in secid_df.iterrows():
    step = 0
    all_history_securities_df = []
    path = f'http://iss.moex.com/iss/history/engines/futures/markets/options/securities/{row["secid"]}.json?iss.meta=off&start={step}'
    json_data = get_json(path)
    pages = get_history_cursor(json_data)
    all_history_securities_df = history_securities(json_data)
    if pages:
        for page in pages:
            path = f'http://iss.moex.com/iss/history/engines/futures/markets/options/securities/{row["secid"]}.json?iss.meta=off&start={page}'
            json_data = get_json(path)
            temp_df = history_securities(json_data)
            all_history_securities_df = pd.concat([all_history_securities_df, temp_df], ignore_index=True)
    client.insert_dataframe('INSERT INTO new_history_securities_tb VALUES', all_history_securities_df)
    time.sleep(3)
client.execute('OPTIMIZE TABLE new_history_securities_tb DEDUPLICATE')
