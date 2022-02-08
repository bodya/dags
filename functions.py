import json
import requests
from datetime import datetime

def get_json(path):
    """получение json данных по path"""
    req = requests.get(path)
    return json.loads(req.text)

def get_history_cursor(json_data):
    """получение списка страниц пагинации для фин. инструмента"""
    history_cursor_df = pd.DataFrame(json_data['history.cursor']['data'],columns=json_data['history.cursor']['columns'])
    history_cursor_df.columns = history_cursor_df.columns.str.lower()
    if history_cursor_df.total.item() > history_cursor_df.pagesize.item():
        cursor_step = history_cursor_df.total.item() // history_cursor_df.pagesize.item()
        return [x*history_cursor_df.pagesize.item() for x in range(1,cursor_step+1)]

def history_securities(json_data):
    """предобработка датафрейма к сохранению в БД"""
    history_securities_df = pd.DataFrame(json_data['history']['data'],columns=json_data['history']['columns'])
    history_securities_df.columns = history_securities_df.columns.str.lower()
    history_securities_df.drop(['boardid'], axis=1, inplace=True)
    history_securities_df.fillna(0, inplace=True)
    history_securities_df.query('(volume > 0 & openposition == 0) | openposition > 0', inplace=True)
    history_securities_df.insert(0, 'datestamp', datetime.now(tz_msk).strftime('%Y-%m-%d %H:%M:%S'), True)
    return history_securities_df