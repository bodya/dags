import json
import requests
import pandas as pd
from datetime import datetime
import pandahouse as ph

connection = dict(database='db_moex',
                  host='http://129.159.245.233:8123/',
                  user='default',
                  password='bodya')

path = 'http://iss.moex.com/iss/engines/futures/markets/options/securities.json'
req = requests.get(path)
json_data = json.loads(req.text)
marketdata_df = pd.DataFrame(json_data['marketdata']['data'],columns=json_data['marketdata']['columns'])
marketdata_df.columns = marketdata_df.columns.str.lower()
marketdata_df.insert(0, 'datestamp', str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')), True)
marketdata_df.query('numtrades != 0', inplace=True)
#записываем данные из pandas в clickhouse
#ph.to_clickhouse(marketdata_df, 'marketdata_tb', index=False, connection=connection)
ph.to_clickhouse(marketdata_df, 'temp_marketdata_tb', index=False, connection=connection)
