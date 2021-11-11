import json
import requests
import pandas as pd
from datetime import datetime
import pandahouse as ph


connection = dict(database='db_moex',
                  host='http://152.70.160.172:8123/',
                  user='default',
                  password='bodya')

path = 'http://iss.moex.com/iss/engines/futures/markets/options/securities.json'
req = requests.get(path)
json_data = json.loads(req.text)
securities_df = pd.DataFrame(json_data['securities']['data'],columns=json_data['securities']['columns'])
securities_df.columns = securities_df.columns.str.lower()
securities_df.insert(0, 'datestamp', str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), True)
#записываем данные из pandas в clickhouse
ph.to_clickhouse(securities_df, 'securities_tb', index=False, connection=connection)
