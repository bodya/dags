import pandas as pd
from clickhouse_driver import Client
import pytz
import time
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

# получаем список финансовых инструментов
secid_df = client.query_dataframe('SELECT DISTINCT(secid) from marketdata_tb WHERE toDate(datestamp) = today()')
#secid_df = secid_df.iloc[:1]

# собираем историю изменений по каждому фин.инструменту
for index, row in secid_df.iterrows():
    step = 0
    all_history_securities_df = []
    path = f'http://iss.moex.com/iss/history/engines/futures/markets/options/securities/{row["secid"]}.json?iss.meta=off&start={step}'
    json_data = functions.get_json(path)
    pages = functions.get_history_cursor(json_data)
    all_history_securities_df = functions.history_securities(json_data)

    # обработка пагинации страниц, если данные не поместились на одной странице
    if pages:
        for page in pages:
            path = f'http://iss.moex.com/iss/history/engines/futures/markets/options/securities/{row["secid"]}.json?iss.meta=off&start={page}'
            json_data = functions.get_json(path)
            temp_df = functions.history_securities(json_data)
            all_history_securities_df = pd.concat([all_history_securities_df, temp_df], ignore_index=True)

    # вставляем в БД полученный датафрейм
    client.insert_dataframe('INSERT INTO new_history_securities_tb VALUES', all_history_securities_df)
    time.sleep(3)
client.execute('OPTIMIZE TABLE new_history_securities_tb DEDUPLICATE')
