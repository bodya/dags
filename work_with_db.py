import pandas as pd
from sqlalchemy import create_engine

host = '152.70.175.117'
port = 5432
database='postgres'
user = 'postgres'
password = 'bodya'


engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

df_test = pd.DataFrame(columns=['id','same_valye'])

name = 'test_table'
df_test.to_sql(name, engine, if_exists='replace')
