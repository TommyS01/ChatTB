import pandas as pd
import json

def send_to_postgres(uploaded_file, engine):
    data = pd.read_csv(uploaded_file)
    data.to_sql(name='t', con=engine, if_exists='replace', index=False)

def send_to_mongo(data, engine):
    pass