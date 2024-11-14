def send_to_postgres(data, engine):
    data.to_sql(name='t', con=engine, if_exists='replace', index=False)

def send_to_mongo(data, engine):
    pass