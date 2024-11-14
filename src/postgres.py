def upload_csv_to_postgres(data, engine):
    data.to_sql(name='t', con=engine, if_exists='replace', index=False)
