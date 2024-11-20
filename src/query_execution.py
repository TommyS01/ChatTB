import pandas as pd
import json

# Upload functions
def send_to_postgres(uploaded_file, engine):
    data = pd.read_csv(uploaded_file)
    data.to_sql(name=uploaded_file.name, con=engine, if_exists='replace', index=False)

def send_to_mongo(data, engine):
    pass

# Show tables
psql_show = "SELECT schemaname, tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema');"

mongo_show = "show collections"

def execute_sql(statement):
    # TODO
    pass

def execute_mongo(statement):
    # TODO
    pass

