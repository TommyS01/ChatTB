import pandas as pd
import json
from sqlalchemy import inspect, text

# Upload functions
def send_to_postgres(uploaded_file, engine):
    data = pd.read_csv(uploaded_file)
    data.to_sql(name=uploaded_file.name.split('.')[0], con=engine, if_exists='replace', index=False)

def send_to_mongo(data, engine):
    pass

# Show tables
def show_sql_tables(engine):
    inspector = inspect(engine)
    return inspector.get_table_names()


def show_mongo_collections():
    # TODO show mongo tables
    pass

# Clear database


# Execute queries
def execute_sql(query, engine):
    query = text(query)
    rows = []
    with engine.connect() as conn:
        result = conn.execute(query)
        for row in result:
            rows.append(row)
    return rows

def execute_mongo(statement):
    # TODO
    pass

