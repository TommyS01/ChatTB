import pandas as pd
import json
from sqlalchemy import inspect, text
from bson import json_util
import re

# Upload functions
def send_to_postgres(uploaded_file, engine):
    data = pd.read_csv(uploaded_file)
    data.to_sql(name=uploaded_file.name.split('.')[0], con=engine, if_exists='replace', index=False)

def send_to_mongo(uploaded_file, client):
    mongodb = client['db']
    collection = mongodb[uploaded_file.name.split('.')[0]]
    data = json_util.loads(uploaded_file.read())
    collection.insert_many(data)

# Show tables
def show_sql_tables(engine):
    inspector = inspect(engine)
    return inspector.get_table_names()

def show_mongo_collections(client):
    return client['db'].list_collection_names()

# Clear database
# TODO

# Execute queries
def execute_sql(query, engine):
    query = text(query)
    rows = []
    with engine.connect() as conn:
        result = conn.execute(query)
        for row in result:
            rows.append(row)
    return rows

def execute_mongo(statement, client):
    outputs = []

    command_list = statement.split('(')[0].split('.')
    collection_name = command_list[1]
    operation_name = command_list[2]
    if operation_name == 'aggregate':
        query = statement.split('(')[1].strip('()')
    else: # find
        query = re.findall('.find\(([^\(\)]*)\)', statement)[0]
        sort = re.findall('.sort\(([^\(\)]*)\)', statement)

    results = None
    if operation_name == 'find':
        regex_q_p = re.findall('^({.*}), ({.*})$', query)
        try:
            q, p = regex_q_p[0]
        except:
            q, p = '{}', '{}'
        q, p = json.loads(q), json.loads(p)
        if len(sort) == 0:
            results = client['db'][collection_name].find(q, p)
        else:
            results = client['db'][collection_name].find(q, p).sort(json.loads(sort[0]))
    else:
        pipeline = json.loads(query)
        results =client['db'][collection_name].aggregate(pipeline)
    
    for doc in results:
        if doc != {}:
            outputs.append(doc)

    return outputs

