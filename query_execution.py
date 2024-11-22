import pandas as pd
import json
import random
from sqlalchemy import inspect, text, TEXT
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
        columns = result.keys()
        for row in result:
            rows.append(row)
    return rows, columns

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


# Example query
def example_sql(table, engine, construct=None):
    # Split column types
    inspector = inspect(engine)
    columns = inspector.get_columns(table)
    all_cols = []
    string_cols = []
    numeric_cols = []
    for c in columns:
        if isinstance(c['type'], TEXT):
            string_cols.append(c['name'])
        else:
            numeric_cols.append(c['name'])
        all_cols.append(c['name'])

    sql_fields = {
        'SELECT': None,
        'FROM': table,
        'WHERE': None,
        'GROUP BY': None,
        #'HAVING': None,
        'ORDER BY': None
    }
    index = random.choice(all_cols)
    agg = random.choice(['MAX', 'MIN'])
    condition = random.choice(numeric_cols)
    operator = random.choice(['<', '>', '!=', '=', '<=', '>='])
    val = random.randrange(0,11)
    direction = random.choice(['DESC', 'ASC'])

    if construct != 'WHERE':
        sql_fields['WHERE'] = random.choice([f'{condition} {operator} {val}', None])
    else:
        sql_fields['WHERE'] = f'{condition} {operator} {val}'

    if construct != 'GROUP BY':
        sql_fields['GROUP BY'] = random.choice([index, None])
    else:
        sql_fields['GROUP BY'] = index

    if construct != 'ORDER BY':
        sql_fields['ORDER BY'] = random.choice([f'{index} {direction}', None])
    else:
        sql_fields['ORDER BY'] = f'{index} {direction}'

    if sql_fields['GROUP BY'] is not None:
        sql_fields['SELECT'] = f'{index}, {agg}({condition})'
    else:
        sql_fields['SELECT'] = f'{index} {condition}'
    
    output_string = ''
    for s in ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY']:
        if sql_fields[s] is not None:
            output_string += f'{s} {sql_fields[s]} '
    output_string = output_string.strip()

    return output_string

def example_mongo():
    return