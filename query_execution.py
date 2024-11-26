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
        results = client['db'][collection_name].aggregate(pipeline)
    
    for doc in results:
        if doc != {}:
            outputs.append(doc)

    return outputs


# Example query
def example_sql(engine, table, construct=None):
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
        'ORDER BY': None
    }
    index = random.choice(all_cols)
    agg = random.choice(['MAX', 'MIN'])
    condition = random.choice(numeric_cols)
    operator = random.choice(['<', '>', '='])
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
        sql_fields['SELECT'] = f'{index}, {condition}'
    
    output_string = ''
    nl_string = ''
    for s in ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY']:
        if sql_fields[s] is not None:
            output_string += f'{s} {sql_fields[s]} '
            if s == 'SELECT':
                nl_string += f'{s.lower()} {sql_fields[s].replace('(', ' ').replace(')', ' ').lower()}'
            elif s == 'FROM':
                nl_string += f' {s.lower()} {sql_fields[s]}'
            elif s == 'WHERE':
                nl_string += f' {s.lower()} {sql_fields[s].replace('<', 'less than').replace('>', 'greater than').replace('=', 'equals')}'
            elif s == 'ORDER BY':
                nl_string += f' {s.lower()} {sql_fields[s].replace('DESC', 'descending').replace('ASC', 'ascending')}'
                    
    output_string = output_string.strip()
    nl_string = nl_string.strip()

    return nl_string, output_string

def example_mongo(client, table, construct=None):
    db = client['db']
    collection = db[table]
    sample = collection.find_one()
    all_keys = sample.keys()
    keys = [k for k in all_keys if not isinstance(sample[k], list) and not isinstance(sample[k], dict)]

    index = random.choice(keys)
    agg = random.choice(['$max', '$min'])
    condition = random.choice(keys)
    while condition == index:
        condition = random.choice(keys)
    operator = random.choice(['$gt', '$lt', '$eq'])
    val = random.randrange(0, 11)
    direction = random.choice([-1, 1])

    mongo_query = {
        'MATCH': None,
        'GROUP': None,
        'PROJECT': None,
        'SORT': None
    }

    if construct != 'MATCH':
        mongo_query['MATCH'] = random.choice([{'$match': {condition: {operator: val}}}, None])
    else:
        mongo_query['MATCH'] = {'$match': {condition: {operator: val}}}

    if construct != 'GROUP':
        mongo_query['GROUP'] = random.choice([{'$group': {'_id': f'${index}', 'agg': {agg: f'${condition}'}}}, None])
    else:
        mongo_query['GROUP'] = {'$group': {'_id': f'${index}', 'agg': {agg: f'${condition}'}}}

    if mongo_query['GROUP'] is not None:
        mongo_query['PROJECT'] = {'$project': {'_id': 1, 'agg': 1}}
    else:
        mongo_query['PROJECT'] = {'$project': {index: 1}}

    if mongo_query['GROUP'] is not None:
        sort_field = random.choice(['_id', 'agg'])
    else:
        sort_field = random.choice([index, condition])

    if construct != 'SORT':
        mongo_query['SORT'] = random.choice([{'$sort': {sort_field: direction}}, None])
    else:
        mongo_query['SORT'] = {'$sort': {sort_field: direction}}
    
    pipeline = []
    for q in ['MATCH', 'GROUP', 'PROJECT', 'SORT']:
        if mongo_query[q] is not None:
            pipeline.append(mongo_query[q])

    nl_string = ''
    for q in ['PROJECT', 'MATCH', 'GROUP', 'SORT']:
        if mongo_query[q] is not None:
            if q == 'PROJECT':
                d = list(mongo_query['PROJECT']['$project'].keys())
                if '_id' not in d:
                    d.append('_id')
                else:
                    for i in range(len(d)):
                        if d[i] == '_id':
                            d[i] = index
                nl_string += f'select {", ".join(d)}'
            elif q == 'MATCH':
                nl_string += f' where {condition} {operator} {val}'.replace('$lt', 'less than').replace('$gt', 'greater than').replace('$eq', 'equals')
            elif q == 'GROUP':
                nl_string += f' group by {index}'
            elif q == 'SORT':
                nl_string += f' sort by {sort_field} {direction}'.replace('-1', 'descending').replace('1', 'ascending')

    return nl_string, f'db.{table}.aggregate({pipeline})'.replace("'", '"')