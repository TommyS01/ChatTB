import os
import numpy as np
import pandas as pd
import psycopg2 # Run queries through streamlite instead?

MAX_VARCHAR_LEN = 25
example_data = 'data/nfl_receiving.csv' # no dates

def create_table(f, table_name):
    """
    Takes a csv file (or filepath) and generates a query to create the table schema.
    """
    def get_data_type(x):
        if x == 'int64':
            return 'INTEGER'
        elif x == 'float64':
            return 'DOUBLE'
        # TODO: Add datetime variables
        return f'VARCHAR({MAX_VARCHAR_LEN})'

    df = pd.read_csv(f)
    types_array = df.dtypes.apply(lambda x: x.name).values
    sql_types_array = np.vectorize(get_data_type)(types_array)
    
    paired = list(zip(df.columns.values, sql_types_array))
    type_joined_columns = list(map(lambda pair: ' '.join(pair), paired))
    values_string = ', '.join(type_joined_columns)
    query_string = f'CREATE TABLE {table_name} ({values_string});'
    return query_string

def insert_df_row(cur, table_name, values):
    n_cols = values.shape[0]
    placeholder = ('(%s) ' * n_cols).strip()
    insert_query = f'INSERT INTO {table_name} VALUES (%s)'
    pass