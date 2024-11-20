import os
from dotenv import load_dotenv
import streamlit as st
from sqlalchemy import create_engine

from query_execution import *
from pattern_match import translate_query

# DB access
load_dotenv()
SQL_URL = os.getenv("DATABASE_URL")

# SQL Engine
@st.cache_resource
def get_sql_engine():
    return create_engine(SQL_URL)

# Mongo Engine
@st.cache_resource
def get_mongo_client():
    return ...

sql_engine = get_sql_engine()
mongo_engine = get_mongo_client()

st.title("ChatTB")

# DB type
db = st.select_slider("Select a DB type to query", options=["PostgreSQL", "MongoDB"], value="PostgreSQL")

# File upload
if db == 'PostgreSQL':
    uploaded_file = st.file_uploader("Choose a file", type="csv")
else:
    uploaded_file = st.file_uploader("Choose a file", type="json")

if uploaded_file is not None:
    if st.button("Upload to Database"):
        try:
            if db == 'PostgreSQL':
                send_to_postgres(uploaded_file, sql_engine)
            else:
                send_to_mongo(uploaded_file, mongo_engine)
            st.success(f"Data uploaded successfully!")
        except Exception as e:
            st.error(f"An error occurred: {e}")


if db == 'PostgreSQL':
    st.write(f'Existing tables: {show_sql_tables(sql_engine)}')
else:
    # TODO: List mongo tables
    st.write(f'Existing collections: {show_mongo_collections()}')

# Chat
st.subheader('Chat')

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message['role']):
        st.markdown(message['content'])

# User input form
user_input = st.chat_input("You:", key="input")

# When user presses Enter
if user_input:
    # Append user message to chat history
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message('user'):
        st.text(user_input)
    
    # Set correct db type
    db_in = 'mongo' if db == 'MongoDB' else ''

    query = translate_query(user_input, db=db_in)
    response = f'{query}'
    if db_in == '':
        response = response + '\n' + '\n'.join(execute_sql(query, sql_engine))
    else:
        execute_mongo(query)
    # TODO: show query results
    st.session_state.messages.append({"role": "assistant", "content": response})
    with st.chat_message('assistant'):
        st.text(response)

