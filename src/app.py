import os
import streamlit as st
from sqlalchemy import create_engine
from pymongo import MongoClient

from query_execution import *
from pattern_match import translate_query

# DB access

SQL_URL = st.secrets["DATABASE_URL"]
MONGO_URL = st.secrets["MONGO_URL"]

# SQL Engine
@st.cache_resource
def get_sql_engine():
    return create_engine(SQL_URL)

# Mongo Engine
@st.cache_resource
def get_mongo_client():
    return MongoClient(MONGO_URL)

sql_engine = get_sql_engine()
mongo_client = get_mongo_client()

st.title("ChatTB")

# DB type
db = st.select_slider("Select a DB type to query:", options=["PostgreSQL", "MongoDB"], value="PostgreSQL")

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
                send_to_mongo(uploaded_file, mongo_client)
            st.success(f"Data uploaded successfully!")
        except Exception as e:
            st.error(f"An error occurred: {e}")


if db == 'PostgreSQL':
    st.write(f'Existing tables: {show_sql_tables(sql_engine)}')
else:
    st.write(f'Existing collections: {show_mongo_collections(mongo_client)}')

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
    # Print query
    response = f'Query: {query}'
    st.session_state.messages.append({"role": "assistant", "content": response})
    with st.chat_message('assistant'):
        st.text(response)

    # Print results
    if db_in == '':
        response = execute_sql(query, sql_engine)
    else:
        response = execute_mongo(query, mongo_client)
    st.session_state.messages.append({"role": "assistant", "content": response})
    with st.chat_message('assistant'):
        st.text(response)

