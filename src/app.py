import os
from dotenv import load_dotenv
import streamlit as st
from sqlalchemy import create_engine

from upload import send_to_postgres, send_to_mongo

# DB access
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# SQL Engine
@st.cache_resource
def get_engine():
    return create_engine(DATABASE_URL)

engine = get_engine()


st.title("ChatTB")

# DB type
db = st.select_slider("Select a DB type to query", options=["PostgreSQL", "MongoDB"], value="PostgreSQL")

# File uplaod
if db == 'PostgreSQL':
    uploaded_file = st.file_uploader("Choose a file", type="csv")
else:
    uploaded_file = st.file_uploader("Choose a file", type="json")


if uploaded_file is not None:
    if st.button("Upload to Database"):
        try:
            send_to_postgres(uploaded_file, engine)
            st.success(f"Data uploaded successfully!")
            show_chatbox = True
        except Exception as e:
            st.error(f"An error occurred: {e}")

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
        st.markdown(user_input)
    
    response = f'Echo: {user_input}' # replace is sql query generation
    st.session_state.messages.append({"role": "assistant", "content": response})
    with st.chat_message('assistant'):
        st.markdown(response)

