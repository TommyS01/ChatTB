import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from postgres import upload_csv_to_postgres

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

@st.cache_resource
def get_engine():
    return create_engine(DATABASE_URL)

st.title("ChatTB")

engine = get_engine()

uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    data = pd.read_csv(uploaded_file)
    if st.button("Upload to Database"):
        try:
            upload_csv_to_postgres(data, engine)
            st.success(f"Data uploaded successfully!")
        except Exception as e:
            st.error(f"An error occurred: {e}")
        st.write("Preview:", data.head())