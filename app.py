import streamlit as st
import google.generativeai as genai
import re
from sqlalchemy import create_engine
import pandas as pd
from langchain.memory import ConversationBufferMemory
from dotenv import load_dotenv
import os

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

genai.configure(api_key=GEMINI_API_KEY)

DATABASE_URL = "mysql+pymysql://root:toor@127.0.0.1/shop"

engine = create_engine(DATABASE_URL)

memory = ConversationBufferMemory()

schema_info = """
The database consists of the following tables:
1. warehouse: 
    - warehouse_id (Primary Key)
    - unit_code 
    - storage_condition 
    - warehouse_city

2. product:
    - product_code (Primary Key)
    - product_name 
    - warehouse_id (Foreign Key referencing warehouse.warehouse_id)
    - quantity_stock 
    - price_unit

3. customer:
    - customer_code (Primary Key)
    - customer_name 
    - gender 
    - age 
    - phone_number 
    - city

4. order_details:
    - order_code (Primary Key)
    - customer_code (Foreign Key referencing customer.customer_code)
    - product_code (Foreign Key referencing product.product_code)
    - quantity_stock 
    - quantity_ordered 
    - order_date (in dd-mm-yyyy format)
    - shipping_date 
    - status 
    - sales
"""

def clean_sql_query(sql_query):
    sql_query = re.sub(r"```sql|```", "", sql_query, flags=re.IGNORECASE).strip()
    return sql_query

def generate_sql_query(user_query, chat_history):
    prompt = f"""
    Using the following schema: {schema_info}

    Conversation history:
    {chat_history}

    Convert this natural language query into an SQL query: {user_query}.

    - Use proper JOIN conditions between tables.
    - Ensure that the date format follows 'YYYY-MM-DD'.
    - Do not use STRFTIME or SUBSTR for DATE.
    - The result should include both the product name and total sales amount.
    """
    model = genai.GenerativeModel("gemini-2.0-flash-exp")
    response = model.generate_content(prompt)
    
    response_text = response.text if hasattr(response, "text") else response.get('text', '')

    return clean_sql_query(response_text)

def execute_query(sql_query):
    try:
        with engine.connect() as connection:
            result = pd.read_sql(sql_query, connection)
            return result
    except Exception as e:
        return f"Error: {str(e)}"


def generate_natural_summary(df, user_query):
    prompt = f"""
    The user asked: "{user_query}"

    Here is the SQL query result:

    {df.to_string(index=False)}

    Provide a concise and informative summary in natural language.
    """
    model = genai.GenerativeModel("gemini-2.0-flash-exp")
    response = model.generate_content(prompt)
    
    return response.text if hasattr(response, "text") else response.get('text', '')

st.title("Chatbot to Query Database")

if "messages" not in st.session_state:
    st.session_state.messages = []  


for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if user_query := st.chat_input("Ask me a question"):
    st.session_state.messages.append({"role": "user", "content": user_query})
    with st.chat_message("user"):
        st.markdown(user_query)

    chat_history = "\n".join([msg["content"] for msg in st.session_state.messages])
    sql_query = generate_sql_query(user_query, chat_history)

    memory.save_context({"input": user_query}, {"output": sql_query})

    result = execute_query(sql_query)
    
    if isinstance(result, pd.DataFrame):
        summary = generate_natural_summary(result, user_query)
    else:
        summary = f"Error: {result}"

    with st.chat_message("assistant"):
        st.markdown(summary)

    st.session_state.messages.append({"role": "assistant", "content": summary})
