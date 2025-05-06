import os
import pandas as pd
import sqlite3

from dotenv import find_dotenv, load_dotenv
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_mistralai.chat_models import ChatMistralAI
from langchain_core.prompts import ChatPromptTemplate

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

API_KEY = os.getenv("API_KEY")
chat = ChatMistralAI(api_key=API_KEY)

dataframe = pd.read_csv('files/products_daily_2024-07-18_2024-10-16_co9020128165.pl.eurolabs.ibm.com.csv')
dataframe["date"] = pd.to_datetime(dataframe["date"])

connection = sqlite3.connect('TestDatabase.db', check_same_thread=False)
dataframe.to_sql('TestTable', connection, if_exists='replace')

def sql_query(query: str):
    return pd.read_sql_query(query, connection).to_dict(orient='records')

system_prompt = """
You are an expert SQL analyst. When appropriate, generate SQL queries based on the user question and the database schema.
When you generate a query, answer the user's question with only the SQL code. Do not say anything else. When using COUNT do not use slashes.

database_schema: [
    {{
        table: 'TestTable',
        columns: [
            {{ name: 'date', type: 'date' }},
            {{ name: 'name', type: 'string' }},
            {{ name: 'id', type: 'string' }},
            {{ name: 'metricName', type: 'string' }},
            {{ name: 'metricQuantity', type: 'int' }},
            {{ name: 'clusterId', type: 'string' }}
        ]
    }}
]
""".strip()

prompt = ChatPromptTemplate.from_messages([
    ("system", system_prompt),
    ("human", "{question}")
])

chain = prompt | chat
