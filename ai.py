import os
import pandas as pd
import sqlite3
import glob

from dotenv import find_dotenv, load_dotenv
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_mistralai.chat_models import ChatMistralAI
from langchain_core.prompts import ChatPromptTemplate

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

API_KEY = os.getenv("API_KEY")
chat = ChatMistralAI(api_key=API_KEY)

csv_files = []

for file in glob.glob("C:/zipfiles/*.csv"):
    csv_files.append(file)

connection = sqlite3.connect('TestDatabase.db', check_same_thread=False)

table_schemas = []

def sql_query(query: str):
    return pd.read_sql_query(query, connection).to_dict(orient='records')

def map_dtype(dtype):
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "date"
    elif pd.api.types.is_integer_dtype(dtype):
        return "int"
    elif pd.api.types.is_float_dtype(dtype):
        return "float"
    else:
        return "string"

for id, fpath in enumerate(csv_files):
    table_name = f"Table{id+1}"
    dataframe = pd.read_csv(fpath)
    dataframe.columns = dataframe.columns.str.replace(' ', '_')

    if 'date' in dataframe.columns:
        dataframe['date'] = pd.to_datetime(dataframe['date'])

    dataframe.to_sql(table_name, connection, if_exists='replace')

    columns_schema = ",\n            ".join([
        f"{{{{ name: '{col}', type: '{map_dtype(dtype)}' }}}}"
        for col, dtype in dataframe.dtypes.items()
    ])

    table_schema = f"""
        {{{{
            table: '{table_name}',
            columns: [
                {columns_schema}
            ]
        }}}}
    """.strip()

    table_schemas.append(table_schema)

database_schema = "[\n    " + ",\n    ".join(table_schemas) + "\n]"

system_prompt = f"""
You are an expert SQL analyst. When appropriate, generate SQL queries based on the user question and the database schema.
When you generate a query, answer the user's question with only the SQL code. 
If a query requires searching for a value across multiple tables, you MUST include all relevant tables in the query using UNION ALL.
When using UNION ALL use SELECT only on the column of interest, not on * (e.g. SELECT Product_Name not SELECT *)
Do NOT stop at the first matching table. Ensure that the query includes every table where relevant columns exist.
Make sure your SQL is syntactically correct. Always include a space between SQL keywords (e.g., SELECT COUNT(*) not SELECTCOUNT(*)). 
Do not use backslashes or extra formatting. Output raw SQL only.

database_schema: {database_schema}
""".strip()

prompt = ChatPromptTemplate.from_messages([
    ("system", system_prompt),
    ("human", "{question}")
])

chain = prompt | chat
