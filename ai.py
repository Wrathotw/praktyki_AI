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

def load_dataframes_into_db(dataframes):
    global table_schemas
    table_schemas.clear()

    for id, (filename, df) in enumerate(dataframes):
        table_name = f"Table{id+1}"
        df.columns = df.columns.str.replace(' ', '_')

        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')

        df.to_sql(table_name, connection, if_exists='replace')

        columns_schema = ",\n            ".join([
            f"{{{{ name: '{col}', type: '{map_dtype(dtype)}' }}}}"
            for col, dtype in df.dtypes.items()
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

def get_prompt_chain():
    database_schema = "[\n    " + ",\n    ".join(table_schemas) + "\n]"
    system_prompt = f"""
    You are an expert SQL analyst. When appropriate, generate SQL queries based on the user question and the database schema.
    When you generate a query, answer the user's question with only the SQL code. 
    Run the queries on tables, not the entire database.
    If a query requires searching for a value across multiple tables, you MUST include all relevant tables in the query using UNION ALL.
    When using UNION ALL use SELECT only on the column of interest, not on * (e.g. SELECT Product_Name not SELECT *)
    Do NOT stop at the first matching table. Ensure that the query includes every table where relevant columns exist.
    Make sure that the relevant column exists in every table, if it doesn't ignore that table.
    Make sure your SQL is syntactically correct. Always include a space between SQL keywords (e.g., SELECT COUNT(*) not SELECTCOUNT(*)). 
    Do not use backslashes or extra formatting. Output raw SQL only.

    Examples — CORRECT:
    User: Find all entries containing the word "toolkit"
    Assistant:
    SELECT Product_Name FROM Table1 WHERE Product_Name LIKE '%toolkit%'
    UNION ALL
    SELECT Product_Name FROM Table2 WHERE Product_Name LIKE '%toolkit%'

    User: How many entries contain "xyz" in their name
    Assistant:
    Select COUNT(SELECT Product_Name FROM Table1 WHERE Product_Name LIKE '%xyz%' UNION ALL SELECT Product_Name FROM Table2 WHERE Product_Name LIKE '%xyz%')

    Examples — INCORRECT:
    User: Find all entries containing the word "toolkit"
    Assistant:
    SELECT Product_Name FROM Table1 WHERE Product_Name LIKE '%toolkit%'; -- This finds product names

    User: How many entries contain "xyz" in their name
    Assistant:
    SELECT Product_Name FROM Table1 WHERE Product_Name LIKE '%xyz%'
    UNION ALL
    SELECT Product_Name FROM Table2 WHERE Product_Name LIKE '%xyz%'

    User: Are there any "xyz"
    Assistant: 
    SELECT COUNT(*) FROM ( SELECT Publisher FROM Table1 WHERE Product_Name LIKE '%xyz%' UNION ALL SELECT Publisher FROM Table2 WHERE Product_Name LIKE '%xyz%' UNION ALL SELECT Publisher FROM Table9 WHERE Product_Name LIKE '%xyz%' UNION ALL SELECT Publisher FROM Table11 WHERE Product_Name LIKE '%xyz%' ) AS AIX7_1_Publisher_Table 
    This query will check for the existence of 'xyz' in the Product_Name column of each table, and then count the number of occurrences.

    User: How many entries have their peak date on 2024-10-16
    Assistant:
    SELECT COUNT(*) FROM ( SELECT Peak_Date FROM Table9 WHERE Peak_Date = '2024-10-16' UNION ALL SELECT 
    Metric_Peak_Value_Time FROM Table1 WHERE Metric_Peak_Value_Time = '2024-10-16' UNION ALL SELECT 
    Metric_Peak_Value_Time FROM Table4 WHERE Metric_Peak_Value_Time = '2024-10-16' UNION ALL SELECT 
    Metric_Peak_Value_Time FROM Table5 WHERE Metric_Peak_Value_Time = '2024-10-16' UNION ALL SELECT 
    Metric_Peak_Value_Time FROM Table6 WHERE Metric_Peak_Value_Time = '2024-10-16' UNION ALL SELECT 
    Metric_Peak_Value_Time FROM Table7 WHERE Metric_Peak_Value_Time = '2024-10-16' UNION ALL SELECT 
    Metric_Peak_Value_Time FROM Table8 WHERE Metric_Peak_Value_Time = '2024-10-16' ) AS Peak_Dates_2024_10_16

    Do not return any of the incorrect behaviors shown above.

    database_schema: {database_schema}
    """.strip()

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("human", "{question}")
    ])
    return prompt | chat

def get_formatting_prompt_chain():
    system_prompt = f"""
    You are going to receive the user's question and the output of an SQL query executed on a database.
    Once that occurs, you should format the output of the query so that it answers the user's question.
    Output only the formatted result, do not explain the formatting process.
    """.strip()

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("human", "{question}, {result}")
    ])

    return prompt | chat
