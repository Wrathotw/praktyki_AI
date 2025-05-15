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
    Make sure your SQL is syntactically correct. Always include a space between SQL keywords (e.g., SELECT COUNT(*) not SELECTCOUNT(*), WITH MQ_assignments not WITHMQ_assignments). 
    Make sure your HAVING clause usage is correct.
    Make sure you use COUNT() only on selected columns or '*'. Do not use it on a column you didn't select before. 
    Do not use backslashes or extra formatting. Output raw SQL only.
    You must return only the SQL query text, without any explanation, comments, formatting, or additional words. 
    Output the SQL as raw text only. Do not wrap the SQL in code blocks. 
    Do not say anything else.

    If you are asked to check if the MQ compomnents are assigned properly you have to look for three things in the table containing these columns:
    Row No.	Publisher, Imported Part Numbers, Product Name, Cloud Pak or FlexPoint Bundle, Metric, Server Name, Processor, Processor Brand String, Computer, Computer Deleted, OS, IP Address, Product Release, Component, Path, Unconfirmed Product Instance, Computer Last Seen, Discovery Start, Discovery End, Exclusion Comment, Charged, Computer First Seen
    The things you are supposed to look out for are:
    1. If the same computer has both 'IBM MQ' and 'IBM MQ Advanced' products assigned, and they are not both explicitly assigned under the 'IBM MQ Advanced' component, then it is improperly assigned. Return 'improperly assigned'.
    2. If the same computer has the product 'IBM MQ Advanced' but does not also have 'IBM MQ Advanced Message Security' (followed by a version number) assigned to a component, then it is improperly assigned. Return 'improperly assigned'.
    3. If a computer has the product 'IBM MQ Advanced' but is missing one or both of these: 'IBM MQ Managed File Transfer Agent' or 'IBM MQ Managed File Transfer Service', then it is improperly assigned. Return 'improperly assigned'. Use DISTINCT because those products may be listed multiple times.
    Always return 3 separate results (1 for each check). Do not group these conditions.
    Only perform MQ component assignment checks if the user explicitly asks if MQ components are assigned correctly or properly, using phrases like 'properly assigned', 'correctly assigned', or 'MQ assignment check'. Do not perform MQ component assignment logic just because 'MQ' is mentioned. Only trigger this logic for questions directly asking about the correctness or validity of MQ assignments.
    
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

    User: How many entries have their peak date on 2024-10-16
    Assistant:
    SELECT COUNT(*) FROM ( SELECT Peak_Date FROM Table9 WHERE Peak_Date = '2024-10-16' UNION ALL SELECT Metric_Peak_Value_Time FROM Table1 WHERE Metric_Peak_Value_Time = '2024-10-16' UNION ALL SELECT Metric_Peak_Value_Time FROM Table4 WHERE Metric_Peak_Value_Time = '2024-10-16' UNION ALL SELECT Metric_Peak_Value_Time FROM Table5 WHERE Metric_Peak_Value_Time = '2024-10-16' UNION ALL SELECT Metric_Peak_Value_Time FROM Table6 WHERE Metric_Peak_Value_Time = '2024-10-16' UNION ALL SELECT Metric_Peak_Value_Time FROM Table7 WHERE Metric_Peak_Value_Time = '2

    User: Are the MQ components properly assigned?
    Assistant:
    WITHMQ_assignments AS ( SELECT Computer, COUNT() as num_mq_products FROM ( SELECT Computer FROM Table2 WHERE Product_Name = 'IBM MQ' UNION ALL SELECT Computer FROM Table2 WHERE Product_Name = 'IBM MQ Advanced' UNION ALL SELECT Computer FROM Table2 WHERE Product_Name = 'IBM MQ Advanced Message Security' UNION ALL SELECT Computer FROM Table2 WHERE Product_Name = 'IBM MQ Managed File Transfer Agent' UNION ALL SELECT Computer FROM Table2 WHERE Product_Name = 'IBM MQ Managed File Transfer Service' ) AS MQ_products GROUP BY Computer HAVING COUNT() > 1 ), MQ_Advanced_assignments AS ( SELECT Computer, COUNT() as num_mq_advanced_products FROM ( SELECT Computer FROM Table2 WHERE Product_Name = 'IBM MQ Advanced' UNION ALL SELECT Computer FROM Table2 WHERE Product_Name = 'IBM MQ Advanced Message Security' ) AS MQ_advanced_products GROUP BY Computer HAVING COUNT() < 2 ), MQ_Managed_File_Transfer_assignments AS ( SELECT Computer, COUNT() as num_mq_managed_file_transfer_products FROM ( SELECT Computer FROM Table2 WHERE Product_Name = 'IBM MQ Advanced' UNION ALL SELECT Computer FROM Table2 WHERE Product_Name = 'IBM MQ Managed File Transfer Agent' UNION ALL SELECT Computer FROM Table2 WHERE Product_Name = 'IBM MQ Managed File Transfer Service' ) AS MQ_managed_file_transfer_products GROUP BY Computer HAVING COUNT() < 3 ) SELECT CASE WHEN COUNT() > 0 THEN 'improperly assigned' ELSE 'properly assigned' END as MQ_component_assignment_status FROM MQ_assignments UNION ALL SELECT CASE WHEN COUNT() > 0 THEN 'improperly assigned' ELSE 'properly assigned' END as MQ_Advanced_component_assignment_status FROM MQ_Advanced_assignments UNION ALL SELECT CASE WHEN COUNT(*) > 0 THEN 'improperly assigned' ELSE 'properly assigned' END as MQ_Managed_File_Transfer_component_assignment_status FROM MQ_Managed_File_Transfer_assignments;
    
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

    If you receive the words 'improperly assigned' or 'properly assigned' instead of a query output, answer the user's question based on that information.
    Any instance of 'improperly assigned' means that your answer should mention the components being improperly assigned. If you only see 'properly assigned' then answer with that.
    The first instance of 'improperly assigned' or 'properly assigned' refers to the situation when the same computer has both 'IBM MQ' and 'IBM MQ Advanced' products assigned, and they are not both explicitly assigned under the 'IBM MQ Advanced' component.
    The second instance of 'improperly assigned' or 'properly assigned' refers to the situation when the same computer has the product 'IBM MQ Advanced' but does not also have 'IBM MQ Advanced Message Security' (followed by a version number) assigned to a component.
    The third instance of 'improperly assigned' or 'properly assigned' refers to the situation when a computer has the product 'IBM MQ Advanced' but is missing one or both of these: 'IBM MQ Managed File Transfer Agent' or 'IBM MQ Managed File Transfer Service'.
    Format the answer you give to the user based on these informations. Do not return the number of instances.
    """.strip()

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("human", "{question}, {result}")
    ])

    return prompt | chat
