import streamlit as st
import time
import zipfile
import pandas as pd
import httpx
import hashlib
import os
import uuid

from ai import load_dataframes_into_db, get_prompt_chain, get_formatting_prompt_chain, sql_query
from supabase import create_client
from dotenv import find_dotenv, load_dotenv

st.set_page_config(layout="wide")

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

URL = os.getenv("database_url")
KEY = os.getenv("database_api")
supabase_client = create_client(URL, KEY)

def DB_chat_app():
    st.title("DB Chat App")

    suggestions = [
        'How many entries contain "watsonx" in their name',
        'Find entries that contain "automation" in their product name',
        'How many entries have their peak date on 2024-10-16'
    ]

    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "show_suggestions" not in st.session_state:
        st.session_state.show_suggestions = True
    if "input_text" not in st.session_state:
        st.session_state.input_text = ""

    def get_file_hash(file):
        file.seek(0)
        content = file.read()
        file.seek(0)
        return hashlib.md5(content).hexdigest()

    uploaded_zip = st.file_uploader("Choose a ZIP file to work on", type=['zip'])

    if uploaded_zip:
        current_hash = get_file_hash(uploaded_zip)
        if st.session_state.get("last_uploaded_hash") != current_hash:
            st.session_state.messages = []
            st.session_state.input_text = ""
            st.session_state.show_suggestions = True
            st.session_state.last_uploaded_hash = current_hash

        with zipfile.ZipFile(uploaded_zip) as archive:
            csv_dataframes = []
            for file_name in archive.namelist():
                if file_name.endswith(".csv"):
                    with archive.open(file_name) as f:
                        df = pd.read_csv(f)
                        csv_dataframes.append((file_name, df))

            load_dataframes_into_db(csv_dataframes)
            st.session_state.chain = get_prompt_chain()
            st.success("ZIP loaded and tables created!")

        if "session_id" not in st.session_state:
            st.session_state.session_id = str(uuid.uuid4())

        def log_chat_to_supabase(session_id, user_msg, assistant_msg):
            supabase_client.table("chat_history").insert({
                "session_id": session_id,
                "user_message": user_msg,
                "assistant_response": assistant_msg
            }).execute()

    def response_generator(question):
        try:
            response = st.session_state.chain.invoke({"question": question})
            result = sql_query(response.content.replace('\\', ''))
            time.sleep(0.3)
            formatted_result = get_formatting_prompt_chain().invoke({
                "question": question,
                "result": str(result)
            }).content

            for word in str(formatted_result).split():
                yield word + " "
                time.sleep(0.1)

            log_chat_to_supabase(st.session_state.session_id, question, formatted_result)
            return formatted_result

        except httpx.HTTPStatusError as err:
            if err.response.status_code == 429:
                yield "Rate limit hit. Please wait and try again."
            else:
                yield f"HTTP error: {str(err)}"
        except Exception as e:
            yield f"Unexpected error: {str(e)}"

    def use_suggestion(suggestion):
        st.session_state.input_text = suggestion
        st.session_state.trigger_send = True
        st.session_state.show_suggestions = False

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if st.session_state.show_suggestions and len(st.session_state.messages) == 0:
        st.markdown("Suggestions:")
        for suggestion in suggestions:
            st.button(suggestion, on_click=use_suggestion, args=(suggestion,))

    user_input = st.chat_input("What should the chatbot return?")

    if st.session_state.get("trigger_send", False):
        user_input = st.session_state.input_text
        st.session_state.trigger_send = False

    if user_input:
        st.session_state.messages.append({"role": "user", "content": user_input})
        with st.chat_message("user"):
            st.markdown(user_input)

        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            full_response = ""
            for word in response_generator(user_input):
                full_response += word
                message_placeholder.markdown(full_response)

        st.session_state.messages.append({"role": "assistant", "content": full_response})
        st.session_state.input_text = ""
        st.session_state.show_suggestions = False

def get_distinct_via_client():
    response = supabase_client.table("chat_history").select("session_id").execute()
    values = [row["session_id"] for row in response.data]
    return list(set(values))

session_pages = []

for i, session_id in enumerate(get_distinct_via_client(), start=1):
    def make_page(sid=session_id):
        def page():
            st.title(f"Chat History: {sid}")
            response = supabase_client.table("chat_history") \
                .select("user_message", "assistant_response") \
                .eq("session_id", sid) \
                .execute()
            for row in response.data:
                st.markdown(f"**User**: {row['user_message']}")
                st.markdown(f"**Assistant**: {row['assistant_response']}")
                st.markdown("---")
        return page
    session_pages.append((f"Session {i}", make_page()))

pages = [("DB Chat", DB_chat_app)] + session_pages

page_names = [name for name, _ in pages]
selection = st.sidebar.selectbox("Go to", page_names)
for name, func in pages:
    if selection == name:
        func()
        break