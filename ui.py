import streamlit as st
import time
import zipfile
import pandas as pd
import httpx
import hashlib

from ai import load_dataframes_into_db, get_prompt_chain, get_formatting_prompt_chain, sql_query

st.set_page_config(layout="wide")

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

st.title("DB chat app")

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

def response_generator(question):
    try:
        response = st.session_state.chain.invoke({"question": question})
        result = sql_query(response.content.replace('\\', ''))
        time.sleep(0.3)
        formatted_result = get_formatting_prompt_chain().invoke({"question":question, "result":str(result)}).content
        
        for word in str(formatted_result).split():
            yield word + " "
            time.sleep(0.1)

        return formatted_result

    except httpx.HTTPStatusError as err:
        if err.response.status_code == 429:
            yield "The AI service is currently rate-limited (too many requests). Please wait a moment and try again."
        else:
            yield f"An error occurred while contacting the AI service: {str(err)}"

    except Exception as e:
        yield f"An unexpected error occurred: {str(err)}"


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
        response_stream = response_generator(user_input)

        for word in response_stream:
            full_response += word
            message_placeholder.markdown(full_response)

    st.session_state.messages.append({"role": "assistant", "content": full_response})
    st.session_state.input_text = ""
    st.session_state.show_suggestions = False
