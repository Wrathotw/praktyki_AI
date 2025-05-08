import streamlit as st
import time
import zipfile
import tempfile

from ai import chain
from ai import sql_query

suggestions = [
    'How many entries contain "watsonx" in their name',
    'Find entries thta contain "automation" in theit product name',
    'How many entries have their peak date on 2024-10-16'
]

if "messages" not in st.session_state:
    st.session_state.messages = []

if "show_suggestions" not in st.session_state:
    st.session_state.show_suggestions = True

if "input_text" not in st.session_state:
    st.session_state.input_text = ""

def response_generator(question):
    response = chain.invoke({"question": question})
    result = sql_query(response.content.replace('\\', ''))

    for word in str(result).split():
        yield word + " "
        time.sleep(0.1)

    return result

def use_suggestion(suggestion):
    st.session_state.input_text = suggestion
    st.session_state.trigger_send = True
    st.session_state.show_suggestions = False

st.title("DB chat app")

uploaded_zip = st.file_uploader(
    "Choose a ZIP file to work on",
    accept_multiple_files=False,
    type=['zip']
)

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

if uploaded_zip:
    with zipfile.ZipFile(uploaded_zip, mode="r") as archive:
        archive.printdir()
        archive.extractall(path="/zipfiles")
