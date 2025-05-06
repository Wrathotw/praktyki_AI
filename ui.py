import streamlit as st
import time
import zipfile
import tempfile

from ai import chain
from ai import sql_query

# Streamed response emulator
def response_generator(question):
    response = chain.invoke({"question": question})
    result = sql_query(response.content)

    for word in str(result).split():
        yield word + " "
        time.sleep(0.1)

    return result

st.title("My first chat app")

uploaded_zip = st.file_uploader(
    "Choose a ZIP file to work on",
    accept_multiple_files=False,
    type=['zip']
)

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Accept user input
if prompt := st.chat_input("What should the chatbot return?"):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)   

    # Stream assistant response
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        full_response = ""
        response_stream = response_generator(prompt)

        for word in response_stream:
            full_response += word
            message_placeholder.markdown(full_response)

    st.session_state.messages.append({"role": "assistant", "content": full_response})
if uploaded_zip:
    temp_dir = tempfile.gettempdir()
    with zipfile.ZipFile(uploaded_zip, mode="r") as archive:
        archive.printdir()
        archive.extractall(path=f'{temp_dir}\zipdir')