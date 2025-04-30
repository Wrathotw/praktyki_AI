import streamlit as st
import random
import time


# Streamed response emulator
def response_generator():
    response = random.choice(
        [
            "Hi! How can i help you?",
            "Hello! Is there anything I can help you with?",
            "Do you need help?",
            "Hi there! What can I do for you?"
        ]
    )
    for word in response.split():
        yield word + " "
        time.sleep(0.10)


st.title("My first chat app")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Accept user input
if prompt := st.chat_input("What is up?"):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)

    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        response = st.write_stream(response_generator())
    # Add assistant response to chat history
    st.session_state.messages.append({"role": "assistant", "content": response})