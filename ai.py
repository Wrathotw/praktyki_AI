import os

from dotenv import find_dotenv, load_dotenv
from langchain_core.messages import HumanMessage
from langchain_mistralai.chat_models import ChatMistralAI
from langchain_core.prompts import ChatPromptTemplate

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

API_KEY = os.getenv("API_KEY")
chat = ChatMistralAI(api_key=API_KEY)

prompt = ChatPromptTemplate.from_template("Tell me a joke about {topic}")
chain = prompt | chat
