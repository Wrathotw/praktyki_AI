import os
import asyncio

from dotenv import find_dotenv, load_dotenv
from langchain_core.messages import HumanMessage
from langchain_mistralai.chat_models import ChatMistralAI
from langchain_core.prompts import ChatPromptTemplate

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

API_KEY = os.getenv("API_KEY")
chat = ChatMistralAI(api_key=API_KEY)

messages = [HumanMessage(content="knock knock")]
response = chat.invoke(messages)
print(f"\n{response.content} \n")

async def main():

    joke_topic = input("Enter the topic for an upcoming joke: ")

    prompt = ChatPromptTemplate.from_template("Tell me a joke about {topic}")
    chain = prompt | chat

    joke_response = await chain.ainvoke({"topic": joke_topic})
    print(f"\nJoke about {joke_topic}:", joke_response.content)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError:
        import nest_asyncio
        nest_asyncio.apply()
        asyncio.get_event_loop().run_until_complete(main())

print()