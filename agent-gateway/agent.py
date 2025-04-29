from agent_gateway import Agent
from agent_gateway.tools import CortexSearchTool, CortexAnalystTool, PythonTool, SQLTool
from snowflake.snowpark import Session
import os
from dotenv import load_dotenv
import requests


class AgentGateway:
    def __init__(self):
        load_dotenv()

        connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        }

        self.connection = Session.builder.configs(connection_parameters).create()

    def  initialize_tools(self):
        pass

    def run(self):
        # Initialize the tools
        python_crawler_config = {
        "tool_description": "reads the html from a given URL or website",
        "output_description": "html of a webpage",
        "python_func": html_crawl,
    }
        search_weather_config = {
        "tool_description": "searches the weather for a given location",
        "output_description": "weather information discription",
        "python_func": search_weather,
    }   
        web_crawler = PythonTool(**python_crawler_config)
        get_weather = PythonTool(**search_weather_config)

        agent = Agent(snowflake_connection=self.connection,tools=[web_crawler, get_weather],max_retries=3)
        
        response = agent("以下にアクセスして情報をまとめてください https://aitc.dentsusoken.com/ 回答は日本語で生成してください。")
        print("Response:", response["output"])
        print("Source:", response["sources"])


if __name__ == "__main__":
    gateway = AgentGateway()
    gateway.initialize_tools()
    gateway.run()
