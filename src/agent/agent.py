from logging import getLogger
import logging
from agent_gateway import Agent
from agent_gateway.tools import CortexSearchTool, CortexAnalystTool, PythonTool, SQLTool
from snowflake.snowpark import Session
import os
from dotenv import load_dotenv
from src.analyst.preprocess import Analyst_preprocess
from src.search.preprocess import Search_preprocess

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = getLogger(__name__)

class AgentGateway:
    def __init__(self):
        load_dotenv(encoding='utf-8', override=True)

        # SearchとAnalystの用意
        self.search_preprocess = Search_preprocess()
        self.search_preprocess.run()
        self.analyst_preprocess = Analyst_preprocess()
        self.analyst_preprocess.run()


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
        search_config = {
            "service_name": self.search_preprocess.search_service,
            "service_topic": "企業が従業員に対して示す、労働条件や職場内の規律を定めた就業に関する規則",
            "data_description": "就業規則",
            "retrieval_columns": ["chunk_id", "file_name", "text"],
            "snowflake_connection": self.connection,
            "k": 10,
        }

        analyst_config = {
            "semantic_model": self.analyst_preprocess.semantic_model_path.replace("src/analyst/semantic_model/", ""),
            "stage": f"{self.analyst_preprocess.table}_STAGE",
            "service_topic": "従業員の勤怠データ",
            "data_description": "４月の従業員の勤怠データ（部署、勤務時間、残業時間、理由）",
            "snowflake_connection": self.connection,
            "max_results": 10,
        }

        self.search_tool = CortexSearchTool(**search_config)
        self.analyst_tool = CortexAnalystTool(**analyst_config)

    def run(self):
        agent = Agent(snowflake_connection=self.connection, tools=[self.search_tool, self.analyst_tool], max_retries=3)

        response = agent("4月の従業員の勤怠データを教えてください。また、就業規則に関する情報も教えてください。")
        logger.info("Response:", response["output"])
        logger.info("Source:", response["sources"])


if __name__ == "__main__":
    agent = AgentGateway()
    agent.initialize_tools()
    agent.run()
