from logging import getLogger
import logging
from agent_gateway import Agent
from agent_gateway.tools import CortexSearchTool, CortexAnalystTool, PythonTool, SQLTool
from snowflake.snowpark import Session
import os
from dotenv import load_dotenv
from src.analyst.preprocess import Analyst_preprocess
from src.search.preprocess import Search_preprocess
from .response import AgentResult
from .tools import html_crawl

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
            "service_topic": "サンプル総研の従業員の4月の勤怠に関する集計データ",
            "data_description": "４月の従業員の勤怠データ（部署、勤務時間、残業時間、勤務理由）",
            "snowflake_connection": self.connection,
            "max_results": 5,
        }

        html_crawl_config = {
            "tool_description": "URLを指定すると、HTMLを取得するツール",
            "output_description": "htmlのウェブページ",
            "python_func": html_crawl
        }

        self.html_crawl_tool = PythonTool(**html_crawl_config)
        self.search_tool = CortexSearchTool(**search_config)
        self.analyst_tool = CortexAnalystTool(**analyst_config)

    def run(self, query) -> AgentResult:
        # クロスリージョン推論を有効化しLLMモデルを変更する
        connector = self.analyst_preprocess.connector
        connector.cursor().execute(f"ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';")
        connector.commit()
        connector.cursor().close()

        agent = Agent(snowflake_connection=self.connection, 
                    tools=[self.search_tool,self.analyst_tool,self.html_crawl_tool], 
                    max_retries=3,
                    planner_llm="claude-3-5-sonnet", 
                    agent_llm="claude-3-5-sonnet")

        response = agent(query)
        logger.info(response)

        # ! Analystの型も取れるように修正
        return AgentResult(
            output=response["output"],
            sources=[
            {
                "tool_type": source.get("tool_type"),
                "tool_name": source.get("tool_name"),
                "metadata": [
                    {
                        "file_name": metadata.get("file_name"),
                        "text": metadata.get("text"),
                        "chunk_id": metadata.get("chunk_id")
                    }
                    for metadata in source.get("metadata", [])
                ]
            }
            for source in response.get("sources", []) or []
        ]
        )


if __name__ == "__main__":
    agent = AgentGateway()
    agent.initialize_tools()
    agent.run("How many employees have worked more than 160 hours?")
