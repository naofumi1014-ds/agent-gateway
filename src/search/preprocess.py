import snowflake.connector
from dotenv import load_dotenv
import os
from logging import getLogger
import logging
from langchain_community.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_openai import AzureOpenAIEmbeddings
from response import Chunk
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = getLogger(__name__)

def load_pdf_document(file_path):
    loader = PyPDFLoader(file_path)
    return loader.load()


class Search_preprocess:
    def __init__(self):
        load_dotenv(encoding='utf-8')

        self.warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        self.database = os.getenv('SNOWFLAKE_DATABASE')
        self.schema = os.getenv('SNOWFLAKE_SCHEMA')
        self.table = "WORK_RULES_OF_EMPLOYMENT"

        self.connector = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'), 
        warehouse=self.warehouse,
        database=self.database,
        schema=self.schema)
        
        self.filename = "src/search/data/モデル就業規則.pdf"
        self.rules_of_employment = load_pdf_document(self.filename)
        self.text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
        self.embeddings = AzureOpenAIEmbeddings(
                deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"), 
                model=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
                azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
                openai_api_type="azure",
                openai_api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                openai_api_version=os.getenv("AZURE_OPENAI_API_VERSION"))
        
    def test_connection(self):
        try:
            cursor = self.connector.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()
            logger.info(f"Connected to Snowflake: {version}")
            cursor.close()
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")


    def run(self) -> None:
        """Cortex Searchを使用する環境を構築"""
        self.create_db()
        self.create_wh()
        self.create_schema()
        self.create_table()
        Chunk_list = self.pdf_to_chunks()
        self.upload_pdf(Chunk_list)
        return logger.info("Cortex Search Preprocessが完了しました")

    def create_db(self) -> None:
        """データベースを作成する"""
        query = f"""CREATE DATABASE IF NOT EXISTS {self.database}"""
        self.connector.cursor().execute(query)
        self.connector.commit()  
        self.connector.cursor().close()
        return logger.info(f"データベース {self.database} を作成しました")
    
    def create_schema(self) -> None:
        """スキーマを作成する"""
        query = f"""CREATE SCHEMA IF NOT EXISTS {self.schema}"""
        cursor = self.connector.cursor()
        cursor.execute(query)
        self.connector.commit()
        cursor.close()
        logger.info(f"スキーマ {self.schema} を作成しました")
    
    def create_wh(self) -> None:
        """ウェアハウスを作成する"""
        query = f"""CREATE OR REPLACE WAREHOUSE {self.warehouse} WITH
            WAREHOUSE_SIZE='X-SMALL'
            AUTO_SUSPEND = 120
            AUTO_RESUME = TRUE
            INITIALLY_SUSPENDED=TRUE"""
        self.connector.cursor().execute(query)
        self.connector.commit()  
        self.connector.cursor().close()
        return logger.info(f"ウェアハウス {self.warehouse} を作成しました")
    
    def create_table(self) -> None:
        """チャンク情報を保存するテーブルを作成する"""
        query = f"""CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} (
                chunk_id NUMBER,
                file_name VARCHAR,
                text VARCHAR
            )
            """
        cursor = self.connector.cursor()
        cursor.execute(query)
        self.connector.commit()
        cursor.close()
        logger.info(f"Snowflakeにテーブル {self.table} を作成しました")

    def pdf_to_chunks(self) -> list[Chunk]:
        """PDFをチャンクに分割する"""
        documents = self.rules_of_employment
        chunks = self.text_splitter.split_documents(documents)

        Chunk_list = []
        for num, chunk in enumerate(chunks):
            chunk_data = Chunk(
                num=num,
                file_name=self.filename,
                text=chunk.page_content
            )
            Chunk_list.append(chunk_data)

        logger.info("PDFのチャンク生成が完了しました")
        return Chunk_list
    
    def upload_pdf(self, Chunk_list) -> None:
        """PDFチャンクをSnowflakeにアップロードする"""
        cursor = self.connector.cursor()

        insert_query = f"""
        INSERT INTO {self.schema}.{self.table} (chunk_id, file_name, text)
        VALUES (%s, %s, %s)
        """

        for chunk in Chunk_list:
            # INSERT 処理
            cursor.execute(
                insert_query,
                (chunk.num, chunk.file_name, chunk.text)
            )

        self.connector.commit()
        cursor.close()
        logger.info("チャンク化したPDFをSnowflakeにアップロードしました")

    def create_search_service(self) -> None:
        """Cortex Searchを作成する 現状はUIか、SQLで作成する必要がある"""



        logger.info("Cortex Searchの作成が完了しました")


if __name__ == "__main__":
    logger.info("Search Preprocessを開始...")
    search_preprocess = Search_preprocess()
    search_preprocess.run()