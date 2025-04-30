import snowflake.connector
from dotenv import load_dotenv
import os
from logging import getLogger
import logging
from langchain_community.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_openai import AzureOpenAIEmbeddings
from response import Chunks

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
        load_dotenv()

        self.database = os.getenv('SNOWFLAKE_DATABASE')
        self.schema = os.getenv('SNOWFLAKE_SCHEMA')

        self.connector = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'), 
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=self.database,
        schema=self.schema)
        
        self.rules_of_employment = load_pdf_document("src/search/data/モデル就業規則.pdf")
        self.text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
        self.embeddings = AzureOpenAIEmbeddings(
                deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"), 
                model=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
                azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
                openai_api_type="azure",
                openai_api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                openai_api_version=os.getenv("AZURE_OPENAI_API_VERSION"))


    def run(self) -> None:
        """Cortex Searchを使用する環境を構築"""
        self.create_db()
        self.create_wh()
        hoge = self.pdf_to_chunks()
        logger.info(len(hoge.embedding))
        return logger.info("Preprocessが完了しました")

    def create_db(self) -> None:
        """データベースを作成する"""
        query = f"""CREATE DATABASE IF NOT EXISTS {self.database}"""
        self.connector.cursor().execute(query)
        self.connector.cursor().close()
        return logger.info(f"データベース {self.database} を作成しました")
    
    def create_wh(self) -> None:
        """ウェアハウスを作成する"""
        query = f"""CREATE OR REPLACE WAREHOUSE {self.schema} WITH
            WAREHOUSE_SIZE='X-SMALL'
            AUTO_SUSPEND = 120
            AUTO_RESUME = TRUE
            INITIALLY_SUSPENDED=TRUE"""
        self.connector.cursor().execute(query)
        self.connector.cursor().close()
        return logger.info(f"ウェアハウス {self.schema} を作成しました")
    
    def pdf_to_chunks(self) -> Chunks:
        """PDFをチャンクに分割する"""
        documents = self.rules_of_employment
        chunks = self.text_splitter.split_documents(documents)

        texts = [chunk.page_content for chunk in chunks]
        vectors = self.embeddings.embed_documents(texts)

        logger.info("PDFのチャンク生成が完了しました")

        
        return Chunks(
            embedding=vectors,
            text=texts
        )



if __name__ == "__main__":
    logger.info("Search Preprocessを開始...")
    search_preprocess = Search_preprocess()
    search_preprocess.run()