import snowflake.connector
from dotenv import load_dotenv
import os
from logging import getLogger
import logging
from langchain_community.document_loaders import PyPDFLoader
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = getLogger(__name__)

def load_pdf_document(file_path):
    loader = PyPDFLoader(file_path)
    return loader.load()


class Analyst_preprocess:
    def __init__(self):
        load_dotenv(encoding='utf-8',override=True)

        self.warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        self.database = os.getenv('SNOWFLAKE_DATABASE')
        self.schema = os.getenv('SNOWFLAKE_SCHEMA')
        self.tables = ["products","sales_summary_april","sales_transactions_april"] # Analyst用のテーブル名 複数必要な場合は、テーブル名を変更する
        self.semantic_model_path = "src/analyst/semantic_model/cortex_analyst_demo.yaml"

        self.connector = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'), 
        warehouse=self.warehouse,
        database=self.database,
        schema=self.schema)

    def test_connection(self):
        try:
            cursor = self.connector.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()
            logger.info(f"Connected to Snowflake: {version}")
            cursor.close()
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")

    def table_exists(self) -> bool:
        """テーブルが既に存在するか確認する"""
        cursor = self.connector.cursor()

        for table in self.tables:
            try:
                check_query = f"""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{self.schema.upper()}' 
                AND TABLE_NAME = '{table.upper()}'
                """
                cursor.execute(check_query)
                exists = cursor.fetchone()[0] > 0
                if not exists:
                    logger.info(f"テーブル {table} は存在しません。")
                    return False
            except Exception as e:
                logger.error(f"テーブル存在確認でエラー: {e}")
                return False
            else:
                logger.info(f"テーブル {table} は存在します。")
                return True

    def run(self) -> None:
        """Cortex Analystを使用する環境を構築"""
        if not self.table_exists():
            logger.info("テーブルは存在しません。")
            self.create_db()
            self.create_wh()
            self.create_schema()
            self.create_table()
            self.insert_data()

        self.enable_aoai()
        self.create_stage()
        # self.upload_file(self.semantic_model_path)

        logger.info("Cortex Analyst Preprocessが完了しました")
        return None
    
    def enable_aoai(self) -> None:
        """Azure OpenAIを有効化する"""
        cursor = self.connector.cursor()
        cursor.execute("USE ROLE ACCOUNTADMIN")
        cursor.execute("ALTER ACCOUNT SET ENABLE_CORTEX_ANALYST_MODEL_AZURE_OPENAI = TRUE")
        self.connector.commit()
        cursor.close()
        logger.info(f"Azure OpenAIを有効化しました")

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
        """テーブルを作成する"""
        cursor = self.connector.cursor()
        for table in self.tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            if table == "products":
                cursor.execute(f"""
                CREATE TABLE {table} (
                    product_id INT,
                    product_name STRING,
                    category STRING,
                    price FLOAT
                )
                """)
            elif table == "sales_summary_april":
                cursor.execute(f"""
                CREATE TABLE {table} (
                    department STRING,
                    total_sales FLOAT,
                    month STRING
                )
                """)
            elif table == "sales_transactions_april":
                cursor.execute(f"""
                CREATE TABLE {table} (
                    transaction_id INT,
                    date DATE,
                    product_id INT,
                    quantity INT,
                    total_price FLOAT,
                    department STRING,
                    channel STRING
                )
                """)
        self.connector.commit()
        cursor.close()
        logger.info("すべてのテーブルを作成しました。")

    def insert_data(self) -> None:
        """データをテーブルに挿入する"""

        cursor = self.connector.cursor()

        # ファイルとテーブルの対応
        file_table_map = {
            "products.csv": "products",
            "sales_summary_april.csv": "sales_summary_april",
            "sales_transactions_april.csv": "sales_transactions_april"
        }

        try:
            for file, table in file_table_map.items():
                logger.info(f"{file} を読み込み、テーブル {table} に挿入します。")

                # CSV読み込み
                df = pd.read_csv(f"src/analyst/semantic_model/data/{file}",encoding='utf-8',header=0)


                # SQLの構築
                columns = ', '.join(df.columns)
                placeholders = ', '.join(['%s'] * len(df.columns))
                insert_sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

                # データ挿入
                for _, row in df.iterrows():
                    # DATE型の処理が必要な場合、ここで文字列をdatetimeに変換して対応
                    values = tuple(row)
                    cursor.execute(insert_sql, values)

                logger.info(f"{file} の {len(df)} 行をテーブル {table} に挿入しました。")

            self.connector.commit()
            logger.info("すべてのデータ挿入が完了しました。")

        except Exception as e:
            logger.error(f"エラー発生: {e}")
            self.connector.rollback()

        finally:
            cursor.close()

    def create_stage(self) -> None:
        """ステージを作成する"""
        cursor = self.connector.cursor()
        self.stage_name = "CORTEX_ANALYST_STAGE"
        query = f"""
        CREATE OR REPLACE STAGE {self.stage_name}
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
        ) DIRECTORY=(ENABLE=TRUE);
        """
        cursor.execute(query)
        self.connector.commit()
        cursor.close()
        logger.info(f"ステージ {self.stage_name} を作成しました")

    def upload_file(self, file_path: str) -> None:
        """ファイルをステージにアップロードする"""
        # ! snowflake-cli系でput コマンドを実行する
        # ! あるいはsemantic model generatorを使う
        # ! あるいはsnowsightから直接アップロード
        return None

if __name__ == "__main__":
    logger.info("Analyst Preprocessを開始...")
    analyst_preprocess = Analyst_preprocess()
    analyst_preprocess.run()